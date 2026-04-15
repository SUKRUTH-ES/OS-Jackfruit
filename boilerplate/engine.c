#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

struct supervisor_ctx;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int run_request;
    int monitor_registered;
    int log_pipe_fd;
    int logger_joined;
    char termination_reason[32];
    char log_path[PATH_MAX];
    void *child_stack;
    pthread_t producer_thread;
    pthread_cond_t state_changed;
    struct container_record *next;
} container_record_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct supervisor_ctx {
    char base_rootfs[PATH_MAX];
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_args_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} client_handler_args_t;

static volatile sig_atomic_t supervisor_sigchld = 0;
static volatile sig_atomic_t supervisor_shutdown_requested = 0;
static volatile sig_atomic_t client_forward_stop = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int write_full(int fd, const void *buf, size_t len)
{
    const char *ptr = (const char *)buf;
    size_t written = 0;

    while (written < len) {
        ssize_t rc = write(fd, ptr + written, len - written);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (rc == 0)
            return -1;
        written += (size_t)rc;
    }

    return 0;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *ptr = (char *)buf;
    size_t consumed = 0;

    while (consumed < len) {
        ssize_t rc = read(fd, ptr + consumed, len - consumed);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (rc == 0)
            return (consumed == 0) ? 0 : -1;
        consumed += (size_t)rc;
    }

    return 1;
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static void copy_cstr(char *dst, size_t dst_len, const char *src)
{
    if (dst_len == 0)
        return;

    if (!src)
        src = "";

    snprintf(dst, dst_len, "%s", src);
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1U) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1U) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static void append_supervisor_log(supervisor_ctx_t *ctx,
                                  const char *container_id,
                                  const char *fmt,
                                  ...)
{
    log_item_t item;
    va_list ap;

    memset(&item, 0, sizeof(item));
    copy_cstr(item.container_id, sizeof(item.container_id), container_id);

    va_start(ap, fmt);
    item.length = (size_t)vsnprintf(item.data, sizeof(item.data), fmt, ap);
    va_end(ap);

    if (item.length >= sizeof(item.data))
        item.length = sizeof(item.data) - 1;

    (void)bounded_buffer_push(&ctx->log_buffer, &item);
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
    }
    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if (cur->host_pid == pid)
            return cur;
    }
    return NULL;
}

static int any_running_on_rootfs_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if ((cur->state == CONTAINER_STARTING || cur->state == CONTAINER_RUNNING) &&
            strncmp(cur->rootfs, rootfs, sizeof(cur->rootfs)) == 0)
            return 1;
    }
    return 0;
}

static int ensure_log_dir(void)
{
    struct stat st;

    if (stat(LOG_DIR, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            errno = ENOTDIR;
            return -1;
        }
        return 0;
    }

    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST)
        return -1;

    return 0;
}

static int write_response(int fd, int status, const char *message, const char *body)
{
    control_response_t resp;

    memset(&resp, 0, sizeof(resp));
    resp.status = status;
    if (message)
        copy_cstr(resp.message, sizeof(resp.message), message);

    if (write_full(fd, &resp, sizeof(resp)) < 0)
        return -1;

    if (body && write_full(fd, body, strlen(body)) < 0)
        return -1;

    return 0;
}

static void producer_cleanup(producer_args_t *args)
{
    if (!args)
        return;
    if (args->read_fd >= 0)
        close(args->read_fd);
    free(args);
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        container_record_t *rec;
        char path[PATH_MAX];
        int fd;

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_locked(ctx, item.container_id);
        if (rec)
            copy_cstr(path, sizeof(path), rec->log_path);
        else
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        path[sizeof(path) - 1] = '\0';
        pthread_mutex_unlock(&ctx->metadata_lock);

        fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        (void)write_full(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    int devnull;

    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount private");

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }

    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount /proc");

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 log");
        return 1;
    }

    devnull = open("/dev/null", O_RDONLY);
    if (devnull >= 0) {
        (void)dup2(devnull, STDIN_FILENO);
        close(devnull);
    }

    close(cfg->log_write_fd);
    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 127;
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (monitor_fd < 0)
        return -1;

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (monitor_fd < 0)
        return -1;

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static void join_container_logger(container_record_t *rec)
{
    if (!rec->logger_joined) {
        pthread_join(rec->producer_thread, NULL);
        rec->logger_joined = 1;
    }

    if (rec->log_pipe_fd >= 0) {
        close(rec->log_pipe_fd);
        rec->log_pipe_fd = -1;
    }
}

static void *producer_thread_main(void *arg)
{
    producer_args_t *args = (producer_args_t *)arg;
    supervisor_ctx_t *ctx = args->ctx;
    char buf[LOG_CHUNK_SIZE];

    for (;;) {
        ssize_t n = read(args->read_fd, buf, sizeof(buf));
        log_item_t item;

        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        if (n == 0)
            break;

        memset(&item, 0, sizeof(item));
        copy_cstr(item.container_id, sizeof(item.container_id), args->container_id);
        memcpy(item.data, buf, (size_t)n);
        item.length = (size_t)n;
        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    producer_cleanup(args);
    return NULL;
}

static void mark_container_finished(supervisor_ctx_t *ctx, pid_t pid, int status)
{
    container_record_t *rec;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_by_pid_locked(ctx, pid);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    if (WIFEXITED(status)) {
        rec->exit_code = WEXITSTATUS(status);
        rec->exit_signal = 0;
        rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
        strncpy(rec->termination_reason,
                rec->stop_requested ? "stopped" : "exited",
                sizeof(rec->termination_reason) - 1);
    } else if (WIFSIGNALED(status)) {
        rec->exit_code = 128 + WTERMSIG(status);
        rec->exit_signal = WTERMSIG(status);
        if (rec->stop_requested) {
            rec->state = CONTAINER_STOPPED;
            strncpy(rec->termination_reason, "stopped", sizeof(rec->termination_reason) - 1);
        } else if (WTERMSIG(status) == SIGKILL) {
            rec->state = CONTAINER_KILLED;
            strncpy(rec->termination_reason,
                    "hard_limit_killed",
                    sizeof(rec->termination_reason) - 1);
        } else {
            rec->state = CONTAINER_KILLED;
            strncpy(rec->termination_reason, "signaled", sizeof(rec->termination_reason) - 1);
        }
    }

    if (rec->monitor_registered) {
        (void)unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);
        rec->monitor_registered = 0;
    }

    pthread_cond_broadcast(&rec->state_changed);
    pthread_mutex_unlock(&ctx->metadata_lock);

    join_container_logger(rec);
    append_supervisor_log(ctx,
                          rec->id,
                          "[supervisor] container=%s pid=%d final_state=%s reason=%s exit_code=%d signal=%d\n",
                          rec->id,
                          rec->host_pid,
                          state_to_string(rec->state),
                          rec->termination_reason,
                          rec->exit_code,
                          rec->exit_signal);
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0)
        mark_container_finished(ctx, pid, status);
}

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGCHLD)
        supervisor_sigchld = 1;
    else
        supervisor_shutdown_requested = 1;
}

static void client_signal_handler(int signo)
{
    (void)signo;
    client_forward_stop = 1;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           char *message,
                           size_t message_len,
                           int *exit_status)
{
    container_record_t *rec = NULL;
    producer_args_t *producer_args = NULL;
    int pipefd[2] = {-1, -1};
    child_config_t *child_cfg = NULL;
    int rc = -1;

    if (ensure_log_dir() < 0) {
        snprintf(message, message_len, "failed to create %s: %s", LOG_DIR, strerror(errno));
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_locked(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_len, "container id '%s' already exists", req->container_id);
        return -1;
    }

    if (any_running_on_rootfs_locked(ctx, req->rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_len, "requested rootfs is already in use");
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(pipefd) < 0) {
        snprintf(message, message_len, "pipe failed: %s", strerror(errno));
        return -1;
    }

    rec = calloc(1, sizeof(*rec));
    child_cfg = calloc(1, sizeof(*child_cfg));
    producer_args = calloc(1, sizeof(*producer_args));
    if (!rec || !child_cfg || !producer_args) {
        snprintf(message, message_len, "allocation failure");
        goto out;
    }

    rec->child_stack = malloc(STACK_SIZE);
    if (!rec->child_stack) {
        snprintf(message, message_len, "stack allocation failure");
        goto out;
    }

    copy_cstr(rec->id, sizeof(rec->id), req->container_id);
    copy_cstr(rec->rootfs, sizeof(rec->rootfs), req->rootfs);
    copy_cstr(rec->command, sizeof(rec->command), req->command);
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, req->container_id);
    rec->started_at = time(NULL);
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->nice_value = req->nice_value;
    rec->state = CONTAINER_STARTING;
    rec->run_request = (req->kind == CMD_RUN);
    rec->log_pipe_fd = pipefd[0];
    pthread_cond_init(&rec->state_changed, NULL);

    copy_cstr(child_cfg->id, sizeof(child_cfg->id), req->container_id);
    copy_cstr(child_cfg->rootfs, sizeof(child_cfg->rootfs), req->rootfs);
    copy_cstr(child_cfg->command, sizeof(child_cfg->command), req->command);
    child_cfg->nice_value = req->nice_value;
    child_cfg->log_write_fd = pipefd[1];

    rec->host_pid = clone(child_fn,
                          (char *)rec->child_stack + STACK_SIZE,
                          CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                          child_cfg);
    if (rec->host_pid < 0) {
        snprintf(message, message_len, "clone failed: %s", strerror(errno));
        goto out;
    }

    close(pipefd[1]);
    pipefd[1] = -1;

    producer_args->ctx = ctx;
    producer_args->read_fd = pipefd[0];
    copy_cstr(producer_args->container_id, sizeof(producer_args->container_id), rec->id);
    if (pthread_create(&rec->producer_thread, NULL, producer_thread_main, producer_args) != 0) {
        snprintf(message, message_len, "failed to create producer thread");
        kill(rec->host_pid, SIGKILL);
        goto out;
    }
    pipefd[0] = -1;
    producer_args = NULL;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->state = CONTAINER_RUNNING;
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_cond_broadcast(&rec->state_changed);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (register_with_monitor(ctx->monitor_fd,
                              rec->id,
                              rec->host_pid,
                              rec->soft_limit_bytes,
                              rec->hard_limit_bytes) == 0) {
        rec->monitor_registered = 1;
    }

    append_supervisor_log(ctx,
                          rec->id,
                          "[supervisor] started container=%s pid=%d rootfs=%s command=%s soft=%luMiB hard=%luMiB nice=%d\n",
                          rec->id,
                          rec->host_pid,
                          rec->rootfs,
                          rec->command,
                          rec->soft_limit_bytes >> 20,
                          rec->hard_limit_bytes >> 20,
                          rec->nice_value);

    snprintf(message, message_len, "started container=%s pid=%d", rec->id, rec->host_pid);

    if (req->kind == CMD_RUN) {
        pthread_mutex_lock(&ctx->metadata_lock);
        while (rec->state == CONTAINER_STARTING || rec->state == CONTAINER_RUNNING)
            pthread_cond_wait(&rec->state_changed, &ctx->metadata_lock);
        *exit_status = rec->exit_code;
        pthread_mutex_unlock(&ctx->metadata_lock);
    } else {
        *exit_status = 0;
    }

    free(child_cfg);
    return 0;

out:
    if (pipefd[0] >= 0)
        close(pipefd[0]);
    if (pipefd[1] >= 0)
        close(pipefd[1]);
    producer_cleanup(producer_args);
    free(child_cfg);
    if (rec) {
        if (rec->child_stack)
            free(rec->child_stack);
        pthread_cond_destroy(&rec->state_changed);
        free(rec);
    }
    return rc;
}

static void stop_all_containers(supervisor_ctx_t *ctx, int sig)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cur = ctx->containers; cur; cur = cur->next) {
        if (cur->state == CONTAINER_STARTING || cur->state == CONTAINER_RUNNING) {
            cur->stop_requested = 1;
            kill(cur->host_pid, sig);
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void format_time(time_t value, char *buf, size_t buf_len)
{
    struct tm tm_value;

    localtime_r(&value, &tm_value);
    strftime(buf, buf_len, "%Y-%m-%d %H:%M:%S", &tm_value);
}

static int handle_stop_request(supervisor_ctx_t *ctx,
                               const char *container_id,
                               char *message,
                               size_t message_len)
{
    container_record_t *rec;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_locked(ctx, container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_len, "container '%s' not found", container_id);
        return -1;
    }

    if (rec->state != CONTAINER_STARTING && rec->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message,
                 message_len,
                 "container '%s' already finished with state=%s",
                 container_id,
                 state_to_string(rec->state));
        return 0;
    }

    rec->stop_requested = 1;
    kill(rec->host_pid, SIGTERM);
    pthread_mutex_unlock(&ctx->metadata_lock);

    append_supervisor_log(ctx,
                          container_id,
                          "[supervisor] stop requested for container=%s\n",
                          container_id);
    snprintf(message, message_len, "stop requested for %s", container_id);
    return 0;
}

static void render_ps_body(supervisor_ctx_t *ctx, char *body, size_t body_len)
{
    container_record_t *cur;
    size_t used = 0;

    used += (size_t)snprintf(body + used,
                             body_len - used,
                             "ID\tPID\tSTATE\tREASON\tSOFT(MiB)\tHARD(MiB)\tNICE\tSTARTED\tLOG\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cur = ctx->containers; cur && used < body_len; cur = cur->next) {
        char started[32];

        format_time(cur->started_at, started, sizeof(started));
        used += (size_t)snprintf(body + used,
                                 body_len - used,
                                 "%s\t%d\t%s\t%s\t%lu\t%lu\t%d\t%s\t%s\n",
                                 cur->id,
                                 cur->host_pid,
                                 state_to_string(cur->state),
                                 cur->termination_reason[0] ? cur->termination_reason : "-",
                                 cur->soft_limit_bytes >> 20,
                                 cur->hard_limit_bytes >> 20,
                                 cur->nice_value,
                                 started,
                                 cur->log_path);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void render_logs_body(supervisor_ctx_t *ctx,
                             const char *container_id,
                             char *body,
                             size_t body_len,
                             int *status,
                             char *message,
                             size_t message_len)
{
    container_record_t *rec;
    int fd;
    ssize_t n;
    size_t used = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_locked(ctx, container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        *status = 1;
        snprintf(message, message_len, "container '%s' not found", container_id);
        return;
    }

    fd = open(rec->log_path, O_RDONLY);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (fd < 0) {
        *status = 1;
        snprintf(message, message_len, "log file unavailable for '%s'", container_id);
        return;
    }

    while ((n = read(fd, body + used, body_len - used - 1)) > 0) {
        used += (size_t)n;
        if (used >= body_len - 1)
            break;
    }
    body[used] = '\0';
    close(fd);

    *status = 0;
    snprintf(message, message_len, "logs for %s", container_id);
}

static void *client_handler_main(void *arg)
{
    client_handler_args_t *handler = (client_handler_args_t *)arg;
    supervisor_ctx_t *ctx = handler->ctx;
    int client_fd = handler->client_fd;
    control_request_t req;
    char body[16384];
    char message[CONTROL_MESSAGE_LEN];
    int status = 0;

    memset(&req, 0, sizeof(req));
    memset(body, 0, sizeof(body));
    memset(message, 0, sizeof(message));

    if (read_full(client_fd, &req, sizeof(req)) != 1) {
        close(client_fd);
        free(handler);
        return NULL;
    }

    switch (req.kind) {
    case CMD_START:
    case CMD_RUN:
        if (start_container(ctx, &req, message, sizeof(message), &status) != 0)
            status = 1;
        break;
    case CMD_PS:
        render_ps_body(ctx, body, sizeof(body));
        snprintf(message, sizeof(message), "container metadata");
        status = 0;
        break;
    case CMD_LOGS:
        render_logs_body(ctx,
                         req.container_id,
                         body,
                         sizeof(body),
                         &status,
                         message,
                         sizeof(message));
        break;
    case CMD_STOP:
        if (handle_stop_request(ctx, req.container_id, message, sizeof(message)) != 0)
            status = 1;
        break;
    default:
        status = 1;
        snprintf(message, sizeof(message), "unsupported request");
        break;
    }

    (void)write_response(client_fd, status, message, body[0] ? body : NULL);
    close(client_fd);
    free(handler);
    return NULL;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    copy_cstr(ctx.base_rootfs, sizeof(ctx.base_rootfs), rootfs);

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (ensure_log_dir() < 0) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "warning: /dev/container_monitor unavailable: %s\n", strerror(errno));

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto out;
    }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto out;
    }

    if (listen(ctx.server_fd, 16) < 0) {
        perror("listen");
        goto out;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        goto out;
    }

    fprintf(stdout, "supervisor listening on %s with base rootfs %s\n", CONTROL_PATH, rootfs);
    fflush(stdout);

    while (!ctx.should_stop) {
        fd_set rfds;
        struct timeval tv;
        int ready;

        if (supervisor_sigchld) {
            supervisor_sigchld = 0;
            reap_children(&ctx);
        }

        if (supervisor_shutdown_requested) {
            ctx.should_stop = 1;
            break;
        }

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        ready = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (ready > 0 && FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                client_handler_args_t *handler = calloc(1, sizeof(*handler));
                pthread_t thread;

                if (!handler) {
                    close(client_fd);
                    continue;
                }

                handler->ctx = &ctx;
                handler->client_fd = client_fd;
                if (pthread_create(&thread, NULL, client_handler_main, handler) == 0)
                    pthread_detach(thread);
                else {
                    close(client_fd);
                    free(handler);
                }
            }
        }
    }

    stop_all_containers(&ctx, SIGTERM);
    sleep(1);
    reap_children(&ctx);
    stop_all_containers(&ctx, SIGKILL);
    sleep(1);
    reap_children(&ctx);

out:
    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
    }

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);

    pthread_mutex_lock(&ctx.metadata_lock);
    while (ctx.containers) {
        container_record_t *next = ctx.containers->next;

        pthread_mutex_unlock(&ctx.metadata_lock);
        join_container_logger(ctx.containers);
        if (ctx.containers->monitor_registered)
            (void)unregister_from_monitor(ctx.monitor_fd, ctx.containers->id, ctx.containers->host_pid);
        pthread_cond_destroy(&ctx.containers->state_changed);
        free(ctx.containers->child_stack);
        free(ctx.containers);
        pthread_mutex_lock(&ctx.metadata_lock);
        ctx.containers = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int forward_stop_request(const char *container_id)
{
    control_request_t req;
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (write_full(fd, &req, sizeof(req)) < 0) {
        close(fd);
        return -1;
    }

    (void)read_full(fd, &resp, sizeof(resp));
    close(fd);
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    int rc;
    struct sigaction sa;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) < 0) {
        perror("write");
        close(fd);
        return 1;
    }

    if (req->kind == CMD_RUN) {
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = client_signal_handler;
        sigemptyset(&sa.sa_mask);
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);

        for (;;) {
            fd_set rfds;
            struct timeval tv;

            FD_ZERO(&rfds);
            FD_SET(fd, &rfds);
            tv.tv_sec = 1;
            tv.tv_usec = 0;
            rc = select(fd + 1, &rfds, NULL, NULL, &tv);
            if (rc < 0) {
                if (errno == EINTR)
                    continue;
                perror("select");
                close(fd);
                return 1;
            }

            if (client_forward_stop) {
                client_forward_stop = 0;
                (void)forward_stop_request(req->container_id);
            }

            if (rc > 0)
                break;
        }
    }

    rc = read_full(fd, &resp, sizeof(resp));
    if (rc != 1) {
        fprintf(stderr, "failed to read supervisor response\n");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0')
        fprintf(resp.status == 0 ? stdout : stderr, "%s\n", resp.message);

    for (;;) {
        char buf[1024];
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (n == 0)
            break;
        (void)write_full(STDOUT_FILENO, buf, (size_t)n);
    }

    close(fd);
    return (resp.status < 0) ? 1 : resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
