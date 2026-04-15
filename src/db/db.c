#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include "data.h"
#include "defs.h"
#include "mmgr.h"
#include "trans.h"
#include "xlog.h"
#include "parser.h"
#include "stmt.h"
#include "free.h"
#include "server.h"
#include "common.h"
#include "session.h"
#include "conf.h"
#include "banner.h"
#include "refer.h"
#include "tablecache.h"
#include "fdesc.h"
#include "tablelock.h"
#include "rowlock.h"
#include "log.h"
#include "shmgr.h"
#include "shmem.h"
#include "tablereg.h"
#include "bufmgr.h"
#include "bgwriter.h"
#include "asctx.h"
#include "systable.h"
#include "sysstate.h"
#include "stacktrace.h"

/* 
 * Conf 
 */
Conf *conf; 

/* 
 * jmp_buf for error. 
 */
jmp_buf errEnv; 

/* 
 * program name
 */
const char *program_name;  

/* Child process signal.*/
static inline void sigchild(int signal) {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            db_log(PANIC, "Child process %d exited with error: %d.", pid, WEXITSTATUS(status));
            print_stacktrace();
            kill(0, SIGTERM);
            _exit(EXIT_FAILURE);
        }
        else if (WIFSIGNALED(status)) {
            db_log(PANIC, "Child process %d killed by signal: %d.", pid, WTERMSIG(status));
            print_stacktrace();
            kill(0, SIGTERM);
            _exit(EXIT_FAILURE);
        }
    }
}


/* Init environment. */
static void init_env() {
    setenv("TZ", conf->time_zone, 1);
    tzset();
}

/* DB Start. */
void init_db() {

    /* Load configuration. */
    conf = load_conf();

    /* Init environment. */
    init_env();

    /* MemoryContext init.*/
    MemoryContextInit();

    /* Initialise shmem. */
    init_shmem();

    /* Initialise memory manger. */
    init_mem();

    /* Initialise tablereg. */
    init_table_reg();

    /* Initialise fesc.*/
    init_fdesc();

    /* Initialise transaction. */
    InitTrans();

    /* Initialise bufmgr. */
    InitBufMgr();

    /* Initialise table lock. */
    init_table_lock();

    /* Initialise row lock. */
    InitRowLock();

    /* Init system table. */
    InitSysTable();

    /* Initialise table cache. */
    InitTableCache();

    /* Signal child process. */
    signal(SIGCHLD, sigchild);

    /* Set signal handler. */
    set_signal_handler();

    MakeSysState(SYS_INITED);
    db_log(SUCCESS, "Init db success.");
}

/* Start bgwriter. */
static void start_bgwriter() {
    /* Create new child process. */
    pid_t pid = fork();
    if (pid < 0) db_log(PANIC, "Create new child process fail.");
    else if (pid == 0) {
        StartBgWriter();
        exit(13);
    } else {
        MakeSysState(SYS_READY);
        db_log(SUCCESS, "Start up background writer successfully.");
    }
}

/* Start backend. */
static void start_backend(int server_socket, struct sockaddr_in *client_name, socklen_t client_name_len) {
    int client_secket = -1;
    /* Listen client connecting. */
    while (true) {
        client_secket = accept(server_socket, (struct sockaddr *) client_name, &client_name_len);
        if (client_secket == -1) db_log(PANIC, "Socket accept fail.");

        /* Create new child process. */
        pid_t pid = fork();
        if (pid < 0) db_log(PANIC, "Create new child process fail.");
        else if (pid == 0) {
            AcceptRequest((intptr_t)client_secket);
            exit(EXECUTE_SUCCESS);
        } 
        else close(client_secket);
    }
}


/* DB Run. */
static void db_run() {
    int server_socket = -1;

    struct sockaddr_in *client_name = dalloc(sizeof(struct sockaddr_in));
    socklen_t client_name_len = sizeof(*client_name);

    /* Start up server. */
    server_socket = Startup(conf->port);

    /* Print out banner. */
    puts(BANNER);

    db_log(INFO, "Simpledb server start up successfully and listen port %d.", conf->port);

    /* Means the system start up successfully, 
     * and accept the request of backend. */
    MakeSysState(SYS_RUNNING);

    /* Start up backend. */
    start_backend(server_socket, client_name, client_name_len);
}

/* DB End */
static void db_end() {
    MemoryContextDelete(TOP_MEMORY_CONTEXT);
    exit(EXIT_SUCCESS);
}

/* Parse argv. */
void parse_argv(int argc, char* argv[]) {
    program_name = argv[0];
    for (int i = 1; i < argc; i++) {
        if ((StrEq(argv[i], "-l") || StrEq(argv[i], "--level"))) {
            if (i + 1 >= argc) continue;
            char *level = argv[++i];
            if (StrEq(level, "TRACE")) conf->log_level = TRACE;
            else if (StrEq(level, "DEBUG")) conf->log_level = DEBUGER;
            else if (StrEq(level, "INFO")) conf->log_level = INFO;
            else if (StrEq(level, "SUCCESS")) conf->log_level = SUCCESS;
            else if (StrEq(level, "WARN")) conf->log_level = WARN;
            else if (StrEq(level, "ERROR")) conf->log_level = ERROR;
            else {
                fprintf(stderr, "Bad log level: %s, only support 'TRACE', 'DEBUG;, 'INFO', 'SUCCS', 'WARN', 'ERROR', 'SYSERR', 'TATAL', 'PANIC'. ", level);
                _exit(1);
            }
        }
    }
}

/* The main entry. */
int run(int argc, char* argv[]) {
    init_db();
    parse_argv(argc, argv);
    start_bgwriter();
    db_run();
    db_end();
    return 0;
}
