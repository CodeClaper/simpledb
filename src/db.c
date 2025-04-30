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
#include "gc.h"
#include "refer.h"
#include "tablecache.h"
#include "fdesc.h"
#include "tablelock.h"
#include "log.h"
#include "shmgr.h"
#include "shmem.h"
#include "tablereg.h"
#include "bufmgr.h"
#include "pager.h"
#include "bgwriter.h"
#include "asctx.h"
#include "systable.h"
#include "sysstate.h"

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
static inline void sigchild() {
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

/* DB Start. */
static void init_db() {

    MakeSysState(SYS_READY);

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

    /* Initialise refer. */
    init_refer();

    /* Initialise table lock. */
    init_table_lock();

    /* Init pager. */
    InitPager();

    /* Load configuration. */
    conf = load_conf();

    /* Initialise table cache. */
    InitTableCache();

    /* Init system table. */
    InitSysTable();

}

/* Start bgwriter. */
static void start_bgwriter() {
    /* Create new child process. */
    pid_t pid = fork();
    if (pid < 0) 
        db_log(PANIC, "Create new child process fail.");
    else if (pid == 0) {
        StartBgWriter();
        exit(EXECUTE_SUCCESS);
    }
    else 
        db_log(SUCCESS, "Start up background writer successfully.");
}

/* Start backend. */
static void start_backend(int server_socket, struct sockaddr_in *client_name, socklen_t client_name_len) {
    int client_secket = -1;
    /* Listen client connecting. */
    while (true) {
        client_secket = accept(server_socket, (struct sockaddr *) client_name, &client_name_len);
        if (client_secket == -1)
            db_log(PANIC, "Socket accept fail.");
        
        /* Signal child process. */
        signal(SIGCHLD, sigchild);

        /* Create new child process. */
        pid_t pid = fork();
        if (pid < 0) 
            db_log(PANIC, "Create new child process fail.");
        else if (pid == 0) {
            accept_request((intptr_t)client_secket);
            exit(EXECUTE_SUCCESS);
        }
        else
            close(client_secket);
    }
}


/* DB Run. */
static void db_run() {
    int server_socket = -1;

    struct sockaddr_in *client_name = dalloc(sizeof(struct sockaddr_in));
    socklen_t client_name_len = sizeof(*client_name);

    /* Start up server. */
    server_socket = startup(conf->port);
    db_log(INFO, "Simpledb server start up successfully and listen port %d.", conf->port);

    /* Means the system start up successfully, 
     * and accept the request of backend. */
    MakeSysState(SYS_RUNNING);

    /* Start up bgwriter.*/
    start_bgwriter();

    /* Start up backend. */
    start_backend(server_socket, client_name, client_name_len);
}

/* DB End */
static void db_end() {
    MemoryContextDelete(TOP_MEMORY_CONTEXT);
    exit(EXIT_SUCCESS);
}


/* The main entry. */
int main(int argc, char* argv[]) {
    program_name = argv[0];
    init_db();
    db_run();
    db_end();
}
