#include <bits/types/struct_timeval.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "server.h"
#include "defs.h"
#include "mmgr.h"
#include "common.h"
#include "stmt.h"
#include "free.h"
#include "session.h"
#include "log.h"
#include "auth.h"
#include "timer.h"
#include "banner.h"
#include "mctx.h"
#include "asctx.h"
#include "stacktrace.h"
#include "instance.h"
#include "jsonwriter.h"

/* Start up the server. */
int startup(u_short port) {
    int httpd = 0;
    int on = 1;
    size_t buff_size = SPOOL_SIZE;
    struct sockaddr_in *address = dalloc(sizeof(struct sockaddr_in));
    httpd = socket(PF_INET, SOCK_STREAM, 0);
    if (httpd == -1)
        db_log(PANIC, "Create socket fail.");
    memset(address, 0, sizeof(struct sockaddr_in));
    address->sin_family = AF_INET;
    address->sin_port = htons(port);
    address->sin_addr.s_addr = htonl(INADDR_ANY);

    /* SO_REUSEADDR */
    if ((setsockopt(httpd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) < 0) 
        db_log(PANIC, "Set socket SO_REUSEADDR option fail.");

    /* SO_RCVBUF */
    if ((setsockopt(httpd, SOL_SOCKET, SO_RCVBUF, &buff_size, sizeof(buff_size))) < 0) 
        db_log(PANIC, "Set socket SO_RCVBUF option fail.");

    /* SO_SNDBUF */
    if ((setsockopt(httpd, SOL_SOCKET, SO_SNDBUF, &buff_size, sizeof(buff_size))) < 0) 
        db_log(PANIC, "Set socket SO_SNDBUF option fail.");

    /* Bind */
    if (bind(httpd, (struct sockaddr *)address, sizeof(*address)) < 0) 
        db_log(PANIC, "Bind socket fail.");

    /* Listen */
    if (listen(httpd, 10) < 0) 
        db_log(PANIC, "Socket listen fail.");

    return httpd;
}


/* Auth client. */
static bool auth_request(intptr_t client) {
    DBResult *result;
    char *login;

    result = new_db_result();
    login = db_recv();

    bool pass = auth(login);
    if (pass) {
        result->success = true;
    } else {
        result->success = false;
        result->message = dstrdup("No access.");
    }
    result->stmt_type = LOGIN_STMT;

    json_db_result(result);
    db_send_over();

    return pass;
}


/* For loop request. */
static void loop_request(intptr_t client) {
    size_t chars_num;
    char buf[SPOOL_SIZE];
    bzero(buf, SPOOL_SIZE);
    db_log(INFO, "Client ID '%ld' connect successfully.", getpid());
    while ((chars_num = recv(client, buf, SPOOL_SIZE, 0)) > 0) {
        buf[chars_num] = '\0';
        Execute(buf);
        bzero(buf, SPOOL_SIZE);
        if (!db_send_over())
            break;
        MemoryContextReset(MASTER_MEMORY_CONTEXT);
        DestroyContextRecorders();
    }
    db_log(INFO, "Client ID '%ld' disconnect.", getpid());
}

/* At the MemoryContext start. */
static void memory_context_start() {
    /* Create the TOP_MEMORY_CONTEXT. */
    MASTER_MEMORY_CONTEXT = AllocSetMemoryContextCreate(TOP_MEMORY_CONTEXT, "MasterMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    CACHE_MEMORY_CONTEXT = AllocSetMemoryContextCreate(MASTER_MEMORY_CONTEXT, "CacheMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    MemoryContextSwitchTo(MASTER_MEMORY_CONTEXT);
}

/* At the MemoryContext end. */
static void memory_context_end() {
    /* Delete the TOP_MEMORY_CONTEXT. */
    MemoryContextDelete(MASTER_MEMORY_CONTEXT);
}


/* Accept request.*/
void accept_request(intptr_t client) {

    // Start new session.
    new_session(client);

    /* Set signal handler. */
    set_signal_handler();

    memory_context_start();
    /* Auth login message. */
    if (auth_request(client)) 
        loop_request(client);
    close(client);
    memory_context_end();
    /* Quite */
    exit(0);
}
