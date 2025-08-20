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
int Startup(u_short port) {
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
static bool AuthRequest(intptr_t client) {
    DBResult *result;
    char *login_info;

    result = new_db_result();
    login_info = ReceiveRequestData(client);

    bool pass = auth(login_info);
    if (pass) {
        result->success = true;
        result->message = dstrdup("Login success.");
    } else {
        result->success = false;
        result->message = dstrdup("No access.");
    }
    result->stmt_type = LOGIN_STMT;

    json_db_result(result);
    DbSendOver();

    return pass;
}

/* Socket Recive data. */
static int SocketRecv(intptr_t client, void *data, size_t size) {
    size_t chars_num, rsize = 0;

    while (rsize < size) {
        chars_num = recv(client, data + rsize, size - rsize, 0);
        if (chars_num > 0)
            rsize += chars_num;
        else
            return -1;
    }

    return rsize;
}

/* Recive request data. */
char *ReceiveRequestData(intptr_t client) {
    size_t chars_num;
    int32_t len;
    char *rdata;

    chars_num = SocketRecv(client, &len, sizeof(int32_t));
    if (chars_num <= 0)
        return NULL;

    rdata = dalloc(len + 1);
    chars_num = SocketRecv(client, rdata, len);
    if (chars_num <= 0)
        return NULL;

    return rdata;
}


/* For loop request. */
static void RequestHandler(intptr_t client) {
    char *rdata;
    struct timeval start_time, end_time;

    /* Start tiem recorder. */
    gettimeofday(&start_time, NULL);

    while ((rdata = ReceiveRequestData(client)) != NULL) {
        Execute(rdata);
        if (!DbSendOver())
            break;
        MemoryContextReset(MASTER_MEMORY_CONTEXT);
        DestroyContextRecorders();
        gettimeofday(&end_time, NULL);
        db_log(INFO, "Loop duration: %lfs", time_span(end_time, start_time));
        start_time = end_time;
    }

    db_log(INFO, "Client ID '%ld' disconnect.", getpid());
}

/* At the MemoryContext start. */
static void MemoryContextStart() {
    /* Create the TOP_MEMORY_CONTEXT. */
    MASTER_MEMORY_CONTEXT = AllocSetMemoryContextCreate(TOP_MEMORY_CONTEXT, "MasterMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    CACHE_MEMORY_CONTEXT = AllocSetMemoryContextCreate(MASTER_MEMORY_CONTEXT, "CacheMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    MemoryContextSwitchTo(MASTER_MEMORY_CONTEXT);
}

/* At the MemoryContext end. */
static void MemoryContextEnd() {
    /* Delete the TOP_MEMORY_CONTEXT. */
    MemoryContextDelete(MASTER_MEMORY_CONTEXT);
}


/* Accept request.*/
void AcceptRequest(intptr_t client) {
    /* Start new session. */
    NewSession(client);

    /* Set signal handler. */
    set_signal_handler();

    MemoryContextStart();

    /* Auth login message. */
    if (AuthRequest(client)) {
        db_log(INFO, "Client ID '%ld' connect successfully.", getpid());
        RequestHandler(client);
    }

    close(client);

    MemoryContextEnd();

    /* Quite */
    exit(0);
}
