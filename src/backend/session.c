#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include "session.h"
#include "data.h"
#include "mmgr.h"
#include "log.h"
#include "utils.h"
#include "asserts.h"

static Session client;
static void CleanUpSession();
static bool DbSendTempData();

/* New session. */
void NewSession(int cli) {
    client.client = cli;
    client.frequency = 0;
    client.volumn = 0;
    client.tempData = NULL;
    CleanUpSession();
}

/* Check if session is empty. */
inline static bool SessionIsEmpty() {
    return client.pindex == 0;
}

/* Check if session is full. */
inline static bool SessionIsFull() {
    return client.pindex >= SPOOL_SIZE - LEFT_SPACE;
}

/* Clear up session pool. */
static void CleanUpSession() {
    bzero(client.spool, SPOOL_SIZE);
    client.pindex = 0;
}

/* Save message to pool. */
static char *SaveSessionMessage(char *message) {
    size_t len = strlen(message);
    size_t current = client.pindex + len;
    if (current < SPOOL_SIZE - LEFT_SPACE) {
        memcpy(client.spool + client.pindex, message, len); 
        client.pindex = current;
        return NULL;
    } else {
        client.pindex = SPOOL_SIZE;
        return message;
    }
}

/* Make the temp data. 
 * -------------------
 * Sometime, we need store the message to the temp data install the pool. 
 * The temp data may be canceled by <CancelTempData>.
 * Before store message into the temp data, we need to send off all data 
 * in the pool to make sure the temp data will be send fistly.
 * */
bool MakeTempData(const char *format, ...) {
    va_list ap;
    ssize_t s;
    uint32_t len;
    char sbuff[SPOOL_SIZE];

    if (format == NULL) return false;
    Assert(strlen(format) < SPOOL_SIZE);

    /* Initialize send buffer. */
    bzero(sbuff, SPOOL_SIZE);

    va_start(ap, format);
    
    /* Assignment send buffer. */
    vsprintf(sbuff, format, ap);
    
    va_end(ap);

    if (!StrIsEmpty(client.tempData))
        DbSendTempData();

    if (!SessionIsEmpty()) {
        len = (uint32_t) strlen(client.spool);
        Assert(len > 0);
        Assert(len < SPOOL_SIZE - LEFT_SPACE);

        memmove(((char *) client.spool) + 4, client.spool, len);
        memcpy(client.spool, &len, 4);

        /* Check if client close connection, if recv get zero 
         * which means client has closed conneciton. */
        if ((s = send(client.client, client.spool, (len + 4), MSG_NOSIGNAL)) > 0) CleanUpSession();
    }
    
    client.tempData = dstrdup(sbuff);

    return true;
}

/* Cancel the temp data. */
bool CancelTempData() {
    if (client.tempData == NULL) return false;
    dfree(client.tempData);
    client.tempData = NULL;
    return true;
}

/* Send the temp data. */
static bool DbSendTempData() {
    if (client.tempData == NULL) return true;
    char sbuff[SPOOL_SIZE];
    Size len;

    len = strlen(client.tempData);
    Assert(len < SPOOL_SIZE - 4);
    memcpy(sbuff, &len, 4);
    memcpy(sbuff + 4, client.tempData, len);

    return send(client.client, sbuff, len + 4, MSG_NOSIGNAL) > 0 && CancelTempData();
}

/* Socket send message.
 * return true if send successfully, else return false. */
bool db_send(const char *format, ...) {
    va_list ap;
    ssize_t s;
    uint32_t len;
    char sbuff[SPOOL_SIZE];

    if (format == NULL) return false;
    Assert(strlen(format) < SPOOL_SIZE);

    /* Initialize send buffer. */
    bzero(sbuff, SPOOL_SIZE);

    va_start(ap, format);
    
    /* Assignment send buffer. */
    vsprintf(sbuff, format, ap);
    
    va_end(ap);

    /* Store message into spool. */
    char *left_msg = SaveSessionMessage(sbuff);

    /* Only when spool is full or OVER FLAG, socket will send the whole spool data. */
    if (!SessionIsFull() && !StrEq(OVER_FLAG, sbuff)) return true;
    Assert(!SessionIsEmpty());

    len = (uint32_t) strlen(client.spool);
    Assert(len > 0);
    Assert(len < SPOOL_SIZE - LEFT_SPACE);

    memmove(((char *) client.spool) + 4, client.spool, len);
    memcpy(client.spool, &len, 4);

    /* Check if client close connection, if recv get zero 
     * which means client has closed conneciton. */
    if (
        DbSendTempData() && 
        (s = send(client.client, client.spool, (len + 4), MSG_NOSIGNAL)) > 0
    ) {
        /* Clear up spool. */
        CleanUpSession();
        /* If there are left message, continue db_send. */
        return (left_msg != NULL) ? db_send(left_msg) : true;
    }

    return false;
}

/* Socket send 'OVER' flag,
 * which means the message is over.
 * */
bool DbSendOver() {
    return db_send(OVER_FLAG);
}

/* Socket recv. */
char *DbRecv() {
    size_t r;
    char *buf = dalloc(SPOOL_SIZE);
    bzero(buf, SPOOL_SIZE);
    
    r = recv(client.client, buf, SPOOL_SIZE, 0);
    if (r > 0) return buf;
    else {
        dfree(buf);
        return NULL;
    }
}
