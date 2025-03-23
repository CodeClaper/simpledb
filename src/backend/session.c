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

static Session inner_session;

static void clearn_up_spool();

/* New session. */
void new_session(int client) {
    inner_session.client = client;
    inner_session.frequency = 0;
    inner_session.volumn = 0;
    clearn_up_spool();
}

/* Spool if empty. */
inline static bool spool_is_empty() {
    return inner_session.pindex == 0;
}

/* Spool if full. */
inline static bool spool_is_full() {
    return inner_session.pindex >= SPOOL_SIZE - LEFT_SPACE;
}

/* Clear up spool. */
inline static void clearn_up_spool() {
    bzero(inner_session.spool, SPOOL_SIZE);
    inner_session.pindex = 0;
}

/* Store spool. */
static char *store_spool(char *message) {
    size_t len = strlen(message);
    size_t current = inner_session.pindex + len;
    if (streq(OVER_FLAG, message)) {
        if (inner_session.pindex == 0) {
            memcpy(inner_session.spool, message, len); 
            inner_session.pindex = current;
            return NULL;
        } else {
            return message;
        }
    } else if (current < SPOOL_SIZE - LEFT_SPACE) {
        memcpy(inner_session.spool + inner_session.pindex, message, len); 
        inner_session.pindex = current;
        return NULL;
    } else {
        inner_session.pindex = SPOOL_SIZE;
        return message;
    }
}

/* Socket send message.
 * return true if send successfully, else return false. */
bool db_send(const char *format, ...) {
    va_list ap;
    ssize_t s, size;
    uint32_t len;
    char sbuff[SPOOL_SIZE];

    if (format == NULL)
        return false;

    Assert(strlen(format) < SPOOL_SIZE);

    /* Initialize send buffer. */
    bzero(sbuff, SPOOL_SIZE);

    va_start(ap, format);
    
    /* Assignment send buffer. */
    vsprintf(sbuff, format, ap);
    
    va_end(ap);

    /* Store message into spool. */
    char *left_msg = store_spool(sbuff);

    /* Only when spool is full or OVER FLAG, socket will send the whole spool data. */
    if (!spool_is_full() && !streq(OVER_FLAG, sbuff))
        return true;

    Assert(!spool_is_empty());

    size = strlen(inner_session.spool);
    len = (uint32_t) size;

    /* Check if client close connection, if recv get zero which means client has closed conneciton. */
    if ((s = send(inner_session.client, &len, sizeof(len), 0)) == sizeof(len)
            && (s = send(inner_session.client, inner_session.spool, len, 0)) == len) {

        /* Clear up spool. */
        clearn_up_spool();

        /* If there are left message, continue db_send. */
        if (left_msg != NULL) 
            return db_send(left_msg);

        return true;
    }

    return false;
}

/* Socket send 'OVER' flag,
 * which means the message is over.
 * */
bool db_send_over() {
    return db_send(OVER_FLAG);
}

