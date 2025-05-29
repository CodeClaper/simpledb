#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include "socket.h"
#include "cJSON.h"

#define OVER_FLAG "\r\n\r\n"  /* Over flag of message. */

/* check if a file has suffix. */
bool endwith(char *str, char *suffix) {
    if (!str || !suffix)
        return false;
    ssize_t str_len = strlen(str);
    ssize_t suffix_size = strlen(suffix);
    if (suffix_size > str_len)
        return false;
    return strcmp(str + str_len - suffix_size, suffix) == 0;
}

/* Socket Recive data. */
static int SocketRecv(int client, void *data, size_t size) {
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
char *ReceiveRequestData(int client) {
    size_t chars_num;
    int32_t len;
    char *rdata;

    chars_num = SocketRecv(client, &len, sizeof(int32_t));
    if (chars_num <= 0)
        return NULL;
    rdata = malloc(len + 1);
    chars_num = SocketRecv(client, rdata, len);
    if (chars_num <= 0)
        return NULL;
    rdata[len] = '\0';
    return rdata;
}

/* Send data. */
int SendData(int client, char *data) {
    int len, slen;
    char *buff;

    len = strlen(data);
    buff = malloc(len + 4);
    memcpy(buff, &len, 4);
    memcpy(buff + 4, data, len);
    slen = send(client, buff, len + 4, 0);
    free(buff);

    return slen;
}

int TryLogin(int client, char *account, char *pwd) {
    char buff[1024];
    memset(buff, 0, 1024);
    sprintf(buff, "%s/%s", account, pwd);
    if (SendData(client, buff) > 0) {
        char *resp = ReceiveRequestData(client);
        cJSON *json = cJSON_Parse(resp);
        if (json == NULL) {
            const char *err = cJSON_GetErrorPtr();
            if (err != NULL)
                fprintf(stderr, "Parse json err: %s\n", err);
            exit(1);
        }
        cJSON *success = cJSON_GetObjectItemCaseSensitive(json, "success");
        return success->valueint ? client : -2;
    }
    return -1;
}

/* Try to connect the database. 
 * --------------------------
 * >0: success.
 * -1: connect fail.
 * -2: bad account or password.
 * */
int TryConnect(char *host, int port, char *account, char *pwd) {
    int client = Socket(host, port);
    if (client > 0) {
        if (TryLogin(client, account, pwd))
            return client;
        else
            return -2;
    }
    return client;
}
