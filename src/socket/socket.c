/* $Id: socket.c 1.1 1995/01/01 07:11:14 cthuang Exp $
 *
 * This module has been modified by Radim Kolar for OS/2 emx
 */

/***********************************************************************
  module:       socket.c
  program:      popclient
  SCCS ID:      @(#)socket.c    1.5  4/1/94
  programmer:   Virginia Tech Computing Center
  compiler:     DEC RISC C compiler (Ultrix 4.1)
  environment:  DEC Ultrix 4.3 
  description:  UNIX sockets code.
 ***********************************************************************/
 
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/time.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>

/* Create socket. */
int Socket(const char *host, int clientPort) {
    int sock;
    unsigned long inaddr;
    struct sockaddr_in ad;
    struct hostent *hp;
    
    memset(&ad, 0, sizeof(ad));
    ad.sin_family = AF_INET;

    inaddr = inet_addr(host);
    if (inaddr != INADDR_NONE)
        memcpy(&ad.sin_addr, &inaddr, sizeof(inaddr));
    else {
        hp = gethostbyname(host);
        if (hp == NULL) return -1;
        memcpy(&ad.sin_addr, hp->h_addr, hp->h_length);
    }
    ad.sin_port = htons(clientPort);
    
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return sock;
    if (connect(sock, (struct sockaddr *)&ad, sizeof(ad)) < 0) return -1;

    return sock;
}

/* Socket Recive data. */
int SocketRecv(int client, void *data, size_t size) {
    size_t chars_num, rsize = 0;

    while (rsize < size) {
        chars_num = recv(client, data + rsize, size - rsize, 0);
        if (chars_num > 0) {
            rsize += chars_num;
        } else if (chars_num == 0) {
            /* EOF: client closed connection gracefully */
            return -1;
        } else {
            /* Interrupted by signal, retry */
            if (errno == EINTR) continue;
            /* Connection reset by peer or broken pipe */
            if (errno == ECONNRESET || errno == EPIPE) return -1;
            /* Other error */
            return -1;
        }
    }

    return rsize;
}


/* Socket send. */
int SocketSend(int fd, const void *buf, size_t size, int flags) {
    ssize_t s;
    size_t total = 0;

    while (total < size) {
        s = send(fd, buf + total, size - total, flags);
        if (s < 0) 
        {
            /* Interrupted by signal, retry */
            if (errno == EINTR) continue;
            /* Other error */
            else return -1;
        } 
        /* EOF: client closed connection gracefully */
        else if (s == 0) return -1;
        else total += s;
    }
    return 1;
}

