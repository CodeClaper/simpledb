#include <stdlib.h>

int Socket(const char *host, int clientPort);
int SocketRecv(int client, void *data, size_t size);
int SocketSend(int fd, const void *buf, size_t size, int flags);
