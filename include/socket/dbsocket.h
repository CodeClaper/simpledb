#include <stddef.h>

/* Try to connect. */
int TryConnect(char *host, int port, char *account, char *pwd);

/* Recv data. */
char *RecvData(int client);

/* Send data. */
int SendData(int client, char *data);
