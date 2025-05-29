
int tryConnect(char *host, int port, char *account, char *pwd);

/* Send data. */
int SendData(int client, char *data);

/* Recive request data. */
char *ReceiveRequestData(int client);
