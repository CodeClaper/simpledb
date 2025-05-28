#include "data.h"

/* Start up the server.*/
int Startup(u_short port);

/* Recive request data. */
char *ReceiveRequestData(intptr_t client);

/* Accept request.*/
void AcceptRequest(intptr_t client);
