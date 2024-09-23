#include "data.h"

/* Session */
typedef struct {
    int client;
    uint32_t frequency;
    uint32_t volumn;
    char spool[SPOOL_SIZE]; /* Store messsage pool. */
    uint32_t pindex;             /* Current spool position index. */
} Session;

/* Generate new session. */
void new_session(int client);

/* Socket send
 * return true if send successfully, else return false.  */
bool db_send(const char *format, ...);

/* Socket send 'Over' flag,
 * which means the message is over.
 * */
bool db_send_over();

