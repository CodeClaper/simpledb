#include "data.h"

/* Session */
typedef struct {
    int client;
    uint32_t frequency;
    uint32_t volumn;
    char spool[SPOOL_SIZE];     /* Store messsage pool. */
    volatile uint32_t pindex;   /* Current spool position index. */
    char *preData;              /* Pre data. */
} Session;

#define OVER_FLAG "\r\n\r\n"  /* Over flag of message. */
#define LEFT_SPACE 4

/* Generate new session. */
void new_session(int client);

/* Clean up pre data. */
bool clean_pre_data();

/* db send pre. */
bool db_send_pre(const char *format, ...);

/* Socket send
 * return true if send successfully, else return false.  */
bool db_send(const char *format, ...);

/* Socket send 'Over' flag,
 * which means the message is over.
 * */
bool db_send_over();

/* Socket recv. */
char *db_recv();
