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
void NewSession(int cli);

/* Clean up pre data. */
bool CleanUpPreData();

/* db send pre. */
bool MakePreData(const char *format, ...);

/* Socket send
 * return true if send successfully, else return false.  */
bool db_send(const char *format, ...);

/* Socket send 'Over' flag,
 * which means the message is over. */
bool DbSendOver();

/* Socket recv. */
char *DbRecv();
