#include "data.h"

/* Session */
typedef struct {
    int client;
    uint32_t frequency;
    uint32_t volumn;
    char spool[SPOOL_SIZE];     /* Store messsage pool. */
    volatile uint32_t pindex;   /* Current spool position index. */
    char *tempData;             /* The temp data. */
} Session;

#define EOF_MG      "\r\n\r\n"  /* EOF flag of message. */
#define LEFT_SPACE  4

void NewSession(int cli);
bool CancelTempData();
bool MakeTempData(const char *format, ...);
bool db_send(const char *format, ...);
bool DbSendOver();
char *DbRecv();
