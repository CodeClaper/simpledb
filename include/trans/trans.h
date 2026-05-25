#include <stdbool.h>
#include "c.h"
#include "data.h"

#define XID_NIL ((Xid) 0)

typedef bool (*COMMIT_EVENT)(Oid oid);

typedef struct TransCommitEventEntry {
    COMMIT_EVENT                    hanler;         /* Commit event handler.*/
    Oid                             oid;            /* Input arguement. */
    struct TransCommitEventEntry    *next;          /* Next. */
} TransCommitEventEntry;

typedef struct TransEntry {
    Xid                             xid;            /* Transaction id. */ 
    Pid                             pid;            /* Processor id. */
    bool                            auto_commit;    /* Auto commit. */
    struct TransEntry               *next;          /* Next */
    struct TransCommitEventEntry    *commit_event;  /* Commit event.*/  
} TransEntry;


void InitTrans();
bool IsActive(Xid xid);
bool IsVisibleInner(Xid created_xid, Xid expired_xid, TransEntry *current);
bool IsVisible(Xid created_xid, Xid expired_xid);
bool RowIsVisible(Row *row);
bool RowIsDeleted(Row *row);
bool AnyTransactionRunning();
bool RegisterCommitEvent(COMMIT_EVENT hanler, Oid oid);
void AutoBeginTransaction();
void BeginTransaction();
TransEntry *FindTransaction();
Xid GetCurrentXid();
void CommitTransaction();
void AutoCommitTransaction();
void RollbackTransaction();
void AutoRollbackTransaction();
void UpdateTransactionState(Row *row, TransOpType trans_op_type);
