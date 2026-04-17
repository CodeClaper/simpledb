#include "c.h"
#include "data.h"

#define XID_NIL ((Xid) 0)

void InitTrans();
bool IsActive(Xid xid);
bool IsVisibleInner(Xid created_xid, Xid expired_xid, TransEntry *current);
bool IsVisible(Xid created_xid, Xid expired_xid);
bool RowIsVisible(Row *row);
bool RowIsDeleted(Row *row);
bool AnyTransactionRunning();
void AutoBeginTransaction();
void BeginTransaction();
TransEntry *FindTransaction();
Xid GetCurrentXid();
void CommitTransaction();
void AutoCommitTransaction();
void RollbackTransaction();
void AutoRollbackTransaction();
void UpdateTransactionState(Row *row, TransOpType trans_op_type);
