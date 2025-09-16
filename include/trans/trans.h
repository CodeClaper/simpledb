#include "c.h"
#include "data.h"

#define XID_NIL ((Xid) 0)

/* Initialise transaction. */
void InitTrans();

/* Check if a transaction is active()
 * Active transaction is an uncommitted transaction. */
bool IsActive(Xid xid);

/* Is visible by created_xid and expired_xid. */
bool IsVisibleInner(Xid created_xid, Xid expired_xid, TransEntry *current);

/* Is visible by created_xid and expired_xid. */
bool IsVisible(Xid created_xid, Xid expired_xid);

/* Check if row is visible for current transaction. */
bool RowIsVisible(Row *row);

/* Check if a row has been deleted. */
bool RowIsDeleted(Row *row);

/* Any running transaction. */
bool AnyTransactionRunning();

/* New transaction which will be committed automatically. */
void AutoBeginTransaction();

/* New transaction which will be commited automatically. */
void BeginTransaction();

/* Get current transction handle. 
 * Return current thread transaction, return NULL if not found. */
TransEntry *FindTransaction();

/* Get current transction xid. */
Xid GetCurrentXid();

/* Commit transaction manually. */
void CommitTransaction();

/* Commit transaction automatically. */
void AutoCommitTransaction();

/* Rollback transaction. */
void RollbackTransaction();

/* Auto transaction rollback. */
void AutoRollbackTransaction();

/* Update transaction state. */
void UpdateTransactionState(Row *row, TransOpType trans_op_type);
