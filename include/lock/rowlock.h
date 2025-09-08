#include "data.h"
#include "spinlock.h"

typedef struct RowLockEntry {
    Oid oid;
    void *key;
    s_lock lock;
    Xid acquirer;
} RowLockEntry;

/* Init the row lock.*/
void InitRowLock();

/* Acquire the row lock, will block if fail. */
void AcquireRowLock(Oid oid, void *key);

/* Release the row lock. */
void ReleaseRowLock(Oid oid, void *key);

/* Release all row locks under current transaction. */
void ReleaseAllRowLock();

