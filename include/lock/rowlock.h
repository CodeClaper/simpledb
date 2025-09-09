#include "data.h"
#include "spinlock.h"

typedef struct RowLockEntry {
    Refer *refer;
    s_lock lock;
    Xid acquirer;
} RowLockEntry;

/* Init the row lock.*/
void InitRowLock();

/* Acquire the row lock, will block if fail. */
void AcquireRowLock(Refer *refer, void *key);

/* Release all row locks under current transaction. */
void ReleaseAllRowLock();

/* Update the row lock refer. */
void UpdateRowLockRefer(Refer *oldRefer, Refer *newRefer);
