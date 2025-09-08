#include <stdbool.h>
#include "rowlock.h"
#include "spinlock.h"
#include "refer.h"
#include "trans.h"
#include "mmgr.h"
#include "log.h"
#include "copy.h"
#include "table.h"
#include "meta.h"
#include "compare.h"

static Queue *RowLockTable;

/* Init the row lock.*/
void InitRowLock() {
    switch_shared();
    RowLockTable = CreateQueue(NODE_VOID);
    switch_local();
}

/* Find the row lock entry. 
 * Return NULL if not found.
 * */
static RowLockEntry *FindRowLock(Oid oid, void *key) {
    Table *table = open_table_inner(oid);
    MetaColumn *primary_meta_column = MetaTableFindPrimaryKey(table->meta_table);

    QueueCell *qc;
    qforeach(qc, RowLockTable) {
        RowLockEntry *rlock = (RowLockEntry *) qfirst(qc);
        if (rlock->oid == oid && EQ(rlock->key, key, primary_meta_column->column_type))
            return rlock;
    }
    return NULL;
}

/* Append new RowLock. */
static RowLockEntry *AppendNewRowLock(Oid oid, void *key) {
    Table *table = open_table_inner(oid);
    MetaColumn *primary_meta_column = MetaTableFindPrimaryKey(table->meta_table);

    switch_shared();
    RowLockEntry *rlock = instance(RowLockEntry);
    rlock->oid = oid;
    rlock->key = copy_value2(key, primary_meta_column);
    rlock->acquirer = GetCurrentXid();
    rlock->lock = SPIN_UN_LOCKED_STATUS;
    AppendQueue(RowLockTable, rlock);
    switch_local();

    return rlock;
}

/* The condition of RowLock. */
static inline bool RowLockCondition(RowLockEntry *rlock) {
    return rlock->acquirer == GetCurrentXid();
}

/* Acquire the row lock, will block if fail. */
void AcquireRowLock(Oid oid, void *key) {
    RowLockEntry *rlock;

    rlock = FindRowLock(oid, key);
    if (rlock != NULL) {
        if (!RowLockCondition(rlock))
            acquire_spin_lock(&rlock->lock);
    } else {
        rlock = AppendNewRowLock(oid, key);
        acquire_spin_lock(&rlock->lock);
    }
}

/* Release the row lock. */
void ReleaseRowLock(Oid oid, void *key) {
    RowLockEntry *rlock = FindRowLock(oid, key);
    Assert(rlock != NULL);
    Assert(rlock->acquirer == GetCurrentXid());
    release_spin_lock(&rlock->lock);
    DeleteQueue(RowLockTable, rlock);
}

/* Release all row locks under current transaction. */
void ReleaseAllRowLock() {
    Xid xid;
    QueueCell *qc;

    xid = GetCurrentXid();
    qforeach (qc, RowLockTable) {
        RowLockEntry *rlock = (RowLockEntry *) qfirst(qc);
        if (rlock->acquirer == xid) {
            release_spin_lock(&rlock->lock);
            DeleteQueue(RowLockTable, rlock);
        }
    }
}

