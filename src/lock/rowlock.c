#include <stdbool.h>
#include "rowlock.h"
#include "bufpool.h"
#include "spinlock.h"
#include "refer.h"
#include "trans.h"
#include "mmgr.h"
#include "log.h"
#include "copy.h"
#include "table.h"
#include "meta.h"
#include "ltbase.h"
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
static RowLockEntry *FindRowLock(Refer *refer) {
    QueueCell *qc;
    qforeach(qc, RowLockTable) {
        RowLockEntry *rlock = (RowLockEntry *) qfirst(qc);
        if (ReferIsEqual(refer, rlock->refer)) return rlock;
    }
    return NULL;
}

/* Append new RowLock. */
static RowLockEntry *AppendNewRowLock(Refer *refer) {
    RowLockEntry *rlock;

    switch_shared();
    rlock = instance(RowLockEntry);
    rlock->refer = copy_refer(refer);
    rlock->acquirer = GetCurrentXid();
    rlock->lock = SPIN_LOCKED_STATUS;
    AppendQueue(RowLockTable, rlock);
    switch_local();

    return rlock;
}

/* Acquire the row lock, will block if fail. */
void AcquireRowLock(Refer *refer, void *key) {
    Table *table;
    MetaColumn *pmeta_column;
    Buffer buffer;
    void *block, *target_key;
    RowLockEntry *rlock;
    Xid current_xid;

    table = open_table_inner(refer->oid);
    pmeta_column = MetaTableFindPrimaryKey(table->meta_table);
    buffer = ReadBuffer(refer->oid, refer->page_num);
    current_xid = GetCurrentXid();
    LockBuffer(buffer, RW_WRITER);

    block = GetBufferPage(buffer);
    target_key = LeafNodeGetCellKey(block, table->key_len, table->index_value_len, 
                                    table->heap_value_len, refer->cell_num);
    /* If not found, append new one. */
    rlock = FindRowLock(refer);
    if (rlock == NULL)
        rlock = AppendNewRowLock(refer);

    /* The scope of lock reaches there. */
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    /* No key conflict. */
    if (NE(key, target_key, pmeta_column->column_type)) return;

    /* Allow reenter. */
    if (rlock->acquirer == current_xid) return;

    /* Block if fail. */
    acquire_spin_lock(&rlock->lock);
    rlock->acquirer = GetCurrentXid();
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

/* Update the row lock refer. */
void UpdateRowLockRefer(Refer *oldRefer, Refer *newRefer) {
    RowLockEntry *rlock = FindRowLock(oldRefer);
    if (rlock != NULL) {
        rlock->refer->oid = newRefer->oid;
        rlock->refer->page_num = newRefer->page_num;
        rlock->refer->cell_num = newRefer->cell_num;
    }
}
