/*
 ********************************* Transaction Log Manager ************************************
 * Auth:        JerryZhou
 * Created:     2024/01/11
 * Modify:      2024/09/05
 * Locataion:   src/trans/xlog.c
 * Description: 
 *  Simpledb transaction log is Write-Ahead Log (WAL). Transaction Log records all transactions 
 *  and the database modifications that are made by each transaction. The transaction log is 
 *  a critical component of the database and, if there`s a system failure, the transaction
 *  log might be required to bring your database back to a consistent state.
 *  The Transaction log manager basically supports three functions:
 *  (1) Supports Transaction Roll Back.
 *  (2) Supports recovery data when server restart up.
 *  (3) Support Transaction replication in distributed cluster.
 * Besides:
 *  Transaction Log is stored in a FIFO XLogEntry chain and the memory is allocated by 
 *  the CACHE_MEMORY_CONTEXT memory context. It`s local memory and works in single thread of 
 *  the endback. So there are not the concurrent security issues.
 ***********************************************************************************************
 * */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <strings.h>
#include "xlog.h"
#include "log.h"
#include "mmgr.h"
#include "trans.h"
#include "copy.h"
#include "free.h"
#include "ltsearch.h"
#include "ltmodify.h"
#include "select.h"
#include "insert.h"
#include "utils.h"
#include "meta.h"
#include "tuple.h"
#include "table.h"
#include "heaptable.h"

/* 
 * The XLogEntry Chain.
 */
static XLogEntry *XLHeader = NULL;

/* Genrate new XLogEntry. */
static XLogEntry *NewXLogEntry(Xid xid, Refer *refer, XLogHeapType type) {
    XLogEntry *entry = instance(XLogEntry);
    entry->type = type;
    entry->next = NULL;
    entry->refer = copy_refer(refer);
    entry->xid = xid;
    return entry;
}

/* Record Xlog. */
void RecordXlog(Refer *refer, XLogHeapType type) {
    /* First, find current transaction and it should exist.*/
    TransEntry *trans = FindTransaction();
    Assert(trans != NULL);

    /* Auto-commit transaction not need to record. */
    if (!conf->auto_rollback && trans->auto_commit)
        return;

    /* Switch to CACHE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

    XLogEntry *entry = NewXLogEntry(trans->xid, refer, type);
    entry->next = XLHeader;
    XLHeader = entry;
    
    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
}

/* Update xlog entry refer. */
void UpdateXlogEntryRefer(ReferUpdateEntity *refer_update_entity) {
    if (XLHeader) {
        for (XLogEntry *current = XLHeader; current != NULL; current = current->next) {
            Refer *current_refer = current->refer;
            if (refer_equals(current_refer, refer_update_entity->old_refer)) {
                /* Switch to CACHE_MEMORY_CONTEXT. */
                MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
                MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

                current->refer = copy_refer(refer_update_entity->new_refer);
                free_refer(current_refer);

                /* Recover the MemoryContext. */
                MemoryContextSwitchTo(oldcontext);
            }
        }  
    }
}

/* Commit XLog . */
void CommitXlog() {
    /* Switch to CACHE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

    /* Free memory. */
    free_xlog_entry(XLHeader);
    XLHeader = NULL;

    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
}


/* Reverse insert operation. */
static void HeapInsertXLog(Refer *refer, TransEntry *transaction) {
    Table *table;
    void *key, *index;

    table = open_table_inner(refer->oid);

    /* Get btree key and value. */
    key = BtreeSearchKeyViaRefer(refer);
    index = BtreeSearchValueViaRefer(refer);

    /* Update heap table exipred xid. */
    HeapTableUpdateRowExpiredXid(table, (Refer *) index, transaction->xid);

    /* Update btree expired xid. */
    BtreeModifyExpiredXid(refer->oid, key, transaction->xid);
}

/* Reverse delete operation. 
 * -------------------------
 * Notice that: when reverse delete operation, it does`t "resurrect" the row, 
 * rather than re-insert the row to keep the principle that visible row always 
 * lies in the forefront of the same key cells.
 * */
static void HeapDeleteXLog(Refer *refer, TransEntry *transaction) {
    Table *table;
    void *key, *tuple, *new_tuple;
    Xid created_xid, expired_xid;

    table = open_table_inner(refer->oid);
    tuple = DefineTuple(refer);
    created_xid = TupleFindCreatedXid(tuple, table->meta_table);
    expired_xid = TupleFindExpiredXid(tuple, table->meta_table);
    Assert(expired_xid == transaction->xid); 
    AssertFalse(IsVisibleInner(created_xid, expired_xid, transaction));

    key = TupleFindKey(tuple, table);
    new_tuple = copy_block(tuple, table->heap_value_len);
    TupleSetExpiredXid(new_tuple, table->meta_table, 0);
    
    /* Reinsert. */
    InsertForTuple(refer->oid, key, new_tuple);
}


/* Execute rollback. */
void ExecuteRollback() {
    /* First, find current transaction and it should exist.*/
    TransEntry *trans = FindTransaction();
    Assert(trans != NULL);

    /* XLHeader might be NULL, when there is no XLogs. */
    if (XLHeader == NULL) 
        return;
    
    /* Loop to rollback. */
    for (XLogEntry *current = XLHeader; current != NULL; current = current->next) {
        switch (current->type) {
            case HEAP_INSERT:
                HeapInsertXLog(current->refer, trans);
                break;
            case HEAP_DELETE:
                HeapDeleteXLog(current->refer, trans);
                break;
            case HEAP_UPDATE_INSERT:
                HeapInsertXLog(current->refer, trans);
                break;
            case HEAP_UPDATE_DELETE:
                HeapDeleteXLog(current->refer, trans);
                break;
            default:
                db_log(PANIC, "Unknown XLogHeapType.");
        }        
    }
}
