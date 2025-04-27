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
#include <string.h>
#include "xlog.h"
#include "log.h"
#include "mmgr.h"
#include "trans.h"
#include "copy.h"
#include "free.h"
#include "ltree.h"
#include "refer.h"
#include "table.h"
#include "select.h"
#include "asserts.h"
#include "utils.h"
#include "pager.h"


/* 
 * The XLogEntry Chain.
 */
static XLogEntry *XLHeader = NULL;

static void HeapInsertXLog(Refer *refer, TransEntry *transaction);
static void HeapDeleteXLog(Refer *refer, TransEntry *transaction);
static void HeadUpdateDeleteXlog(Refer *refer, TransEntry *transaction);

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
                HeadUpdateDeleteXlog(current->refer, trans);
                break;
            default:
                db_log(PANIC, "Unknown XLogHeapType.");
        }        
    }
}

/* Reverse insert operation. */
static void HeapInsertXLog(Refer *refer, TransEntry *transaction) {
    Row *row = define_row(refer);

    KeyValue *created_xid_col = lfirst(second_last_cell(row->data));
    KeyValue *expired_xid_col = lfirst(last_cell(row->data));
    Xid created_xid = *(Xid *)created_xid_col->value;
    Assert(created_xid == transaction->xid);

    /* Delete the insered row. */
    *(Xid *)expired_xid_col->value = transaction->xid;
}

/* Reverse delete operation. 
 * Notice that: when reverse delete operation, it does`t "resurrect" the row, 
 * rather than re-insert the row to keep the principle that visible row always 
 * lies in the forefront of the same key cells.
 * */
static void HeapDeleteXLog(Refer *refer, TransEntry *transaction) {
    Row *rawRow = define_row(refer);
    Assert(RowIsDeleted(rawRow));

    Row *newRow = copy_row(rawRow);
    KeyValue *expired_xid_col = lfirst(last_cell(newRow->data));
    Xid expired_xid = *(Xid *)expired_xid_col->value;
    Assert(expired_xid == transaction->xid);

    /* Make the row visible. */
    *(Xid *)expired_xid_col->value = 0;
    
    /* Repositioning. */
    Cursor *new_cur = define_cursor(
        open_table_inner(refer->oid), 
        newRow->key, false
    );

    /* Re-insert. */
    insert_leaf_node_cell(new_cur, newRow);

    free_row(newRow);
}

/* Reverse update delete transaction. */
static void HeadUpdateDeleteXlog(Refer *refer, TransEntry *transaction) {
    Row *rawRow = define_row(refer);
    Assert(RowIsDeleted(rawRow));

    Row *newRow = copy_row(rawRow);

    KeyValue *expired_xid_col = lfirst(last_cell(newRow->data));
    Xid expired_xid = *(Xid *)expired_xid_col->value;
    Assert(expired_xid == transaction->xid); 

    /* Make the row visible. */
    *(Xid *)expired_xid_col->value = 0;

    /* Repositioning. */
    Cursor *new_cur = define_cursor(
        open_table_inner(refer->oid), 
        newRow->key, false
    );
    Refer *new_ref = convert_refer(new_cur);

    /* Lock update refer. */
    add_refer_update_lock(new_ref);

    /* Re-insert. */
    insert_leaf_node_cell(new_cur, newRow);

    /* Free update refer lock. */
    free_refer_update_lock(new_ref);
}
