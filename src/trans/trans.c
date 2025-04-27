/*
 *************************************************** Transaction Manager ********************************************************************************************************
 * Auth:        JerryZhou
 * Created:     2023/12/26
 * Modify:      2024/09/13
 * Locataion:   src/trans/trans.c
 * Description:
 *  If you are unfamiliar with transaction and MVCC, this article (https://levelup.gitconnected.com/implementing-your-own-transactions-with-mvcc-bba11cab8e70) will help a lot.
 *  Simply put, the implement of MVCC depends on two system reserved columns, created_xid and expired_xid.
 *  System reserved means it is only visible for system, not accessible for user and are always at the end of row.
 *  [created_xid] stores the id of transaction which create the inserted row.
 *  [expired_xid] stores the id of transaction which delete the row. [expired_xid] always is zero until deleted.
 *
 *  Before talking about how to implement MVCC, we need to know what is visible row and what is locked row.
 *  Visible row is the row who is accessible for current transaction. 
 *  Visible row must satisfy any of the follows conditions:
 *  (1) the current transaction create the row, and the row is not deleted.
 *  (2) other transaction creates the row, and transaction is committed and the row is not deleted.
 *  (3) the row is deleted by another uncommitted transaction (which not creates the row)  
 *  What is locked row. Firstly locked row is visible for at least row transaction.
 *  When the first transaction manipulating the row, other transaction will block until the first transaction committed.
 *
 *  There are four transaction operation types: TR_INSERT, TR_SELECT, TR_DELETE, TR_UPDATE;
 *  For insert operation, created_xid of new row stores the current transaction id and expired_xid stores zore;
 *  For select operation, the current transaction will only accesses visible rows.
 *  For delete operation, expired_xid of deleted row stores the current transaction id.
 *  For update, there is no need to manipulate deletion separately. We can simply treat update operations as delete operations 
 *  and add operations.
 *  Transaction isolation levels are a measure of the extent to which transaction isolation succeeds. 
 *  In particular, transaction isolation levels are defined by the presence or absence of the following phenomena:
 *  (1)  Dirty Reads: A dirty read occurs when a transaction reads data that has not yet been committed. 
 *       For example, suppose transaction 1 updates a row. Transaction 2 reads the updated row before transaction 1 commits the update. 
 *       If transaction 1 rolls back the change, transaction 2 will have read data that is considered never to have existed.
 *  (2)  Nonrepeatable Reads: A nonrepeatable read occurs when a transaction reads the same row twice but gets different data each time. 
 *       For example, suppose transaction 1 reads a row. Transaction 2 updates or deletes that row and commits the update or delete. 
 *       If transaction 1 rereads the row, it retrieves different row values or discovers that the row has been deleted.
 *  (3)  Phantoms: A phantom is a row that matches the search criteria but is not initially seen. 
 *       For example, suppose transaction 1 reads a set of rows that satisfy some search criteria. 
 *       Transaction 2 generates a new row (through either an update or an insert) that matches the search criteria for transaction 1. 
 *       If transaction 1 reexecutes the statement that reads the rows, it gets a different set of rows.
 *  The four transaction isolation levels (as defined by SQL-92) are defined in terms of these phenomena. 
 *  In the following table, an "X" marks each phenomenon that can occur.
 *  -----------------------------------------------------------------------------------------------
 *  - Transaction isolation level        Dirty reads         Nonrepeatable reads         Phantoms -
 *  -----------------------------------------------------------------------------------------------
 *  - Read uncommitted                   X                   X                           X        -
 *  -----------------------------------------------------------------------------------------------
 *  - Read committed                     --                  X                           X        -
 *  -----------------------------------------------------------------------------------------------
 *  - Read uncommitted                   --                  --                          X        -
 *  -----------------------------------------------------------------------------------------------
 *  - Read uncommitted                   --                  --                          --       -
 *  -----------------------------------------------------------------------------------------------
 *
 *******************************************************************************************************************************************************************/

#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "data.h"
#include "defs.h"
#include "trans.h"
#include "mmgr.h"
#include "shmem.h"
#include "log.h"
#include "copy.h"
#include "free.h"
#include "timer.h"
#include "refer.h"
#include "utils.h"
#include "xlog.h"
#include "asserts.h"
#include "instance.h"
#include "spinlock.h"

/* 
 * The trans chain stores active transactions. 
 * Note: The header of chain does not store any info.
 */
static TransEntry *xheader; 

/*
 * Transaction sync lock
 */
static s_lock *xlock;

static void CreateXlock();
static void *NewTransEntry(Xid xid, Pid pid, bool auto_commit, TransEntry *next);

/* Initialise transaction. */
void InitTrans() {
    xheader = NewTransEntry(0, 0, false, NULL);
    CreateXlock();
}

/* Generate new TransEntry. */
static void *NewTransEntry(Xid xid, Pid pid, bool auto_commit, TransEntry *next) {
    switch_shared();
    TransEntry *entry = instance(TransEntry);
    entry->xid = xid;
    entry->pid = pid;
    entry->auto_commit = auto_commit;
    entry->next = next;
    switch_local();
    return entry;
}


/* Create the xlock. */
static void CreateXlock() {
    switch_shared();
    xlock = instance(s_lock);
    init_spin_lock(xlock);
    switch_local();
}

/* Generate next xid. 
 * return next xid, if return -1, there is an error. */
static inline Xid NextXid() {
    return get_current_sys_time(NANOSECOND);
}

/* Any running transaction. */
bool inline AnyTransactionRunning() {
    return !is_null(xheader->next);
}

/* Get the tail of TransEntry list. */
static void *GetTransTail() {
    TransEntry *entry = xheader;
    while (entry->next) 
        entry = entry->next;
    return entry;
}

/* Find transaction handle by thread id. 
 * If not found, return NULL. */
TransEntry *FindTransaction() {
    /* Lock here to avoid the trans change when reading. */
    acquire_spin_lock(xlock);
    /* Current thread id. */
    Pid pid = getpid();
    Pid ppid = getppid();

    TransEntry *current = xheader;
    while ((current = current->next)) {
        if (current->pid == pid || current->pid == ppid) 
            break;
    }

    release_spin_lock(xlock);
    return current;
}


/* Get current transction xid. */
Xid GetCurrentXid() {
    TransEntry *transEntity = FindTransaction();
    Assert(transEntity);
    return transEntity->xid;
}


/* Register transaction. */
static void RegisterTransaction(TransEntry *entry) {
    Assert(entry != NULL);
    /* New TransEntry should all be shared memory pointer. */
    Assert(shmem_addr_valid(entry));
    
    acquire_spin_lock(xlock);

    TransEntry *tail = GetTransTail();
    Assert(tail != NULL);
    tail->next = entry;

    release_spin_lock(xlock);
}

/* Destroy transaction. */
static void DestroyTransaction() {
    acquire_spin_lock(xlock);
    switch_shared();

    TransEntry *current = xheader;
    TransEntry *pres = xheader;
    while ((current = current->next)) {
        if (current->pid == getpid()) {
            pres->next = current->next;
            dfree(current);
        } else
            pres = current;
    }

    switch_local();
    release_spin_lock(xlock);
}

/* Check if a transaction is active()
 * Active transaction is an uncommitted transaction. */
static bool IsActive(Xid xid) {
    bool active = false;
    /* Lock here is necessary to avoid to read 
     * the trans chain while it is being modified. */
    acquire_spin_lock(xlock);
    TransEntry *current;
    for (current = xheader->next; current != NULL; current = current->next) {
        Assert(shmem_addr_valid(current));
        if (current->xid == xid) {
            active = true;
            break;
        }
    }
    release_spin_lock(xlock);
    return active;
}

/* New transaction which will be committed automatically. */
void AutoBeginTransaction() {
    TransEntry *entry = FindTransaction();

    /* Already exists transaction, no need to auto begin transaction. */
    if (entry != NULL) return;

    /* Generate new transaction. */
    entry = NewTransEntry(NextXid(), getpid(), true, NULL);
    db_log(INFO, "Auto begin transaction xid: %"PRId64" and pid: %"PRId64".", 
           entry->xid, entry->pid);

    /* Register the transaction. */
    RegisterTransaction(entry);
}

/* Begin a new transaction which need be committed manually. */
void BeginTransaction() {
    TransEntry *entry = FindTransaction();
    Assert(!entry);

    /* Generate new transaction. */
    entry = NewTransEntry(NextXid(), getpid(), false, NULL);

    /* Register the transaction. */
    RegisterTransaction(entry);

    /* Send message. */
    db_log(SUCCESS, "Begin new transaction xid:%"PRId64" and pid: %"PRId64"successfully.",
           entry->xid, entry->pid);
}

/* Commit transaction manually. */
void CommitTransaction() {
    TransEntry *entry = FindTransaction();
    if (is_null(entry) || entry->auto_commit)
        db_log(ERROR, "Not in any transaction, please begin a transaction");

    /* Commit Xlog. */
    CommitXlog();

    /* Destroy transaction. */
    DestroyTransaction(); 
    db_log(
        INFO,
        "Commit the transaction xid: %"PRId64" successfully.", 
        entry->xid
    );
    db_log(SUCCESS, "Commit the transaction successfully");
}

/* Commit transaction automatically. */
void AutoCommitTransaction() {
    TransEntry *entry = FindTransaction();

    /* Only deal with auto-commit transaction. */
    if (entry != NULL && entry->auto_commit) {

        /* Commit Xlog. */
        CommitXlog();

        /* Destroy transaction. */
        DestroyTransaction();
        db_log(
            SUCCESS, 
            "Auto commit the transaction xid: %"PRId64" successfully.", 
            entry->xid
        );
    }
}

/* Tranasction rollback. */
void RollbackTransaction() {
    TransEntry *entry = FindTransaction();
    if (is_null(entry) || entry->auto_commit)
        db_log(ERROR, "Not in any transaction, please begin a transaction");

    ExecuteRollback();
    CommitTransaction();
    db_log(
        SUCCESS, 
        "Transaction xid: %"PRId64" rollbacked and commited successfully.", 
        entry->xid
    );
}


/* Auto transaction rollback. */
void AutoRollbackTransaction() {
    if (conf->auto_rollback) {
        /* Return if not exists transaction. */
        TransEntry *entry = FindTransaction();
        if (is_null(entry))
            return;

        /* Rollback Xlog. */
        ExecuteRollback();
        db_log(
            INFO, 
            "Transaction xid: %"PRId64" rollbacked and commited successfully.", 
            entry->xid
        );
    }
    
}

/* 
 * Check if row is visible for current transaction. 
 * Visible row must satisfy any of the follows conditions:
 * (1) the current transaction create the row, and the row is not deleted.
 * (2) other transaction creates the row, and transaction is committed and the row is not deleted.
 * (3) the row is deleted by another uncommitted transaction (which not creates the row)
 * */
bool IsVisible(Xid created_xid, Xid expired_xid) {
    Assert(created_xid != XID_NIL);
    /* Get current transaction. */
    TransEntry *entry = FindTransaction();
    Assert(entry != NULL);

    /* If satisfy above three conditions, 
     * row is visible for current transaction. */
    if (created_xid == entry->xid && 
            expired_xid == 0)
        return true;
    if (created_xid != entry->xid && 
            !IsActive(created_xid) && 
                expired_xid == 0)
        return true;
    if (expired_xid != 0 && 
            expired_xid != entry->xid && 
                IsActive(expired_xid) && 
                    created_xid != expired_xid)
        return true;
    
    /* Else not visible. */
    return false;
}

/* 
 * Check if row is visible for current transaction. 
 * ------------------------------------------------
 * Visible row must satisfy any of the follows conditions:
 * (1) the current transaction create the row, and the row is not deleted.
 * (2) other transaction creates the row, and transaction is committed and the row is not deleted.
 * (3) the row is deleted by another uncommitted transaction (which not creates the row)
 * */
bool RowIsVisible(Row *row) {
    /* Get current transaction. */
    TransEntry *entry = FindTransaction();
    Assert(entry != NULL);

    /* Get row created_xid and expired_xid. */
    KeyValue *created_xid_col = lfirst(second_last_cell(row->data));
    KeyValue *expired_xid_col = lfirst(last_cell(row->data));
    Xid row_created_xid = *(Xid *)created_xid_col->value;
    Xid row_expired_xid = *(Xid *)expired_xid_col->value;

    /* If satisfy above three conditions, 
     * row is visible for current transaction. */
    if (row_created_xid == entry->xid && 
            row_expired_xid == 0)
        return true;
    if (row_created_xid != entry->xid && 
            !IsActive(row_created_xid) && 
                row_expired_xid == 0)
        return true;
    if (row_expired_xid != 0 && 
            row_expired_xid != entry->xid && 
                IsActive(row_expired_xid) && 
                    row_created_xid != row_expired_xid)
        return true;
    
    /* Else not visible. */
    return false;
}

/* Check if a row has been deleted. */
bool RowIsDeleted(Row *row) {
    KeyValue *expired_xid_col = lfirst(last_cell(row->data));
    Xid row_expired_xid = *(Xid *)expired_xid_col->value;
    return row_expired_xid != 0;
}

/* Transaction operation for insert row. */
static void TransactionInsertRow(Row *row) {
    /* Get current transaction. */
    TransEntry *entry = FindTransaction();
    Assert(entry != NULL);

    /* For created_xid */
    KeyValue *created_xid_col = new_key_value(
        dstrdup(CREATED_XID_COLUMN_NAME), 
        copy_value(&entry->xid, T_LONG), 
        T_LONG
    );
    lfirst(second_last_cell(row->data)) = created_xid_col;

    /* For expired_xid */
    Xid zero = 0;
    KeyValue *expired_xid_col = new_key_value(
        dstrdup(EXPIRED_XID_COLUMN_NAME),
        copy_value(&zero, T_LONG),
        T_LONG
    );
    lfirst(last_cell(row->data)) = expired_xid_col;
}

/* Tranasction operation for delete row. */
static void TransactionDeleteRow(Row *row) {
    /* Get current transaction. */
    TransEntry *entry = FindTransaction();
    Assert(entry != NULL);

    /* Asssign current transaction id to expired_xid. */
    KeyValue *expired_xid_col = lfirst(last_cell(row->data));
    *(Xid *)expired_xid_col->value = entry->xid;
}

/* Update transaction state. */
void UpdateTransactionState(Row *row, TransOpType trans_op_type) {
    switch(trans_op_type) {
        case TR_SELECT:
            break;
        case TR_INSERT:
            TransactionInsertRow(row);  
            break;
        case TR_DELETE:
            TransactionDeleteRow(row);
            break;
        case TR_UPDATE:
            break;
    }
}
