/********************************************* Tabble Lock Module ***********************************************
 * Auth:        JerryZhou
 * Created:     2024/08/10
 * Modify:      2024/11/28
 * Locataion:   src/lock/tablelock.c
 * Description: This module supports the table-level lock mechanism which depends on spinlock at the bottom layer.
 * The table-level lock is an exclusive lock which means only one thread could acquire the table at the time. 
 * Ony DDL statements executor will try to acquire the lock via the function <try_acquire_table>.
 * Other DML or DQL statement executors need to check the table if locked.
 *****************************************************************************************************************
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "tablelock.h"
#include "utils.h"
#include "mmgr.h"

/* The list stores TableLockEntity. */
static List *lock_list;

/* Initiliaze the table lock. */
void init_table_lock() {
    switch_shared();
    lock_list = create_list(NODE_VOID);
    switch_local();
}

/* Find TableLockEntity in list.
 * Return NULL if not found. */
static TableLockEntity *find_lock_entry(Oid oid) {
    ListCell *lc;
    foreach (lc, lock_list) {
        TableLockEntity *current = lfirst(lc);
        if (oid == current->oid)
            return current;
    }
    return NULL;
}

/* Register TableLockEntity. */
static TableLockEntity *register_lock_entry(Oid oid) {
    TableLockEntity *lock_entry;

    switch_shared();

    lock_entry = instance(TableLockEntity);
    lock_entry->oid = oid;
    lock_entry->entry_lock = instance(ExLockEntry);
    init_exlock(lock_entry->entry_lock);
    append_list(lock_list, lock_entry);

    switch_local();

    return lock_entry;
}

/* Check table if locked. 
 * ---------------------
 * Firstly, find TableLockEntity in list and check if exist. 
 * If there is not TableLockEntity, just return, 
 * otherwise, try to acqurie the lock. 
 * */
void check_table_locked(Oid oid) {
    AssertFalse(ZERO_OID(oid));
    TableLockEntity *lock_entry = find_lock_entry(oid);
    if (lock_entry) {
        /* Try to check exlock if unlolocked, maybe block here when locked. */
        wait_for_exlock(lock_entry->entry_lock);   
    }
}

/* Try to acquire the table. 
 * -------------------------
 * First, find TableLockEntity and if not exists, register one.
 * Second, try to acquire the table lock.
 * After acquired the lock, wait unitl there is no other threads manipulating the table. 
 * At the end, acquire the table successfully and exclusively.
 * */
void try_acquire_table(Oid oid) {
    TableLockEntity *lock_entry = find_lock_entry(oid);
    if (is_null(lock_entry)) {
       lock_entry = register_lock_entry(oid);
    }
    
    /* Make sure TableLockEntity exists. */
    Assert(lock_entry);
    
    /* Try acquire exclusive lock, maybe block here. */
    acquire_exlock(lock_entry->entry_lock);
}

/* Try to release the table. */
void try_release_table(Oid oid) {

    TableLockEntity *lock_entry = find_lock_entry(oid);

    /* Make sure exists. */
    Assert(lock_entry);

    /* Release lock. */
    release_exlock(lock_entry->entry_lock);
}

