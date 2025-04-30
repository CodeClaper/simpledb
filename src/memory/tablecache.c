#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "tablecache.h"
#include "systable.h"
#include "spinlock.h"
#include "utils.h"
#include "mmgr.h"
#include "copy.h"
#include "free.h"
#include "trans.h"
#include "table.h"
#include "log.h"

/*
 * TableCache, 
 * which store the table in cache. 
 */
static List *TableCache;

/*
 * Table Cache lock.
 */
static s_lock *tlock;

static void CreateTableCache();
static void LoadObjectToTableCache();

/* Initialise table cache. */
void InitTableCache() {
    CreateTableCache();
    LoadObjectToTableCache();
}

/* Create the TableCache. */
static void CreateTableCache() {
    switch_shared();
    TableCache = create_list(NODE_TABLE);
    tlock = instance(s_lock);
    init_spin_lock(tlock);
    switch_local();
}

/* Load object and push to table cache. 
 * ----------------------------------
 * Note: Use SIDE_MEMORY_CONTEXT as the memory context 
 * when loading object. And delete the SIDE_MEMORY_CONTEXT
 * after loading.
 * */
static void LoadObjectToTableCache() {

    /* Swith to SIDE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    MemoryContextSwitchTo(SIDE_MEMORY_CONTEXT);

    List *obj_list = FindAllObject();

    ListCell *lc;
    foreach(lc, obj_list) {
        Object *entity = (Object *) lfirst(lc);
        if (entity->reltype == OTABLE) {
            Table *table = load_table(entity->oid);
            SaveTableCache(table);
        }
    }

    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(SIDE_MEMORY_CONTEXT);
}

/* Get all table cache. */
inline List *GetAllTableCache() {
    Assert(TableCache != NULL);
    return TableCache;
}

/* Save table cache. */
void SaveTableCache(Table *table) {
    /* Not allowed repeated. */
    AssertFalse(TableExistsInCache(GET_TABLE_OID(table)));

    acquire_spin_lock(tlock);
    switch_shared();

    /* Insert new table cache. */
    append_list(TableCache, copy_table(table));

    switch_local();
    release_spin_lock(tlock);
}

/* Find out if exists table in caceh. */
bool TableExistsInCache(Oid oid) {
    bool found = false;
    acquire_spin_lock(tlock);
    ListCell *lc;
    foreach (lc, TableCache) {
        Table *cur_table = (Table *) lfirst(lc);
        if (oid == GET_TABLE_OID(cur_table)) {
            found = true;
            break;
        }
    }
    release_spin_lock(tlock);
    return found;
}

/* Find out if exists table in caceh. */
bool TableNameExistsInCache(char *tableName) {
    bool found = false;
    acquire_spin_lock(tlock);
    ListCell *lc;
    foreach (lc, TableCache) {
        Table *cur_table = (Table *) lfirst(lc);
        if (streq(tableName, GET_TABLE_NAME(cur_table))) {
            found = true;
            break;
        }
    }
    release_spin_lock(tlock);
    return found;
}
 
/* Find table cache by oid, 
 * ------------------------
 * Return the found table cache, 
 * return null if not exist. */
Table *FindTableCache(Oid oid) {
    Table *found = NULL;
    acquire_spin_lock(tlock);
    ListCell *lc;
    foreach (lc, TableCache) {
        Table *cur_table = (Table *) lfirst(lc);
        Assert(cur_table);
        if (oid == GET_TABLE_OID(cur_table)) {
            found = cur_table;
            break;
        }
    }
    release_spin_lock(tlock);
    return found;
}

/* Find table cahce by table name. */
Table *NameFindTableCache(char *tableName) {
    Table *found = NULL;
    acquire_spin_lock(tlock);
    ListCell *lc;
    foreach (lc, TableCache) {
        Table *cur_table = (Table *) lfirst(lc);
        Assert(cur_table);
        if (streq(tableName, GET_TABLE_NAME(cur_table))) {
            found = cur_table;
            break;
        }
    }
    release_spin_lock(tlock);
    return found;
}

/* Remove table cache. */
void RemoveTableCache(Oid oid) {
    acquire_spin_lock(tlock);
    ListCell *lc;
    foreach (lc, TableCache) {
        Table *current = (Table *) lfirst(lc);
        if (oid == GET_TABLE_OID(current)) {
            switch_shared();
            list_delete(TableCache, current);
            free_table(current);
            switch_local();
        }
    }
    release_spin_lock(tlock);
}

