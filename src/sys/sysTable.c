#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include "sysTable.h"
#include "mmgr.h"
#include "data.h"
#include "const.h"
#include "defs.h"
#include "table.h"
#include "log.h"

/* System table meta column list. */
MetaColumn SYS_TABLE_COLUMNS[] = {
    { SYS_TABLE_OID_NAME, T_INT, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int32_t)), true, false, false, false, 0, 0},
    { SYS_TABLE_RELNAME_NAME, T_VARCHAR, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + MAX_COLUMN_SIZE), false, false, false, false, 0, 0},
    { SYS_TABLE_RELTYPE_NAME, T_INT, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int32_t)), false, false, false, false, 0, 0}
};

/* System reserved columns. */
MetaColumn SYS_RESERVED_COLUMNS[] = {
    { SYS_RESERVED_ID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 },
    { CREATED_XID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 },
    { EXPIRED_XID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 }
}; 

/* If system table file already exists, 
 * return true. 
 * */
static bool SysTableFileExists() {
    char sys_table_file[100];
    struct stat buf;

    memset(sys_table_file, 0, 100);
    sprintf(sys_table_file, "%s%d", conf->data_dir, SYS_ROOT_OID);

    return (stat(sys_table_file, &buf) == 0);
}

/* Create the system meta table. */
static MetaTable *CreateSysMetaTable() {
    int i;
    MetaTable *meta_table;

    meta_table = instance(MetaTable);
    meta_table->refId = SYS_ROOT_OID;
    meta_table->table_name = dstrdup(SYS_TABLE_NAME);
    meta_table->column_size = SYS_TABLE_COLUMNS_LENGTH;
    meta_table->all_column_size = SYS_TABLE_COLUMNS_LENGTH + SYS_RESERVED_COLUMNS_LENGTH;
    meta_table->meta_column = dalloc(sizeof(MetaColumn *) * meta_table->all_column_size);
    
    /* Define system table columns. */
    for (i = 0; i < SYS_TABLE_COLUMNS_LENGTH; i++) {
        MetaColumn *meta_column = instance(MetaColumn);
        memcpy(meta_column, SYS_TABLE_COLUMNS + i, sizeof(MetaColumn));
        meta_table->meta_column[i] = meta_column;
    }

    /* Define system reserved columns. */
    for (; i < SYS_TABLE_COLUMNS_LENGTH + SYS_RESERVED_COLUMNS_LENGTH; i++) {
        MetaColumn *meta_column = instance(MetaColumn);
        memcpy(meta_column, (SYS_RESERVED_COLUMNS + i - SYS_TABLE_COLUMNS_LENGTH), sizeof(MetaColumn));
        meta_table->meta_column[i] = meta_column;
    }

    return meta_table;
}

/* Create the sys table 
 * ---------------------
 * Called by db startup. 
 * Skip if already exists, otherwiese, create the sys table. 
 * Panic if fail.
 * */
void CreateSysTable() {
    MetaTable *sysMetaTable;

    /* Avoid repeat create system table. */
    if (SysTableFileExists())
        return;

    sysMetaTable = CreateSysMetaTable();
    if (!create_table(sysMetaTable))
        panic("Create system table fail");
}


/* RefIdFindObject
 * ------------------
 * The interface which find object by refId.
 * Panic if not found refId.
 * */
ObjectEntity RefIdFindObject(Oid refId) {
    ObjectEntity entity;

    return entity;
}


/* Find next Oid. */
Oid FindNextOid() {
    Oid noid;
    return noid;
}
