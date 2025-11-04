#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include "table.h"
#include "index.h"
#include "bufpool.h"
#include "sys.h"
#include "systable.h"
#include "mmgr.h"
#include "copy.h"
#include "free.h"
#include "tablecache.h"
#include "buftable.h"
#include "common.h"
#include "asserts.h"
#include "utils.h"
#include "meta.h"
#include "ltbase.h"
#include "ltindex.h"
#include "pager.h"
#include "log.h"
#include "tablelock.h"
#include "fdesc.h"
#include "compres.h"
#include "heaptable.h"

/* Get table file path. */
char *table_file_path(Oid oid) {
    char *file_path;

    file_path = dalloc(100);
    sprintf(file_path, "%s%ld", conf->data_dir, oid);

    return file_path;
}

/* Check table file if exist 
 * Return true if exist or false if not exist. */
bool table_file_exist(char *table_file_path) {
    struct stat buffer;
    return (stat(table_file_path, &buffer) == 0);
}

/* Check if table exist directly. */
bool check_table_exist_direct(Oid oid) {
    char *file_path = table_file_path(oid);
    return table_file_exist(file_path);
}

/* Check if table exists. */
bool check_table_exist(char *table_name) {
    Oid oid = TableNameFindOid(table_name);
    return check_table_exist_direct(oid);
}

/* Check if index exists. */
bool check_index_exist(char *index_name) {
    Oid oid = IndexNameFindOid(index_name);
    return check_table_exist_direct(oid);
}

/* Create a new table. */
bool create_table(Oid oid, MetaTable *meta_table) {
    char *file_path;
    int descr;
    void *root_node;
    uint32_t default_value_len;

    AssertFalse(ZERO_OID(oid));
    Assert(meta_table);

    file_path = table_file_path(oid);
    if (table_file_exist(file_path)) {
        db_log(ERROR, "Table '%s' already exists.", meta_table->table_name);
        dfree(file_path);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(ERROR, "Open database file '%s' fail.", file_path);
        dfree(file_path);
        return false;
    }

    root_node = dalloc(PAGE_SIZE);

    default_value_len = MetaTableCalcRowLenght(meta_table);

    /* Initialize root node */
    LeafNodeInitialize(root_node, default_value_len, true);

    /* Set meta column */
    RootNodeSetColumnSize(root_node, meta_table->all_column_size);
    
    /* Get default value cell. */
    void *default_value_dest = RootNodeGetDefaultValue(root_node);

    /* Serialize */
    uint32_t offset = 0;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        void *destination = MetaColumnSeriable(meta_column);
        RootNodeSetMetaColumn(root_node, __i, destination);
        if (meta_column->default_value_type == DEFAULT_VALUE)
            memcpy(default_value_dest + offset, meta_column->default_value, meta_column->column_length);
        else
            memset(default_value_dest + offset, 0, meta_column->column_length);
        offset += meta_column->column_length;
    }

    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    ssize_t w_size = write(descr, root_node, PAGE_SIZE);
    if (w_size == -1) {
        db_log(ERROR, "Write table meta info error and error message: %s.", strerror(errno));
        dfree(file_path);
        dfree(root_node);
        return false;
    }

    /* Close desription. */
    close(descr);

    /* Free memory. */
    dfree(file_path);
    dfree(root_node);

    return true;
}

/* Create a new table. */
bool shrink_table(Oid oid, MetaTable *meta_table) {
    Buffer root_buffer;
    void *root_node, *default_value_dest;
    uint32_t default_value_len;

    AssertFalse(ZERO_OID(oid));
    Assert(meta_table);
    
    /* Read buffer. */
    root_buffer = ReadBuffer(oid, ROOT_PAGE_NUM);
    LockBuffer(root_buffer, RW_WRITER);
    root_node = GetBufferPage(root_buffer);

    default_value_len = MetaTableCalcRowLenght(meta_table);

    /* Initialize root node */
    LeafNodeInitialize(root_node, default_value_len, true);

    /* Set meta column */
    RootNodeSetColumnSize(root_node, meta_table->all_column_size);

    /* Reset zero cells. */
    LeafNodeSetCellNum(root_node, default_value_len, 0);

    /* Get default value cell. */
    default_value_dest = RootNodeGetDefaultValue(root_node);

    /* Serialize */
    uint32_t offset = 0;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        void *destination = MetaColumnSeriable(meta_column);
        RootNodeSetMetaColumn(root_node, __i, destination);
        if (meta_column->default_value_type == DEFAULT_VALUE)
            memcpy(default_value_dest + offset, meta_column->default_value, meta_column->column_length);
        else
            memset(default_value_dest + offset, 0, meta_column->column_length);
        offset += meta_column->column_length;
    }
    
    /* Unlock and release buffer. */
    MakeBufferDirty(root_buffer);
    UnlockBuffer(root_buffer);
    ReleaseBuffer(root_buffer);

    return true;
}

/* Load meta index info by the toid. 
 * --------------------------------
 * Return list of meta index info of the table.
 * */
static List *LoadMetaIndex(Oid toid, Table *table) {
    List *indexs;
    List *meta_indexs;

    indexs = ToidFindIndexs(toid);
    meta_indexs = create_list(NODE_META_INDEX);

    ListCell *lc;
    foreach (lc, indexs) {
        Oid oid = *(Oid *) lfirst(lc);
        append_list(meta_indexs, IndexLoad(oid, table));
    }

    return meta_indexs;
}

/* Load column meta info by index. */
static MetaColumn *LoadMetaColumnByIndex(void *root_node, uint32_t index, uint32_t offset) {
    void *destination = RootNodeGetMetaColumn(root_node, index);
    MetaColumn *meta_column = MetaColumnDeseriable(destination);
    if (meta_column->default_value_type == DEFAULT_VALUE) {
        void *default_value_dest = RootNodeGetDefaultValue(root_node);
        meta_column->default_value = copy_value(default_value_dest + offset, meta_column->column_type);
    }
    return meta_column;
}

/* Load table meta info. */
MetaTable *LoadMetaTable(Oid oid) {
    Buffer buffer;
    void *root_node;
    uint32_t column_size;
    MetaTable *meta_table;

    buffer = ReadBuffer(oid, ROOT_PAGE_NUM);
    LockBuffer(buffer, RW_READERS);
    root_node = GetBufferPage(buffer);
    column_size = RootNodeGetColumnSize(root_node);

    meta_table = instance(MetaTable);
    meta_table->table_name = IS_SYS_ROOT(oid) ? dstrdup(SYS_TABLE_NAME) : OidFindRelName(oid);
    meta_table->column_size = 0;
    meta_table->all_column_size = 0;
    meta_table->meta_columns = create_list(NODE_META_COLUMN);

    uint32_t offset = 0;
    uint32_t i;
    for (i = 0; i < column_size; i++) {
        MetaColumn *current = LoadMetaColumnByIndex(root_node, i, offset);
        memcpy(current->own_table_name, meta_table->table_name, MAX_COLUMN_NAME_LEN);
        append_list(meta_table->meta_columns, current);
        /* Skip to system reserved column. */
        if (!current->sys_reserved)
            meta_table->column_size++;
        meta_table->all_column_size++;
        current->offset = offset;
        offset += current->column_length;
    }

    Assert(meta_table->all_column_size == column_size);

    /* Release the buffer. */
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return meta_table;
}

/* Load Table from disk. */
Table *load_table(Oid oid) {
    Table *table = instance(Table);
    table->oid = oid;
    table->root_page_num = ROOT_PAGE_NUM; 
    table->creator = getpid();
    table->meta_table = LoadMetaTable(oid);
    table->meta_indexs = LoadMetaIndex(oid, table);
    table->page_size = GetPageSize(oid);
    table->hoid = TableNameFindHeapOid(GET_TABLE_NAME(table));
    table->key_len = TableCalcPrimaryKeyLength(table);
    table->index_value_len = TableCalcIndexLength(table);
    table->heap_value_len = TableCalcRowLength(table);
    return table;
}

/* Open a table object. 
 * ---------------------
 * Return the found table or NULL if missing. 
 * */
Table *open_table_inner(Oid oid) {

    /* Check table if locked. 
     * Block here until acquire the table if locked. */
    check_table_locked(oid);

    /* Firstly, try to find in buffer. */
    Table *mtable = FindTableCache(oid);
    if (mtable != NULL)
        return mtable;

    try_acquire_table(oid);
    
    /* Double check to avoid other transaction save 
     * table cache before current transaction acquire the table lock. */
    mtable = FindTableCache(oid);
    if (mtable != NULL) {
        try_release_table(oid);
        return mtable;
    }

    /* Memory missing, get from disk. */
    if (!check_table_exist_direct(oid)) {
        try_release_table(oid);
        return NULL;
    }

    /* Load table from disk. */
    Table *table = load_table(oid);
    
    /* Save table cache. */
    SaveTableCache(table);
    
    /* Release table lock. */
    try_release_table(oid);

    /* Only return buffer table to keep the same table pointer 
     * in the same transaction. */
    return FindTableCache(oid);
}

/* Open a table object. 
 * -----------------------
 * Firstly, find int table cache, 
 * if missing, find in dish.
 * return null if all missing. 
 * */
Table *open_table(char *table_name) {
    Assert(table_name);
    Table *table;
    
    /* Find in table cache. */
    table = NameFindTableCache(table_name);
    if (table != NULL)
        return table;
    
    /* Find in disk. */
    Oid oid = TableNameFindOid(table_name);
    if (ZERO_OID(oid))
        return NULL;

    return open_table_inner(oid);
}


/* Drop an existed table. */
bool drop_table(char *table_name) {
    Oid oid, soid;

    /* Check if exist the table. */
    oid = TableNameFindOid(table_name);
    soid = StrTableNameFindOid(table_name);

    Assert(NON_ZERO_OID(oid));
    Assert(NON_ZERO_OID(soid));

    char *file_path = table_file_path(oid);
    if (!table_file_exist(file_path)) {
        dfree(file_path);
        return false;
    }

    /* Try to acquire the table lock. */
    try_acquire_table(oid);


    /* It will do:
     * (1) Remove table file from disk. 
     * (2) Remove related indexs.
     * (3) Remove systable object.
     * (4) Remove table cache.
     * (5) Remove table buffer. */
    if (
        remove(file_path) == 0 && 
        IndexDropByTableName(table_name) &&
        RemoveObject(oid) &&
        RemoveTableCache(oid) &&
        RemoveTableBuffer(oid)
    ) {
        /* Unregister fdesc. */
        unregister_fdesc(oid);
        unregister_fdesc(soid);
        /* Release the table lock. */
        try_release_table(oid);
        return true;
    }

    /* Release the table lock. */
    try_release_table(oid);
    
    /* Not reach here logically. */
    db_log(ERROR, "Table '%s' deleted fail, error: %s", 
           table_name, strerror(errno));

    return false;
}
