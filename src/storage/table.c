#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
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
#include "trans.h"
#include "compres.h"
#include "heaptable.h"
#include "sidcreate.h"
#include "ridcreate.h"

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
    if (ZERO_OID(oid)) return false;
    else return check_table_exist_direct(oid);
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
        logger(ERROR, "Table '%s' already exists.", meta_table->table_name);
        dfree(file_path);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        logger(ERROR, "Open database file '%s' fail.", file_path);
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
        logger(ERROR, "Write table meta info error and error message: %s.", strerror(errno));
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

/* Shrink the main table. */
static bool ShinkMainTable(Oid oid, MetaTable *meta_table) {
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

/* Create a new table. 
 * Will do these:
 * (1) Reset main table page.
 * (2) Reset rid table page.
 * (3) Shrink main table.
 * (4) Shrink rid table.
 * */
bool ShrinkTable(Table *table) {
    uint32_t page_num;
    for (page_num = ROOT_PAGE_NUM;  page_num < table->page_size; page_num++) ResetPage(table->oid, page_num);
    for (page_num = ROOT_PAGE_NUM;  page_num < table->sid_page_size; page_num++) ResetPage(table->soid, page_num);
    for (page_num = ROOT_PAGE_NUM;  page_num < table->rid_page_size; page_num++) ResetPage(table->roid, page_num);
    table->page_size = 1; table->sid_page_size = 1; table->rid_page_size = 1;
    return ShinkMainTable(table->oid, table->meta_table) && ShrinkSidTable(table->soid) && ShrinkRidTable(table->roid);
}

/* Load primary meta index info. */
static MetaIndex *LoadPrimaryMetaIndex(Table *table) {
    MetaIndex *meta_index = instance(MetaIndex);

    meta_index->oid = table->oid;
    meta_index->tid = table->oid;
    meta_index->index_name = FormatStr("%s_pri_index", GET_TABLE_NAME(table));
    meta_index->type = BTREE_INDEX;
    meta_index->is_pri = true;
    meta_index->is_user = UserPrimaryKeyExists(table->meta_table);
    meta_index->is_unique = true;
    meta_index->page_num = GetPageSize(table->oid);
    meta_index->value_len = table->index_value_len;
    meta_index->column_size = 0;
    meta_index->key_len = 0;
    meta_index->meta_columns = create_list(NODE_META_COLUMN);

    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        if (meta_column->is_primary) {
            append_list(meta_index->meta_columns, meta_column);
            meta_index->key_len += meta_column->column_length;
            meta_index->column_size++;
            meta_index->key_len += meta_column->column_length;
        }
    }

    return meta_index;
}

/* Load meta index info by the toid. 
 * --------------------------------
 * Return list of meta index info of the table.
 * */
List *LoadMetaIndex(Oid toid, Table *table) {
    List *indexs;
    List *meta_indexs;

    indexs = ToidFindIndexs(toid);
    meta_indexs = create_list(NODE_META_INDEX);

    /* Load primary meta index. */
    append_list(meta_indexs, LoadPrimaryMetaIndex(table));

    /* Load non-primary meta index. */
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
        current->tid = oid;
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
    table->hoid = ToidFindHoid(oid);
    table->soid = ToidFindSoid(oid);
    table->roid = ToidFindRoid(oid);
    table->stid = ToidFindStoid(oid);
    table->aoid = ToidFindAoid(oid);
    table->root_page_num = ROOT_PAGE_NUM; 
    table->creator = getpid();
    table->meta_table = LoadMetaTable(oid);
    table->meta_indexs = LoadMetaIndex(oid, table);
    table->page_size = GetPageSize(oid);
    table->sid_page_size = GetPageSize(table->soid);
    table->rid_page_size = GetPageSize(table->roid);
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

static bool DropTableFromDisk(void *arg) {
    return remove((char *)arg) == 0;
}

/* Drop an existed table. */
bool drop_table(char *table_name) {
    Oid oid;

    /* Check if exist the table. */
    oid = TableNameFindOid(table_name);
    Assert(NON_ZERO_OID(oid));

    char *file_path = table_file_path(oid);
    if (!table_file_exist(file_path)) {
        dfree(file_path);
        return false;
    }

    /* It will do:
     * (1) Remove systable object.
     * (3) Remove table cache.
     * (4) Remove table buffer. 
     * (5) Remove table file from disk. 
     * */
    if (
        RemoveObject(oid) &&
        RemoveTableCache(oid) &&
        RemoveTableBuffer(oid) &&
        RegisterCommitEvent(DropTableFromDisk, file_path)
    ) {
        /* Unregister fdesc. */
        unregister_fdesc(oid);
        return true;
    }

    /* Not reach here logically. */
    logger(ERROR, "Table '%s' deleted fail, error: %s", 
           table_name, strerror(errno));

    return false;
}
