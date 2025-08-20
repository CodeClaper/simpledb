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
#include "sys.h"
#include "systable.h"
#include "mmgr.h"
#include "free.h"
#include "tablecache.h"
#include "buftable.h"
#include "common.h"
#include "asserts.h"
#include "utils.h"
#include "meta.h"
#include "ltree.h"
#include "pager.h"
#include "log.h"
#include "tablelock.h"
#include "index.h"
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
    initial_leaf_node(root_node, default_value_len, true);

    /* Set meta column */
    set_column_size(root_node, meta_table->all_column_size);
    
    /* Get default value cell. */
    void *default_value_dest = get_default_value_cell(root_node);

    /* Serialize */
    uint32_t offset = 0;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        void *destination = serialize_meta_column(meta_column);
        set_meta_column(root_node, destination, __i);
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
        db_log(ERROR, "Write table meta info error and errno %d.\n", errno);
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

/* Get Column Position. */
static int get_column_position(MetaTable *meta_table, ColumnPositionDef *pos_def) {
    /* If not ColumnPositionDef, append column at last. */
    if (IsNull(pos_def))
        return meta_table->column_size;

    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (StrEq(meta_column->column_name, pos_def->column)) {
            switch (pos_def->type) {
                case POS_BEFORE:
                    return __i;
                case POS_AFTER:
                    return __i + 1;
            }
        }
    }

    db_log(ERROR, "Column '%s' not exists in table '%s'.", 
           pos_def->column, 
           meta_table->table_name);

    return -1;
}

/* Add new MetaColumn to table.
 * ---------------------------
 * This function is actually bottom-level routine for alter-table-add-column action. */
bool add_new_meta_column(char *table_name, MetaColumn *new_meta_column, ColumnPositionDef *post_def) {
    Table *table;
    int pos;

    table = open_table(table_name);
    pos = get_column_position(table->meta_table, post_def);

    /* Append index table new column. */
    append_new_column(table->root_page_num, table, new_meta_column, pos);

    /* Append heap table new column. */
    HeapTableAppendColumn(table, new_meta_column, pos);

    return true;
}

/* Drop table`s meta_column. */
bool drop_meta_column(char *table_name, char *column_name) {
    Table *table = open_table(table_name);
    int pos = NameFindMetaColumnPostion(table->meta_table, column_name);
    MetaColumn *oldColumn = NameFindMetaColumn(table->meta_table, column_name);
    Assert(pos >= 0);
    /* Drop index table column. */
    drop_column(table->root_page_num, table, pos);
    /* Drop heap table column. */
    HeapTableDropColumn(table, oldColumn, pos);
    return true;
}


/* Load Table from disk. */
Table *load_table(Oid oid) {
    /* New table. */
    Table *table = instance(Table);
    /* Define root page is first page. */
    table->oid = oid;
    table->root_page_num = ROOT_PAGE_NUM; 
    table->creator = getpid();
    table->meta_table = GenerateMetaTable(oid);
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

    /* Unregister fdesc. */
    unregister_fdesc(oid);
    unregister_fdesc(soid);

    /* Disk remove. */
    if (remove(file_path) == 0 && RemoveObject(oid)) {
        /* Remove table cache. */
        RemoveTableCache(oid);
        /* Remove table buffer. */
        RemoveTableBuffer(oid);
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
