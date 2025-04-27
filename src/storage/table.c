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

    default_value_len = calc_table_row_length2(meta_table);

    /* Initialize root node */
    initial_leaf_node(root_node, default_value_len, true);

    /* Set meta column */
    set_column_size(root_node, meta_table->all_column_size);
    
    /* Get default value cell. */
    void *default_value_dest = get_default_value_cell(root_node);

    /* Serialize */
    uint32_t i, offset = 0;
    for (i = 0; i < meta_table->all_column_size; i++) {
        MetaColumn *meta_column = (MetaColumn *) (meta_table->meta_column[i]);
        void *destination = serialize_meta_column(meta_column);
        set_meta_column(root_node, destination, i);
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

    /* Save to table cache. */
    Table *table = instance(Table);
    table->oid = oid;
    table->meta_table = meta_table;
    table->root_page_num = ROOT_PAGE_NUM;
    table->creator = getpid();
    table->page_size = 1;
    SaveTableCache(table);

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
    if (is_null(pos_def))
        return meta_table->column_size;

    int i;
    for (i = 0; i < meta_table->column_size; i++) {
        MetaColumn *current = meta_table->meta_column[i];
        if (streq(current->column_name, pos_def->column)) {
            switch (pos_def->type) {
                case POS_BEFORE:
                    return i;
                case POS_AFTER:
                    return i + 1;
            }
        }
    }

    db_log(ERROR, "Column '%s' not exists in table '%s'.", 
           pos_def->column, 
           meta_table->table_name);

    return -1;
}

/* Add new MetaColumn to table.
 * This function is actually bottom-level routine for alter-table-add-column action.
 * */
bool add_new_meta_column(char *table_name, MetaColumn *new_meta_column, ColumnPositionDef *post_def) {
    Table *table = open_table(table_name);
    MetaTable *meta_table = table->meta_table;
    int pos = get_column_position(meta_table, post_def);
    append_new_column(table->root_page_num, table, new_meta_column, pos);
    return true;
}

/* Drop table`s meta_column. */
bool drop_meta_column(char *table_name, char *column_name) {
    Table *table = open_table(table_name);
    int pos = get_meta_column_pos_by_name(table->meta_table, column_name);
    Assert(pos >= 0);
    drop_column(table->root_page_num, table, pos);
    return true;
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

    db_log(DEBUGER, "Will load object %ld from disk.", oid);

    /* Memory missing, get from disk. */
    if (!check_table_exist_direct(oid)) {
        try_release_table(oid);
        return NULL;
    }
    
    /* New table. */
    Table *table = instance(Table);

    /* Define root page is first page. */
    table->oid = oid;
    table->root_page_num = ROOT_PAGE_NUM; 
    table->creator = getpid();
    table->meta_table = gen_meta_table(oid);
    table->page_size = GetPageSize(oid);

    /* Save table cache. */
    SaveTableCache(table);
    
    /* Release table lock. */
    try_release_table(oid);

    db_log(DEBUGER, "Has loaded table %ld from disk.", oid);

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

    /* Check if exist the table. */
    Oid oid = TableNameFindOid(table_name);
    char *file_path = table_file_path(oid);
    if (!table_file_exist(file_path)) {
        dfree(file_path);
        return false;
    }

    /* Try to acquire the table lock. */
    try_acquire_table(oid);

    /* Get file descriptor. */
    FDesc fdesc = get_file_desc(oid);

    /* Close file descriptor. */
    close(fdesc);

    /* Unregister fdesc. */
    unregister_fdesc(oid);

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
