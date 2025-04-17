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
#include "mmgr.h"
#include "free.h"
#include "cache.h"
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
#include "sys.h"

/* Get table list. */
List *get_table_list() {
    List *list;
    DIR *dir;
    struct dirent *entry;

    list = create_list(NODE_STRING);
    if ((dir = opendir(conf->data_dir)) == NULL) {
        db_log(PANIC, "System error, not found directory: %s", conf->data_dir); 
    } else {
        while((entry = readdir(dir)) != NULL) {
            if (entry->d_type == 8 && endwith(entry->d_name, ".dbt")) {
                char *table_name = replace_once(entry->d_name, ".dbt", "");
                /* Skip the system reserved table. */
                if (!if_table_reserved(table_name))
                    append_list(list, table_name);
            }
        }
        closedir(dir);
    }
    return list;
}

/* Get table file path. */
char *table_file_path(char *table_name) {
    if (table_name == NULL) {
        fprintf(stderr, "Inner error, table name can`t be NULL.\n");
        exit(EXIT_FAILURE);
    }
    int len = strlen(conf->data_dir) + strlen(table_name) + strlen(".dbt") + 1;
    char *file_path = dalloc(len);
    sprintf(file_path, "%s%s%s", conf->data_dir, table_name, ".dbt");
    return file_path;
}

/* Check table file if exist 
 * Return true if exist or false if not exist. */
bool table_file_exist(char *table_file_path) {
    struct stat buffer;
    return (stat(table_file_path, &buffer) == 0);
}

/* Check if table exists. */
bool check_table_exist(char *table_name) {
    char *file_path = table_file_path(table_name);
    bool ret = table_file_exist(file_path);
    dfree(file_path);
    return ret;
}

/* Create a new table. */
bool create_table(MetaTable *meta_table) {

    Assert(meta_table);

    char *file_path = table_file_path(meta_table->table_name);
    if (table_file_exist(file_path)) {
        db_log(ERROR, "Table '%s' already exists. \n", meta_table->table_name);
        dfree(file_path);
        return false;
    }

    int descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(ERROR, "Open database file '%s' fail.\n", file_path);
        dfree(file_path);
        return false;
    }

    void *root_node = dalloc(PAGE_SIZE);

    uint32_t default_value_len = calc_table_row_length2(meta_table);

    /* Initialize root node */
    initial_leaf_node(root_node, default_value_len, true);

    /* Set meta column */
    set_column_size(root_node, meta_table->all_column_size);
    
    /* Get default value cell. */
    void *default_value_dest = get_default_value_cell(root_node);

    /* Serialize */
    uint32_t offset = 0;
    uint32_t i;
    for (i = 0; i < meta_table->all_column_size; i++) {
        MetaColumn *meta_column = (MetaColumn *)(meta_table->meta_column[i]);
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

/* Open a table file. 
 * Return Table or NULL if not exists. */
Table *open_table(char *table_name) {
    Assert(table_name);

    /* Check table if locked. Block here until acquire the table if locked. */
    check_table_locked(table_name);

    /* Firstly, try to find in buffer. */
    Table *mtable = find_table_cache(table_name);
    if (mtable != NULL)
        return mtable;

    try_acquire_table(table_name);
    
    /* Double check. */
    mtable = find_table_cache(table_name);
    if (mtable != NULL) {
        try_release_table(table_name);
        return mtable;
    }

    db_log(DEBUGER, "Will load table %s from disk.", table_name);

    /* Memory missing, get from disk. */
    char *file_path = table_file_path(table_name);
    if (!table_file_exist(file_path)) {
        dfree(file_path);
        try_release_table(table_name);
        return NULL;
    }
    
    /* New table. */
    Table *table = instance(Table);

    /* Define root page is first page. */
    table->root_page_num = 0; 
    table->creator = getpid();
    table->meta_table = gen_meta_table(table, table_name);
    table->page_size = GetPageSize(table_name);

    /* Save table cache. */
    save_table_cache(table);
    
    /* Free memory. */
    dfree(file_path);

    try_release_table(table_name);

    db_log(DEBUGER, "Has loaded table %s from disk.", table_name);

    /* Only return buffer table to keep same table pointer in the same transaction. */
    return find_table_cache(table_name);
}


/* Drop an existed table. */
bool drop_table(char *table_name) {
    /* Check if exist the table. */
    char *file_path = table_file_path(table_name);
    if (!table_file_exist(file_path)) {
        dfree(file_path);
        return false;
    }

    /* Try to acquire the table lock. */
    try_acquire_table(table_name);

    /* Get file descriptor. */
    FDesc fdesc = get_file_desc(table_name);

    /* Close file descriptor. */
    close(fdesc);

    /* Unregister fdesc. */
    unregister_fdesc(table_name);

    /* Disk remove. */
    if (remove(file_path) == 0) {
        /* Remove table cache. */
        remove_table_cache(table_name);
        /* Remove table buffer. */
        RemoveTableBuffer(table_name);
        /* Release the table lock. */
        try_release_table(table_name);
        return true;
    }

    /* Release the table lock. */
    try_release_table(table_name);
    
    /* Not reach here logically. */
    db_log(ERROR, "Table '%s' deleted fail, error : %d", table_name, errno);

    return false;
}
