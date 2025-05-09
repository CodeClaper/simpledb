#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "index.h"
#include "mmgr.h"
#include "pager.h"
#include "ltree.h"
#include "meta.h"
#include "compare.h"
#include "common.h"
#include "log.h"
#include "bufmgr.h"
#include "table.h"
#include "strheaptable.h"

/* Check if key already exists  */
 bool check_duplicate_key(Cursor *cursor, void *key) {
    Buffer buffer;
    void *node, *target;
    uint32_t key_len, value_len;
    MetaColumn *primary_key_meta_column;
    Table *table = cursor->table;

    /* Get the buffer. */
    buffer = ReadBuffer(GET_TABLE_OID(table), cursor->page_num); 
    node = GetBufferPage(buffer);

    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);

    primary_key_meta_column = get_primary_key_meta_column(table->meta_table);
    target = get_leaf_node_cell_key(node, cursor->cell_num, key_len, value_len);

    /* Release the buffer. */
    ReleaseBuffer(buffer);

    /* Get result. */
    return (target < node + PAGE_SIZE) && equal(target, key, primary_key_meta_column->column_type);
}


/* Get key string value. */
char *get_key_str(void *key, DataType data_type) {
    switch(data_type) {
        case T_BOOL: 
            return *(bool *)key ? "true" : "false";
        case T_CHAR:
        case T_VARCHAR:
        case T_STRING: 
            return (char *)key;
        case T_INT: {
            char *str = dalloc(50);
            sprintf(str, "%d", *(int32_t *)key);
            return str;
        }
        case T_LONG: {
            char *str = dalloc(100);
            sprintf(str, "%ld", *(int64_t *)key);
            return str;
        }
        case T_DOUBLE: {
            char *str = dalloc(50);
            sprintf(str, "%lf", *(double *)key);
            return str;
        }
        case T_FLOAT: {
            char *str = dalloc(50);
            sprintf(str, "%f", *(float *)key);
            return str;
        }
        case T_DATE: {
            char *str = dalloc(30);
            struct tm *tmp_time = localtime(key);
            strftime(str, strlen(str), "%Y-%m-%d", tmp_time);
            return str;
        }
        case T_TIMESTAMP: {
            char *str = dalloc(40);
            struct tm *tmp_time = localtime(key);
            strftime(str, strlen(str), "%Y-%m-%d %H:%M:%S", tmp_time);
            return str;
        } default: {
            db_log(ERROR, "Not allowed data type as primary key.");
            return NULL;
        }
    }
}

/* Get key type name */
char *key_type_name(MetaColumn *meta_column) {
    if (meta_column->is_primary) 
        return "primary";
    else if (meta_column->is_unique)
        return "unique";
    else
        return NULL;
}
