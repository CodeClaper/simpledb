#include <stdint.h>
#include <string.h>
#include "row.h"
#include "tuple.h"
#include "const.h"
#include "meta.h"
#include "mmgr.h"
#include "instance.h"
#include "log.h"

/* New a row. */
Row *NewRow() {
    Row *row = instance(Row);
    row->data = create_list(NODE_KEY_VALUE);
    return row;
}

/* Generate row by tuple. */
Row *GenerateRowInner(void *tuple, List *meta_columns) {
    Row *row = NewRow();

    /* Assignment row data. */
    ListCell *lc;
    foreach (lc, meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        /* Generate a key value pair. */
        KeyValue *key_value = new_key_value(meta_column->column_name,
                                            TupleFindValue(tuple, meta_column),
                                            meta_column->column_type,
                                            meta_column->tid);
        key_value->is_array = meta_column->array_dim > 0;

        /* Append to row data. */
        append_list(row->data, key_value);
    }

    return row;
}

/* Generate row by tuple. */
Row *GenerateRow(void *tuple, MetaTable *meta_table) {
    return GenerateRowInner(tuple, meta_table->meta_columns);
}

/* Seriable row to tuple. */
void *RowSeriableTuple(Row *row, Table *table) {
    uint32_t offset = 0;
    void *tuple;

    tuple = dalloc(table->heap_value_len);

    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column;
        void *value;

        meta_column = (MetaColumn *)lfirst(lc);
        value = RowGetValueOrDefault(row, meta_column);

        if (meta_column->not_null && IsNull(value)) {
            db_log(ERROR, "Column '%s' does`t have a default value.", 
                   meta_column->column_name);
            return NULL;
        }

        /* Assign row value to destination. */
        MetaColumnAssignValueToDestination(tuple + offset, value, meta_column);
        offset += meta_column->column_length;
    }

    return tuple;
}

/* Find the key in a row. 
 * ---------------------
 * Return NULL if not found.
 * */
void *RowFindKey(Row *row, Table *table) {
    Assert(row != NULL);
    Assert(table != NULL);
        
    void *key;
    MetaIndex *pri_meta_index;
    uint32_t offset = 0;

    pri_meta_index = TableFindPrimaryMetaIndex(table);
    key = dalloc(pri_meta_index->key_len);


    ListCell *lc;
    foreach (lc, pri_meta_index->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        void *value = RowGetValueOrDefault(row, meta_column);
        memcpy(key + offset, value, meta_column->column_length);
        offset += meta_column->column_length;
    }

    return key;
}

/* Get row value or default. 
 * -------------------------
 * Note: Firstly, find values in user-passed row,
 * If missing, return the default values.
 * If not define default values, return null.
 * */
void *RowGetValueOrDefault(Row *row, MetaColumn *meta_column) {
    char *column_name = meta_column->column_name;

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (StrEq(column_name, key_value->key))
           return key_value->value;
    }

    switch (meta_column->default_value_type) {
        case DEFAULT_VALUE_NULL:
        case DEFAULT_VALUE_NONE:
            return NULL;
        case DEFAULT_VALUE:
            return meta_column->default_value;
        default:
            UNEXPECTED_VALUE(meta_column->default_value_type);
    }
}


