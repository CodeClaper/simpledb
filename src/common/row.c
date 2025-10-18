#include "row.h"
#include "tuple.h"
#include "meta.h"
#include "mmgr.h"
#include "instance.h"

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
                                            TupeFindValue(tuple, meta_column), 
                                            meta_column->column_type, 
                                            meta_column->own_table_name);
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

/* Find the key in a row. 
 * ---------------------
 * Return NULL if not found.
 * */
void *RowFindKey(Row *row, MetaTable *meta_table) {
    Assert(row != NULL);
    Assert(meta_table != NULL);
    
    ListCell *lc;
    MetaColumn *primary_meta_column;

    primary_meta_column = MetaTableFindPrimaryKey(meta_table);
    foreach (lc, row->data) {
        KeyValue *key_value = (KeyValue *) lfirst(lc);
        AssertFalse(StrIsEmpty(key_value->table_name));
        if (StrEq(key_value->key, primary_meta_column->column_name) && StrEq(key_value->table_name, meta_table->table_name))
            return key_value->value;
    }

    return NULL;
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

