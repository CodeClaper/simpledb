#include "row.h"
#include "meta.h"
#include "mmgr.h"
#include "instance.h"

/* New a row. */
Row *NewRow() {
    Row *row = instance(Row);
    row->data = create_list(NODE_KEY_VALUE);
    return row;
}

/* Find the key in a row. 
 * ---------------------
 * Return NULL if not found.
 * */
void *RowFindKey(Row *row, MetaTable *meta_table) {
    Assert(row != NULL);
    Assert(meta_table != NULL);
    
    MetaColumn *primary_meta_column;

    primary_meta_column = get_primary_key_meta_column(meta_table);

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = (KeyValue *) lfirst(lc);
        AssertFalse(is_empty(key_value->table_name));
        if (streq(key_value->key, primary_meta_column->column_name) && streq(key_value->table_name, meta_table->table_name))
            return key_value->value;
    }

    return NULL;
}
