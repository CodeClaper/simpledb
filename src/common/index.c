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
 bool check_duplicate_key(void *key, Refer *refer) {
    Buffer buffer;
    void *node, *target;
    uint32_t key_len, value_len, default_value_len;
    MetaColumn *primary_key_meta_column;
    Table *table = open_table_inner(refer->oid);

    /* Get the buffer. */
    buffer = ReadBuffer(refer->oid, refer->page_num); 
    node = GetBufferPage(buffer);

    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
    key_len = table->key_len;

    /* If overflow after the new tuple inserting, it not duplcate of course. */
    if (overflow_leaf_node(node, key_len, value_len, default_value_len, refer->cell_num))
        return false;

    primary_key_meta_column = MetaTableFindPrimaryKey(table->meta_table);
    target = get_leaf_node_cell_key(node, refer->cell_num, key_len, value_len, default_value_len);
    Assert(target < (void *) ((char *) node + PAGE_SIZE));

    /* Release the buffer. */
    ReleaseBuffer(buffer);

    /* Get result. */
    return (target < node + PAGE_SIZE) && 
            EQ(GetComparableValue(target, primary_key_meta_column->column_type), 
               GetComparableValue(key, primary_key_meta_column->column_type), 
               primary_key_meta_column->column_type
    );
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
