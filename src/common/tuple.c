#include "data.h"
#include "meta.h"
#include "const.h"
#include "ltree.h"
#include "copy.h"
#include "instance.h"

/* Get tuple array value. 
 * Return ArrayValue. */
static ArrayValue *GetTupleArrayValue(void *destination, MetaColumn *meta_column) {
    uint32_t array_num = get_array_number(destination);

    /* Generate ArrayValue instance. */
    ArrayValue *array_value = new_array_value(meta_column->column_type, array_num);
    uint32_t span = (meta_column->column_length - LEAF_NODE_ARRAY_NUM_SIZE - LEAF_NODE_CELL_NULL_FLAG_SIZE) / meta_column->array_cap;

    uint32_t i;
    for (i = 0; i < array_num; i++) {
        void *value = get_array_value(destination, i, span);
        append_list(array_value->list, copy_value(value, meta_column->column_type));
    }

    return array_value;
}

/* Get tuple value. */ 
static void *GetTupleValue(void *destination, MetaColumn *meta_column) {
    return (meta_column->array_dim == 0)
            /* For non-array data. */
            ? destination + LEAF_NODE_CELL_NULL_FLAG_SIZE 
            /* For array data. */
            : GetTupleArrayValue(destination, meta_column); 
}

/* Get value in tuple. */
void *TupleFindValue(void *tuple, MetaColumn *meta_column) {
    bool nflag =  *(bool *)(tuple + meta_column->offset);
    return nflag ? NULL : GetTupleValue((tuple + meta_column->offset), meta_column);
}

/* Get primary key value in tuple. */
void *TupleFindKey(void *tuple, MetaTable *meta_table) {
    MetaColumn *primary_meta_column = MetaTableFindPrimaryKey(meta_table);
    return TupleFindValue(tuple, primary_meta_column);
}
