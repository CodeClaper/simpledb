#include <stdint.h>
#include <string.h>
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

/* Set value in tuple.*/
void TupleSetValue(void *tuple, MetaColumn *meta_column, void *value) {
    void *destination = tuple + meta_column->offset;
    MetaColumnAssignValueToDestination(destination, value, meta_column);
}

/* Get primary key value in tuple. */
void *TupleFindKey(void *tuple, MetaTable *meta_table) {
    MetaColumn *primary_meta_column = MetaTableFindPrimaryKey(meta_table);
    Assert(primary_meta_column != NULL);
    return TupleFindValue(tuple, primary_meta_column);
}

/* Get created xid in tuple. */
Xid TupleFindCreatedXid(void *tuple, MetaTable *meta_table) {
    MetaColumn *created_xid_meta_column = NameFindAllMetaColumn(meta_table, CREATED_XID_COLUMN_NAME);
    Assert(created_xid_meta_column != NULL);
    return *(Xid *)TupleFindValue(tuple, created_xid_meta_column);
}

/* Set created xid in tuple. */
void TupleSetCreatedXid(void *tuple, MetaTable *meta_table, Xid created_xid) {
    MetaColumn *created_xid_meta_column = NameFindAllMetaColumn(meta_table, CREATED_XID_COLUMN_NAME);
    Assert(created_xid_meta_column != NULL);
    TupleSetValue(tuple, created_xid_meta_column, &created_xid);
}

/* Get created xid in tuple. */
Xid TupleFindExpiredXid(void *tuple, MetaTable *meta_table) {
    MetaColumn *expired_xid_meta_column = NameFindAllMetaColumn(meta_table, EXPIRED_XID_COLUMN_NAME);
    Assert(expired_xid_meta_column != NULL);
    return *(Xid *)TupleFindValue(tuple, expired_xid_meta_column);
}

/* Set expired xid in tuple. */
void TupleSetExpiredXid(void *tuple, MetaTable *meta_table, Xid expired_xid) {
    MetaColumn *expired_xid_meta_column = NameFindAllMetaColumn(meta_table, EXPIRED_XID_COLUMN_NAME);
    Assert(expired_xid_meta_column != NULL);
    TupleSetValue(tuple, expired_xid_meta_column, &expired_xid);
}

/* Get sys id in tuple. */
int64_t TupleGetSysId(void *tuple, MetaTable *meta_table) {
    MetaColumn *sys_id_meta_column = NameFindAllMetaColumn(meta_table, SYS_RESERVED_ID_COLUMN_NAME);
    Assert(sys_id_meta_column != NULL);
    return *(Xid *)TupleFindValue(tuple, sys_id_meta_column);
}

/* Set sys id in tuple. */
void TupleSetSysId(void *tuple, MetaTable *meta_table, int64_t sys_id) {
    MetaColumn *sys_id_meta_column = NameFindAllMetaColumn(meta_table, SYS_RESERVED_ID_COLUMN_NAME);
    Assert(sys_id_meta_column != NULL);
    TupleSetValue(tuple, sys_id_meta_column, &sys_id);
}

