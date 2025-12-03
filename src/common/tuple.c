#include <stdint.h>
#include <string.h>
#include "data.h"
#include "meta.h"
#include "const.h"
#include "copy.h"
#include "instance.h"
#include "mmgr.h"
#include "refer.h"

/* Get array number. */
static uint32_t GetArrayNumber(void *destination) {
    return *(uint32_t *)(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Get array value. */
static void *GetArrayValue(void *destination, uint32_t i, uint32_t span) {
    return (destination + LEAF_NODE_CELL_NULL_FLAG_SIZE + LEAF_NODE_ARRAY_NUM_SIZE + span * i);
}

/* Get tuple array value. 
 * Return ArrayValue. */
static ArrayValue *GetTupleArrayValue(void *destination, MetaColumn *meta_column) {
    uint32_t array_num, span, i;
    ArrayValue *array_value;

    array_num = GetArrayNumber(destination);
    span = (meta_column->column_length - LEAF_NODE_ARRAY_NUM_SIZE - LEAF_NODE_CELL_NULL_FLAG_SIZE) / meta_column->array_cap;
    array_value = new_array_value(meta_column->column_type, array_num);

    for (i = 0; i < array_num; i++) {
        void *value = GetArrayValue(destination, i, span);
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
void *TupleFindKey(void *tuple, Table *table) {
    void *key;
    MetaIndex *pri_meta_index;
    uint32_t offset = 0;

    pri_meta_index = TableFindPrimaryMetaIndex(table);
    key = dalloc(pri_meta_index->key_len);

    ListCell *lc;
    foreach (lc, pri_meta_index->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        void *value = TupleFindValue(tuple, meta_column);
        memcpy(key + offset, value, meta_column->column_length);
        offset += meta_column->column_length;
    }

    return key;
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

/* Get ref id in tuple. */
Rid TupleGetRefId(void *tuple, MetaTable *meta_table) {
    MetaColumn *ref_id_meta_column = NameFindAllMetaColumn(meta_table, SYS_REF_ID_COLUMN_NAME);
    Assert(ref_id_meta_column != NULL);
    return *(Xid *)TupleFindValue(tuple, ref_id_meta_column);
}

/* Set ref id in tuple. */
void TupleSetRefId(void *tuple, MetaTable *meta_table, int64_t sys_id) {
    MetaColumn *ref_id_meta_column = NameFindAllMetaColumn(meta_table, SYS_RESERVED_ID_COLUMN_NAME);
    Assert(ref_id_meta_column != NULL);
    TupleSetValue(tuple, ref_id_meta_column, &sys_id);
}

