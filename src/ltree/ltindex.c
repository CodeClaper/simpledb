#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "ltindex.h"
#include "const.h"
#include "mmgr.h"
#include "pager.h"
#include "ltbase.h"
#include "ltinsert.h"
#include "meta.h"
#include "compare.h"
#include "common.h"
#include "log.h"
#include "bufmgr.h"
#include "table.h"
#include "tuple.h"
#include "strheaptable.h"

/* Check if key already exists  */
 bool IndexDuplicateKeyCheck(void *key, Refer *refer) {
    Table *table;
    Buffer buffer;
    void *node, *target;
    uint32_t key_len, value_len, default_value_len;
    MetaColumn *primary_key_meta_column;
    bool ret = false;

    /* Get the buffer. */
    table = open_table_inner(refer->oid);
    buffer = ReadBuffer(refer->oid, refer->page_num); 
    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);

    value_len = table->index_value_len;
    default_value_len = table->heap_value_len;
    key_len = table->key_len;

    /* If the cell is overflow the page, it not duplcate of course. */
    if (!BtreeInsertForLeafNodeSafe(node, key_len, value_len, default_value_len, refer->cell_num))
        goto direct_exit;

    primary_key_meta_column = MetaTableFindPrimaryKey(table->meta_table);
    target = LeafNodeGetCellKey(node, key_len, value_len, default_value_len, refer->cell_num);
    Assert(target < (void *) ((char *) node + PAGE_SIZE));

    /* Get result. */
    ret = (target < node + PAGE_SIZE) && 
            EQ(GetComparableValue(target, primary_key_meta_column->column_type), 
               GetComparableValue(key, primary_key_meta_column->column_type), 
               primary_key_meta_column->column_type);

direct_exit:
    /* Release the buffer. */
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    return ret;
}

/* Get index created xid. */
Xid IndexGetCreatedXid(void *index) {
    return *(Xid *) (index + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Set index created xid. */
void IndexSetCreatedXid(void *index, Xid created_xid) {
    *(Xid *) (index + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE) = created_xid;
}

/* Get index expired xid. */
Xid IndexGetExpiredXid(void *index) {
    return *(Xid *) (index + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Set index expired xid. */
void IndexSetExpiredXid(void *index, Xid expired_xid) {
    *(Xid *) (index + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE) = expired_xid;
}

/* Get index sys id. */
int64_t IndexGetSysId(void *index) {
    return *(int64_t *) (index + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Get index sys id. */
void IndexSetSysId(void *index, int64_t sys_id) {
    *(int64_t *) (index + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE) = sys_id;
}

/* Get index refer. */
Refer *IndexGetRefer(void *index) {
    return (Refer *) index;
}

/* Set index refer. */
void IndexSetRefer(void *index, Refer *refer) {
    memcpy(index, refer, REFER_SIZE);
}

/* Generate index. */
void *GenerateIndex(Oid oid, void *tuple, Refer *hrefer) {
    Table *table;
    void *destination;
    uint32_t offset = REFER_SIZE;

    table = open_table_inner(oid);
    destination = dalloc(table->index_value_len);
    
    /* Set refer value. */
    IndexSetRefer(destination, hrefer);

    /* Set system reserved value. */
    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        if (meta_column->sys_reserved) {
            void *value = TupleFindValue(tuple, meta_column);
            MetaColumnAssignValueToDestination(destination + offset, value, meta_column);
            offset += meta_column->column_length;
        }
    }

    return destination;
}

