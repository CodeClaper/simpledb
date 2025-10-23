#include <string.h>
#include "ltmodify.h"
#include "const.h"
#include "ltbase.h"
#include "bufmgr.h"
#include "refer.h"
#include "table.h"
#include "meta.h"
#include "compare.h"
#include "mmgr.h"
#include "log.h"

static void BtreeModifyInner(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer);
static void BtreeModifyExpiredXidInner(Oid oid, void *key, void *boundary_key, Xid expired_xid, uint32_t page_num, Refer *refer);

/* Modify for the leaf node cell. */
static void BtreeModifyForLeafNodeCell(Oid oid, void *key, void *value, Buffer buffer, Refer *refer) {
    Table *table;
    void *leaf_node;
    uint32_t target_index;

    table = open_table_inner(oid);
    leaf_node = GetBufferPage(buffer);
    target_index = LeafNodeFindCellNum(oid, key, leaf_node);

    refer->cell_num = target_index;
    LeafNodeSetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index, value);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Modify for the leaf node. */
static void BtreeModifyForLeafNode(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *leaf_node, *high_key;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    leaf_node = GetBufferPage(buffer);
    high_key = NodeGetHighKey(table, leaf_node);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (GT(GetComparableValue(boundary_key, ptype), GetComparableValue(high_key, ptype), ptype) &&
        GT(GetComparableValue(key, ptype), GetComparableValue(high_key, ptype), ptype)
    ) {
        uint32_t next_sibling = NodeGetNextSibling(table, leaf_node);
        Assert(next_sibling != 0);
        BtreeModifyForLeafNode(oid, key, boundary_key, value, next_sibling, refer);

        UnlockBuffer(buffer);
        ReleaseBuffer(buffer);
    } else {
        refer->page_num = page_num;
        BtreeModifyForLeafNodeCell(oid, key, value, buffer, refer);    
    }
}


/* Modify for internal node. 
 * ---------------------------------------------
 * In this function, we will use binary search to find the target cell.
 * */
static void BtreeModifyForInternalNodeExtend(Oid oid, void *key, void *value, void *internal_node, Refer *refer) {
    Table *table;
    DataType ptype;
    uint32_t keys_num, min_index, max_index, target_page;
    void *boundary_key;
    
    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    keys_num = InternalNodeGetKeysNum(internal_node, table->heap_value_len);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        void *cell_key;

        index = (max_index + min_index) / 2;
        cell_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, index);
        /* Notice: Greate EQ opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (GE(GetComparableValue(cell_key, ptype), GetComparableValue(key, ptype), ptype)) 
            max_index = index;
        else 
            min_index = index + 1;
    }

    if (min_index > keys_num)
        db_log(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, 
               keys_num);
    else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = InternalNodeGetRightKey(internal_node, table->heap_value_len);
        target_page = InternalNodeGetRightNum(internal_node, table->heap_value_len);
        BtreeModifyInner(oid, key, boundary_key, value, target_page, refer);
    } else {
        /* The target cell in cells. */
        boundary_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, min_index);
        target_page = InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, min_index);
        BtreeModifyInner(oid, key, boundary_key, value, target_page, refer);
    }
}

/* Modify for the internal node. 
 * ---------------------------------------------
 * Modify for internal node, we just lock the node using reader lock.
 * Because, we do not change it.
 * */
static void BtreeModifyForInternalNode(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *internal_node, *high_key;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPageCopy(buffer);
    high_key = NodeGetHighKey(table, internal_node);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (GT(GetComparableValue(boundary_key, ptype), GetComparableValue(high_key, ptype), ptype) &&
        GT(GetComparableValue(key, ptype), GetComparableValue(high_key, ptype), ptype)
    ) {
        uint32_t next_sibling = NodeGetNextSibling(table, internal_node);
        Assert(next_sibling != 0);
        BtreeModifyForInternalNode(oid, key, boundary_key, value, next_sibling, refer);
    } else
        BtreeModifyForInternalNodeExtend(oid, key, value, internal_node, refer);    

    dfree(internal_node);
}

/* Btree Modify. 
 * --------------
 * This function just defines to go to leaf node or internal node.
 * */
static void BtreeModifyInner(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer) {
    Buffer buffer;
    void *node;
    NodeType type;

    buffer = ReadBuffer(oid, page_num);

    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    type = GetNodeType(node);
    switch (type) {
        case LEAF_NODE:
            BtreeModifyForLeafNode(oid, key, boundary_key, value, page_num, refer);
            break;
        case INTERNAL_NODE:
            BtreeModifyForInternalNode(oid, key, boundary_key, value, page_num, refer);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}

/* Modify the btree. */
Refer *BtreeModify(Oid oid, void *key, void *value) {
    Assert(key != NULL);
    Assert(value != NULL);
    Refer *refer = new_refer(oid, -1, -1);
    BtreeModifyInner(oid, key, NULL, value, ROOT_PAGE_NUM, refer);
    return refer;
}

/* Modify for the leaf node cell. */
static void BtreeModifyExpiredXidForLeafNodeCell(Oid oid, void *key, Xid expired_xid, Buffer buffer, Refer *refer) {
    Table *table;
    void *leaf_node, *value;
    uint32_t target_index;

    table = open_table_inner(oid);
    leaf_node = GetBufferPage(buffer);
    target_index = LeafNodeFindCellNum(oid, key, leaf_node);

    refer->cell_num = target_index;
    value = LeafNodeGetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);
    *(Xid *) (value + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE) = expired_xid;

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}


/* Modify for the leaf node. */
static void BtreeModifyExpiredXidForLeafNode(Oid oid, void *key, void *boundary_key, Xid expired_xid, uint32_t page_num, Refer *refer) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *leaf_node, *high_key;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    leaf_node = GetBufferPage(buffer);
    high_key = NodeGetHighKey(table, leaf_node);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (GT(GetComparableValue(boundary_key, ptype), GetComparableValue(high_key, ptype), ptype) &&
        GT(GetComparableValue(key, ptype), GetComparableValue(high_key, ptype), ptype)
    ) {
        uint32_t next_sibling = NodeGetNextSibling(table, leaf_node);
        Assert(next_sibling != 0);
        BtreeModifyExpiredXidForLeafNode(oid, key, boundary_key, expired_xid, next_sibling, refer);

        UnlockBuffer(buffer);
        ReleaseBuffer(buffer);
    } else {
        refer->page_num = page_num;
        BtreeModifyExpiredXidForLeafNodeCell(oid, key, expired_xid, buffer, refer);    
    }
}

/* Modify for internal node expired xid. 
 * ---------------------------------------------
 * In this function, we will use binary search to find the target cell.
 * */
static void BtreeModifyExpiredXidForInternalNodeExtend(Oid oid, void *key, Xid expired_xid, Xid *internal_node, Refer *refer) {
    Table *table;
    DataType ptype;
    uint32_t keys_num, min_index, max_index, target_page;
    void *boundary_key;
    
    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    keys_num = InternalNodeGetKeysNum(internal_node, table->heap_value_len);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        void *cell_key;

        index = (max_index + min_index) / 2;
        cell_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, index);
        /* Notice: Greate EQ opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (GE(GetComparableValue(cell_key, ptype), GetComparableValue(key, ptype), ptype)) 
            max_index = index;
        else 
            min_index = index + 1;
    }

    if (min_index > keys_num)
        db_log(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, 
               keys_num);
    else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = InternalNodeGetRightKey(internal_node, table->heap_value_len);
        target_page = InternalNodeGetRightNum(internal_node, table->heap_value_len);
        BtreeModifyExpiredXidInner(oid, key, boundary_key, expired_xid, target_page, refer);
    } else {
        /* The target cell in cells. */
        boundary_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, min_index);
        target_page = InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, min_index);
        BtreeModifyExpiredXidInner(oid, key, boundary_key, expired_xid, target_page, refer);
    }
}

/* Modify for the internal node expired_xid. 
 * ---------------------------------------------
 * Modify for internal node, we just lock the node using reader lock.
 * Because, we do not change it.
 * */
static void BtreeModifyExpiredXidForInternalNode(Oid oid, void *key, void *boundary_key, Xid expired_xid, uint32_t page_num, Refer *refer) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *internal_node, *high_key;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPageCopy(buffer);
    high_key = NodeGetHighKey(table, internal_node);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (GT(GetComparableValue(boundary_key, ptype), GetComparableValue(high_key, ptype), ptype) &&
        GT(GetComparableValue(key, ptype), GetComparableValue(high_key, ptype), ptype)
    ) {
        uint32_t next_sibling = NodeGetNextSibling(table, internal_node);
        Assert(next_sibling != 0);
        BtreeModifyExpiredXidForInternalNode(oid, key, boundary_key, expired_xid, next_sibling, refer);
    } else
        BtreeModifyExpiredXidForInternalNodeExtend(oid, key, expired_xid, internal_node, refer);    

    dfree(internal_node);
}



/* Btree Modify expired xid. 
 * -------------------------
 * This function just defines to go to leaf node or internal node.
 * */
static void BtreeModifyExpiredXidInner(Oid oid, void *key, void *boundary_key, Xid expired_xid, uint32_t page_num, Refer *refer) {
    Buffer buffer;
    void *node;
    NodeType type;

    buffer = ReadBuffer(oid, page_num);

    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    type = GetNodeType(node);
    switch (type) {
        case LEAF_NODE:
            BtreeModifyExpiredXidForLeafNode(oid, key, boundary_key, expired_xid, page_num, refer);
            break;
        case INTERNAL_NODE:
            BtreeModifyExpiredXidForInternalNode(oid, key, boundary_key, expired_xid, page_num, refer);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}



/* Modify the btree expired_xid. 
 * ----------------------------
 * This function usually use for delete index.
 * */
Refer *BtreeModifyExpiredXid(Oid oid, void *key, Xid expired_xid) {
    Assert(key != NULL);
    Refer *refer = new_refer(oid, -1, -1);
    BtreeModifyExpiredXidInner(oid, key, NULL, expired_xid, ROOT_PAGE_NUM, refer);
    return refer;
}
