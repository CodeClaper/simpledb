#include <stdbool.h>
#include <stdint.h>
#include "bininsert.h"
#include "bin.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "log.h"

static void BinInsertInner(MetaIndex *meta_index, void *key, void *boundary_key, Refer *value, uint32_t page_num);

/* Bin insert for internal node. 
 * ----------------------------
 * Will use binnary seach to find the next level child cell.
 * */
static void BinInsertForInternalNodeExtend(MetaIndex *meta_index, void *key, Refer *value, void *internal_node) {
    void *boundary_key;
    uint32_t keys_num, min_index, max_index, target_page;
    
    keys_num = BinInternalNodeGetKeysNum(internal_node);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        void *cell_key;

        index = (max_index + min_index) / 2;
        cell_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, index);
        /* Notice: Greate EQ opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (BinCompareKey(meta_index, cell_key, key) >= 0) 
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
        boundary_key = BinInternalNodeGetRightKey(internal_node);
        target_page = BinInternalNodeGetRightNum(internal_node);
        BinInsertInner(meta_index, key, boundary_key, value, target_page);
    } else {
        /* The target cell in cells. */
        boundary_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, min_index);
        target_page = BinInternalNodeGetCellValue(internal_node, meta_index->key_len, min_index);
        BinInsertInner(meta_index, key, boundary_key, value, target_page);
    }
}


/* Bin insert for internal node. */
static void BinInsertForInternalNode(MetaIndex *meta_index, void *key, void *boundary_key, Refer *value, uint32_t page_num) {
    Oid oid;
    Buffer buffer;
    void *internal_node, *high_key;

    oid = meta_index->oid;
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPageCopy(buffer);
    high_key = BinNodeGetHighKey(internal_node, meta_index->key_len, meta_index->value_len);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (BinCompareKey(meta_index, boundary_key, high_key) > 0 &&
        BinCompareKey(meta_index, key, high_key) > 0
    ) {
        uint32_t next_sibling = BinInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        BinInsertForInternalNode(meta_index, key, boundary_key, value, next_sibling);
    } else
        BinInsertForInternalNodeExtend(meta_index, key, value, internal_node);

    dfree(internal_node);
}

/* Check if bin leaf node is safe when inserting new cell. */
static bool BinInsertForLeafNodeSafe(MetaIndex *meta_index, void *leaf_node) {
    uint32_t cell_len, cell_num;
    
    cell_len = meta_index->key_len + meta_index->value_len;
    cell_num = BinLeafNodeGetCellNum(leaf_node);

    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        return COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
               BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + CELL_NUM_SIZE + LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * (cell_num + 1) <= PAGE_SIZE;
    } else 
        return LEAF_NODE_HEAD_SIZE + cell_len * (cell_num + 1) <= PAGE_SIZE;
}

/* Bin insert into a cell and not split leaf node. */
static void BinInsertForLeafNodeNoSplit(MetaIndex *meta_index, void *key, void *value, Buffer buffer) {

}

/* Bin insert into a cell and split leaf node. */
static void BinInsertForLeafNodeSplit(MetaIndex *meta_index, void *key, void *value, Buffer buffer) {

}

/* Bin insert into a cell. */
static void BinInsertForLeafNodeInsertCell(MetaIndex *meta_index, void *key, void *value, Buffer buffer) {
    void *leaf_node = GetBufferPage(buffer);
    if (BinInsertForLeafNodeSafe(meta_index, leaf_node))
        BinInsertForLeafNodeNoSplit(meta_index, key, value, buffer);
    else
        BinInsertForLeafNodeSplit(meta_index, key, value, buffer);
}

/* Bin insert for leaf node. */
static void BinInsertForLeafNode(MetaIndex *meta_index, void *key, void *boundary_key, Refer *value, uint32_t page_num) {
    Oid oid;
    Buffer buffer;
    void *leaf_node, *high_key;

    oid = meta_index->oid;
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    leaf_node = GetBufferPage(buffer);
    high_key = BinNodeGetHighKey(leaf_node, meta_index->key_len, meta_index->value_len);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (BinCompareKey(meta_index, boundary_key, high_key) > 0 &&
        BinCompareKey(meta_index, key, high_key) > 0
    ) {
        uint32_t next_sibling = BinLeafNodeGetSibling(leaf_node);
        Assert(next_sibling != 0);
        BinInsertForLeafNode(meta_index, key, boundary_key, value, next_sibling);

        UnlockBuffer(buffer);
        ReleaseBuffer(buffer);
    } else 
        BinInsertForLeafNodeInsertCell(meta_index, key, value, buffer);    
}

/* Bin insert. 
 * ------------
 * This function just defines to go to leaf node or internal node.
 * */
static void BinInsertInner(MetaIndex *meta_index, void *key, void *boundary_key, Refer *value, uint32_t page_num) {
    Oid oid;
    Buffer buffer;
    void *node;
    NodeType type;

    oid = meta_index->oid;
    buffer = ReadBuffer(oid, page_num);

    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    type = GetNodeType(node);
    switch (type) {
        case LEAF_NODE:
            BinInsertForLeafNode(meta_index, key, boundary_key, value, page_num);
            break;
        case INTERNAL_NODE:
            BinInsertForInternalNode(meta_index, key, boundary_key, value, page_num);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}

/* Btree index insert. */
bool BtreeIndexInsert(MetaIndex *meta_index, void *key, Refer *value) {
    Assert(key != NULL);
    Assert(value != NULL);
    BinInsertInner(meta_index, key, NULL, value, ROOT_PAGE_NUM);
    return true;
}
