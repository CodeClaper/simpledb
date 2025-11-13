#include <stdint.h>
#include "binsearch.h"
#include "bin.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "log.h"

static Refer *BinSearchInner(MetaIndex *meta_index, void *key, void *boundary_key, uint32_t page_num);

/* Bin search for internal node. 
 * We will use binary search for targer cell.
 * */
static Refer *BinSearchForInternalNodeExtend(MetaIndex *meta_index, void *key, void *internal_node) {
    uint32_t keys_num, min_index, max_index, target_page_num;
    void *boundary_key;

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

    if (min_index > keys_num) {
        db_log(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, keys_num);
        return NULL;
    } else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = BinInternalNodeGetRightKey(internal_node);
        target_page_num = BinInternalNodeGetRightNum(internal_node);
        return BinSearchInner(meta_index, key, boundary_key, target_page_num);
    } else {
        /* The target cell in cells. */
        boundary_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, min_index);
        target_page_num = BinInternalNodeGetCellValue(internal_node, meta_index->key_len, min_index);
        return BinSearchInner(meta_index, key, boundary_key, target_page_num);
    }
}

/* Bin search for internal node. */
static Refer *BinSearchForInternalNode(MetaIndex *meta_index, void *key, void *boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *internal_node, *high_key;
    Refer *refer;

    buffer = ReadBuffer(meta_index->oid, page_num);
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
        refer = BinSearchForInternalNode(meta_index, key, boundary_key, next_sibling);
    } else
        refer = BinSearchForInternalNodeExtend(meta_index, key, internal_node);    

    dfree(internal_node);

    return refer;
}

/* Btree search leaf cell value. */
static Refer *BinSearchForLeafNodeExtend(MetaIndex *meta_index, void *key, void *leaf_node) {
    uint32_t target_index = BinLeafNodeFindCellNum(meta_index, leaf_node, key);
    return (Refer *) BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, target_index);
}

static Refer *BinSearchForLeafNode(MetaIndex *meta_index, void *key, void *boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *leaf_node, *high_key;
    Refer *refer;

    buffer = ReadBuffer(meta_index->oid, page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);
    high_key = BinNodeGetHighKey(leaf_node, meta_index->key_len, meta_index->value_len);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (BinCompareKey(meta_index, boundary_key, high_key) > 0 &&
        BinCompareKey(meta_index, key, high_key) > 0
    ) {
        uint32_t next_sibling = BinLeafNodeGetNextSibling(leaf_node);
        Assert(next_sibling != 0);
        refer = BinSearchForLeafNode(meta_index, key, boundary_key, next_sibling);
    } else 
         refer = BinSearchForLeafNodeExtend(meta_index, key, leaf_node);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return refer;
}

/* Bin search inner. */
static Refer *BinSearchInner(MetaIndex *meta_index, void *key, void *boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *node;
    NodeType type;

    buffer = ReadBuffer(meta_index->oid, page_num);

    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    type = GetNodeType(node);
    switch (type) {
        case LEAF_NODE:
            return BinSearchForLeafNode(meta_index, key, boundary_key, page_num);
            break;
        case INTERNAL_NODE:
            return BinSearchForInternalNode(meta_index, key, boundary_key, page_num);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }

    return NULL;
}

/* Bin search refer via key. */
Refer *BinSearch(MetaIndex *meta_index, void *key) {
    return BinSearchInner(meta_index, key, NULL, ROOT_PAGE_NUM);
}
