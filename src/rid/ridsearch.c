#include <stdint.h>
#include "ridsearch.h"
#include "ridbase.h"
#include "ltbase.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "log.h"

/* Rid search inner.  */
static Refer *RidSearchInner(Oid roid, Rid key, Rid boundary_key, uint32_t page_num);

/* Rid search for internal node extend. */
static Refer *RidSearchForInternalNodeExtend(Oid roid, Rid key, void *internal_node) {
    uint32_t keys_num, min_index, max_index, target_page;
    Rid boundary_key;
    
    keys_num = RidInternalNodeGetKeysNum(internal_node);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        Rid cell_key;

        index = (max_index + min_index) / 2;
        cell_key = RidInternalNodeGetCellKey(internal_node, index);
        /* Notice: Greate EQ opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (cell_key >= key) 
            max_index = index;
        else 
            min_index = index + 1;
    }

    if (min_index > keys_num)
        db_log(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, keys_num);
    else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = RidInternalNodeGetRightKey(internal_node);
        target_page = RidInternalNodeGetRightNum(internal_node);
        return RidSearchInner(roid, key, boundary_key, target_page);
    } else {
        /* The target cell in cells. */
        boundary_key = RidInternalNodeGetCellKey(internal_node, min_index);
        target_page = RidInternalNodeGetCellValue(internal_node, min_index);
        return RidSearchInner(roid, key, boundary_key, target_page);
    }

    return NULL;
}

/* Rid search for internal node. */
static Refer *RidSearchForInternalNode(Oid roid, Rid key, Rid boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *internal_node;
    Rid high_key;
    Refer *refer;

    buffer = ReadBuffer(roid, page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPageCopy(buffer);
    high_key = RidNodeGetHighKey(internal_node);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (boundary_key > high_key && key > high_key) {
        uint32_t next_sibling = RidInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        refer = RidSearchForInternalNode(roid, key, boundary_key, next_sibling);
    } else
        refer = RidSearchForInternalNodeExtend(roid, key, internal_node);    

    dfree(internal_node);

    return refer;
}

/* Rid search for leaf node. */
static Refer *RidSearchForLeafNode(Oid roid, Rid key, Rid boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *leaf_node;
    Rid high_key;
    Refer *refer;

    buffer = ReadBuffer(roid, page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);
    high_key = RidNodeGetHighKey(leaf_node);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (boundary_key > high_key && key > high_key) {
        uint32_t next_sibling = RidLeafNodeGetNextSibling(leaf_node);
        Assert(next_sibling != 0);
        refer = RidSearchForLeafNode(roid, key, boundary_key, next_sibling);
    } else {
        uint32_t target_index = RidLeafNodeFindCellNum(leaf_node, key);
        refer = RidLeafNodeGetCellValue(leaf_node, target_index);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return refer;
}


/* Rid search inner.  */
static Refer *RidSearchInner(Oid roid, Rid key, Rid boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *node;
    NodeType type;

    buffer = ReadBuffer(roid, page_num);

    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    type = GetNodeType(node);
    switch (type) {
        case LEAF_NODE:
            return RidSearchForLeafNode(roid, key, boundary_key, page_num);
        case INTERNAL_NODE:
            return RidSearchForInternalNode(roid, key, boundary_key, page_num);
        default:
            UNEXPECTED_VALUE(type);
            return NULL;
    }
}

/* Rid search refer value. */
Refer *RidSearch(Oid roid, Rid key) {
    return RidSearchInner(roid, key, RID_ZERO, ROOT_PAGE_NUM);
}
