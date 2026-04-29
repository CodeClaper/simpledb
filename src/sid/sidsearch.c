#include <stdint.h>
#include "sidsearch.h"
#include "sidbase.h"
#include "ltbase.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "log.h"

/* Sid search inner.  */
static Refer *SidSearchInner(Oid soid, Sid key, Sid boundary_key, uint32_t page_num);

/* Sid search for internal node extend. */
static Refer *SidSearchForInternalNodeExtend(Oid soid, Sid key, void *internal_node) {
    uint32_t keys_num, min_index, max_index, target_page;
    Sid boundary_key;
    
    keys_num = SidInternalNodeGetKeysNum(internal_node);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        Sid cell_key;

        index = (max_index + min_index) / 2;
        cell_key = SidInternalNodeGetCellKey(internal_node, index);
        /* Notice: Greate EQ opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (cell_key >= key) 
            max_index = index;
        else 
            min_index = index + 1;
    }

    if (min_index > keys_num)
        logger(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, keys_num);
    else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = SidInternalNodeGetRightKey(internal_node);
        target_page = SidInternalNodeGetRightNum(internal_node);
        return SidSearchInner(soid, key, boundary_key, target_page);
    } else {
        /* The target cell in cells. */
        boundary_key = SidInternalNodeGetCellKey(internal_node, min_index);
        target_page = SidInternalNodeGetCellValue(internal_node, min_index);
        return SidSearchInner(soid, key, boundary_key, target_page);
    }

    return NULL;
}

/* Sid search for internal node. */
static Refer *SidSearchForInternalNode(Oid soid, Sid key, Sid boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *internal_node;
    Sid high_key;
    Refer *refer;

    buffer = ReadBuffer(soid, page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPageCopy(buffer);
    high_key = SidNodeGetHighKey(internal_node);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (boundary_key > high_key && key > high_key) {
        uint32_t next_sibling = SidInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        refer = SidSearchForInternalNode(soid, key, boundary_key, next_sibling);
    } else
        refer = SidSearchForInternalNodeExtend(soid, key, internal_node);    

    dfree(internal_node);

    return refer;
}

/* Sid search for leaf node. */
static Refer *SidSearchForLeafNode(Oid soid, Sid key, Sid boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *leaf_node;
    Sid high_key;
    Refer *refer;

    buffer = ReadBuffer(soid, page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);
    high_key = SidNodeGetHighKey(leaf_node);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (boundary_key > high_key && key > high_key) {
        uint32_t next_sibling = SidLeafNodeGetNextSibling(leaf_node);
        Assert(next_sibling != 0);
        refer = SidSearchForLeafNode(soid, key, boundary_key, next_sibling);
    } else {
        uint32_t target_index = SidLeafNodeFindCellNum(leaf_node, key);
        refer = SidLeafNodeGetCellValue(leaf_node, target_index);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return refer;
}


/* Sid search inner.  */
static Refer *SidSearchInner(Oid soid, Sid key, Sid boundary_key, uint32_t page_num) {
    Buffer buffer;
    void *node;
    NodeType type;

    buffer = ReadBuffer(soid, page_num);

    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    type = GetNodeType(node);
    switch (type) {
        case LEAF_NODE:
            return SidSearchForLeafNode(soid, key, boundary_key, page_num);
        case INTERNAL_NODE:
            return SidSearchForInternalNode(soid, key, boundary_key, page_num);
        default:
            UNEXPECTED_VALUE(type);
            return NULL;
    }
}

/* Sid search refer value. */
Refer *SidSearch(Oid soid, Sid key) {
    return SidSearchInner(soid, key, SID_ZERO, ROOT_PAGE_NUM);
}
