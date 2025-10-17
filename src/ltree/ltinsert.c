#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "ltinsert.h"
#include "bufpool.h"
#include "ltr.h"
#include "table.h"
#include "const.h"
#include "meta.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "compare.h"
#include "pager.h"
#include "refer.h"
#include "heaptable.h"
#include "copy.h"
#include "log.h"
#include "trans.h"

#define OK   1
#define WAIT 0
#define ERRO -1

static void BtreeInsertInner(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer);
static void BtreeInsertForInternalNodeInsertCell(Oid oid, uint32_t page_num, void *old_child_key, void *old_new_key, void *new_child_key, uint32_t new_child_page);


/* Predicate duplicate key.
 * These are three duplicate key type.
 * (1) 1: A deleted duplicate key which does not need to care about.
 * (2) 0: An un-commited duplicate key which need to wait for commit.
 * (3) -1: A commited duplicate key which case duplicate key issue. 
 * */
static int BtreeInsertDuplicateKeyPredicate(Xid current_xid, Xid created_xid, Xid expired_xid) {
    Assert(created_xid != 0);
    if (expired_xid == 0) {
        if (created_xid == current_xid)
            return ERRO;
        else if (IsActive(created_xid))
            return WAIT;
        else
            return ERRO;
    } else {
        if (expired_xid == current_xid)
            return OK;
        else if (IsActive(expired_xid))
            return WAIT;
        else 
            return OK;
    }
}

static void BtreeInsertWaitForRetry(Oid oid, void *key, void *value, Xid created_xid, Xid expired_xid) {
    Assert(created_xid != 0);
    /* Wait for transaction commit. */
    if (expired_xid == 0) {
        while (IsActive(created_xid)) {
            lock_sleep(DEFAULT_SPIN_INTERVAL);
        }
    } else {
        while (IsActive(expired_xid)) {
            lock_sleep(DEFAULT_SPIN_INTERVAL);
        }
    }
    /* Retry to insert. */
    BtreeInsert(oid, key, value);
}

/* Update internal cell key. */
static void BtreeInsertForInternalNodeUpdateCellKey(Oid oid, uint32_t page_num, void *old_key, void *new_key) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *internal_node, *high_key;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    internal_node = GetBufferPage(buffer);
    high_key = NodeGetHighKey(table, internal_node);

    /* These are three cases:
     * (1) Old key is less than high key, which means it is in the cells of the internal node. 
     * (2) Old key is equals to high key, which means it is the right child.
     * (3) Old key is more than high key, which means the old internal node has spliten, and need to move to next sibling to search. */
    if (LT(GetComparableValue(old_key, ptype), GetComparableValue(high_key, ptype), ptype)) {
        uint32_t index;
        void *cell_key;

        index = InternalNodeFindCellNum(oid, old_key, internal_node);
        cell_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, index);
        /* Theoretically EQ, just for check. Lots of tricky bugs are caught by the check. */
        Assert(EQ(GetComparableValue(old_key, ptype), GetComparableValue(cell_key, ptype), ptype));
        InternalNodeSetCellKey(internal_node, table->key_len, table->heap_value_len, index, new_key);
    } else if (EQ(GetComparableValue(old_key, ptype), GetComparableValue(high_key, ptype), ptype)) {
        InternalNodeSetRightKey(internal_node, table->key_len, table->heap_value_len, new_key);
    } else {
        uint32_t next_sibling = NodeGetNextSibling(table, internal_node); 
        Assert(next_sibling != 0);
        BtreeInsertForInternalNodeUpdateCellKey(oid, next_sibling, old_key, new_key);
        goto DirectExit; 
    }

    MakeBufferDirty(buffer);
    
    /* Update current internal node parent. */
    if (!NodeIsRoot(internal_node) &&
        EQ(GetComparableValue(new_key, ptype), GetComparableValue(high_key, ptype), ptype)
    ) {
        uint32_t parent_num;
        parent_num = NodeGetParentNum(internal_node);
        BtreeInsertForInternalNodeUpdateCellKey(oid, parent_num, old_key, new_key);
    }

DirectExit:
    /* Unlock Buffer. */
    UnlockBuffer(buffer);
    /* Relase Buffer. */
    ReleaseBuffer(buffer);
} 

/* Update internal node chidren parent. 
 * ------------------------------------
 * Indeed, update the child node parent, should lock it and update it.
 * But, we do not use any lock, and think it` ok. The reason as follow:
 * There are only two operations to use parent num, insert new item to parent 
 * and update parent cell key. Search operation does not use parent num. 
 * In both the two operations, We use sibling link to make sure find the right target parent.
 * */
static void BtreeInsertForInternalNodeUpdateChildrenParent(Oid oid, void *internal_node, uint32_t parent_num) {
    Table *table;
    uint32_t keys_num, i;

    table = open_table_inner(oid);
    keys_num = InternalNodeGetKeysNum(internal_node, table->heap_value_len);

    for (i = 0; i < keys_num; i++) {
        uint32_t child_page;
        Buffer child_buffer;
        void *child_node;

        child_page = InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, i);
        child_buffer = ReadBuffer(oid, child_page);
        child_node = GetBufferPage(child_buffer);
        NodeSetParentNum(child_node, parent_num);
        
        MakeBufferDirty(child_buffer);
        ReleaseBuffer(child_buffer);
    }
}

/* Upgrade root internal node. */
static void BtreeInsertForInternalNodeUpgradeRoot(Oid oid, void *root, void *right_node, uint32_t right_page) {
    Table *table;
    Buffer new_buffer;
    void *new_internal_node;
    uint32_t next_page_num, keys_num, i;
    
    table = open_table_inner(oid);
    keys_num = InternalNodeGetKeysNum(root, table->heap_value_len);

    next_page_num = GetNextUnusedPageNum(table);
    new_buffer = ReadBuffer(oid, next_page_num);
    new_internal_node = GetBufferPage(new_buffer);

    /* Initialize new internal node. */
    InternalNodeInitialize(new_internal_node, table->heap_value_len, false);
    /* Set keys num, */
    InternalNodeSetKeysNum(new_internal_node, table->heap_value_len, keys_num);
    /* Set sibling. */
    NodeSetNextSibling(table, new_internal_node, right_page);
    /* Set right child*/
    InternalNodeSetRightKey(new_internal_node, table->key_len, table->heap_value_len, 
                            InternalNodeGetRightKey(root, table->heap_value_len));
    InternalNodeSetRightNum(new_internal_node, table->heap_value_len,
                            InternalNodeGetRightNum(root, table->heap_value_len));

    /* Set cells. */
    for (i = 0; i < keys_num; i++) {
        InternalNodeSetCellKey(new_internal_node, table->key_len, table->heap_value_len, i,
                               InternalNodeGetCellKey(root, table->key_len, table->heap_value_len, i));
        InternalNodeSetCellValue(new_internal_node, table->key_len, table->heap_value_len, i, 
                                 InternalNodeGetCellValue(root, table->key_len, table->heap_value_len, i));
    }

    /* Update chidren parent of the new internal node. */
    BtreeInsertForInternalNodeUpdateChildrenParent(oid, new_internal_node, next_page_num);

    /* Set keys num. */
    InternalNodeSetKeysNum(root, table->heap_value_len, 1);

    /* Register leaf node to root. */
    InternalNodeSetCellKey(root, table->key_len, table->heap_value_len, 0, NodeGetHighKey(table, new_internal_node));
    InternalNodeSetCellValue(root, table->key_len, table->heap_value_len, 0, next_page_num);
    InternalNodeSetRightKey(root, table->key_len, table->heap_value_len, NodeGetHighKey(table, right_node));
    InternalNodeSetRightNum(root, table->heap_value_len, right_page);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}

/* Check if internal node is safe when inserting new item. */
static bool BtreeInsertForInternalNodeSafe(void *internal_node, uint32_t keys_num, uint32_t key_len, uint32_t default_value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return COMMON_NODE_HEADER_SIZE + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size + 
               default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * (keys_num + 1) <= PAGE_SIZE;
    } else {
        uint32_t internal_head_len = COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len;
        return internal_head_len + cell_len * (keys_num + 1) <= PAGE_SIZE;
    }
}

/* Insert item into the internal node and split. */
static void BtreeInsertForInternalNodeSplit(Oid oid, void *internal_node, 
                                            void *old_child_key, void *old_new_key, 
                                            void *new_child_key, uint32_t new_child_page) {
    Table *table;
    DataType ptype;
    void *high_key;
    Buffer new_buffer;
    void *new_internal_node;
    uint32_t keys_num, next_page_num, target_index, right_page, LEFT_SPLIT_COUNT, RIGHT_SPLIT_COUNT;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    high_key = copy_value(NodeGetHighKey(table, internal_node), ptype);
    right_page = InternalNodeGetRightNum(internal_node, table->heap_value_len);
    keys_num = InternalNodeGetKeysNum(internal_node, table->heap_value_len);

    next_page_num = GetNextUnusedPageNum(table);
    new_buffer = ReadBuffer(oid, next_page_num);
    new_internal_node = GetBufferPage(new_buffer);

    /* Initialize new internal node. */
    InternalNodeInitialize(new_internal_node, table->heap_value_len, false);
    /* Switch sibling. */
    NodeSetNextSibling(table, new_internal_node, NodeGetNextSibling(table, internal_node));
    NodeSetNextSibling(table, internal_node, next_page_num);

    /* Set parent. */
    NodeSetParentNum(new_internal_node, NodeGetParentNum(internal_node));

    /* We need to deal with two case:
     * (1) The new child key is greater than or equal to high key, which means should be the right child of the internal node. 
     * (2) Otherwise, the new child should be in cells of the internal node. */
    if (GE(GetComparableValue(new_child_key, ptype), 
           GetComparableValue(high_key, ptype), 
           ptype)
    ) {
        Assert(EQ(GetComparableValue(old_new_key, ptype), GetComparableValue(high_key, ptype), ptype));
        InternalNodeSetRightKey(internal_node, table->key_len, table->heap_value_len, new_child_key);
        InternalNodeSetRightNum(internal_node, table->heap_value_len, new_child_page);

        /* If current internal node is not root, 
         * should update it` parent cell key. */
        if (!NodeIsRoot(internal_node)) {
            uint32_t parent_num = NodeGetParentNum(internal_node);
            BtreeInsertForInternalNodeUpdateCellKey(oid, parent_num, high_key, new_child_key);
        }
    
        /* Use the old right child as the new child. */
        new_child_page = right_page;
        new_child_key = old_new_key;
        /* Get target index. */
        target_index = keys_num;
    } else {
        uint32_t old_target_index;

        old_target_index = InternalNodeFindCellNum(oid, old_child_key, internal_node);
        Assert(EQ(GetComparableValue(old_child_key, ptype), 
                  GetComparableValue(InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, old_target_index), ptype), 
                  ptype));
        InternalNodeSetCellKey(internal_node, table->key_len, table->heap_value_len, old_target_index, old_new_key);
        /* Get target index. */
        target_index = InternalNodeFindCellNum(oid, new_child_key, internal_node);
    }

    RIGHT_SPLIT_COUNT = (keys_num + 1) / 2;
    LEFT_SPLIT_COUNT = (keys_num + 1) - RIGHT_SPLIT_COUNT;

    int i;
    for (i = keys_num; i >= 0; i--) {
        uint32_t new_index;
        void *destination_node;

        /* New position. */
        new_index = i % LEFT_SPLIT_COUNT;
        /* Define which node. */ 
        destination_node = (i >= LEFT_SPLIT_COUNT)
                    ? new_internal_node 
                    : internal_node;

        if (i == target_index) {
            InternalNodeSetCellKey(destination_node, table->key_len, table->heap_value_len, new_index, new_child_key); 
            InternalNodeSetCellValue(destination_node, table->key_len, table->heap_value_len, new_index, new_child_page);
        } else if (i > target_index) {
            /* Right cells make cell space. */
            InternalNodeSetCellKey(destination_node, table->key_len, table->heap_value_len, new_index,
                                   InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, i - 1));
            InternalNodeSetCellValue(destination_node, table->key_len, table->heap_value_len, new_index, 
                                     InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, i - 1));
        } else {
            InternalNodeSetCellKey(destination_node, table->key_len, table->heap_value_len, new_index, 
                                   InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, i));
            InternalNodeSetCellValue(destination_node, table->key_len, table->heap_value_len, new_index, 
                                     InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, i));
        }
    }

    /* Set new internal node keys num. */
    InternalNodeSetKeysNum(new_internal_node, table->heap_value_len, RIGHT_SPLIT_COUNT);
    /* Set new internal right child. */
    InternalNodeSetRightKey(new_internal_node, table->key_len, table->heap_value_len,
                            InternalNodeGetRightKey(internal_node, table->heap_value_len));
    InternalNodeSetRightNum(new_internal_node, table->heap_value_len, 
                            InternalNodeGetRightNum(internal_node, table->heap_value_len));
    /* Update chidren parent of the new internal node. */
    BtreeInsertForInternalNodeUpdateChildrenParent(oid, new_internal_node, next_page_num);


    /* Set old internal node keys num. */
    InternalNodeSetKeysNum(internal_node, table->heap_value_len, LEFT_SPLIT_COUNT - 1);
    /* Set old internal node right child. */
    InternalNodeSetRightKey(internal_node, table->key_len, table->heap_value_len, 
                            InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, LEFT_SPLIT_COUNT - 1));
    InternalNodeSetRightNum(internal_node, table->heap_value_len, 
                            InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, LEFT_SPLIT_COUNT -1));


    /* If old internal is root, need to upgrade. 
     * Otherwise, it`s a normal internal node. 
     * Maybe the max key change, need update max key in parent internal node. 
     * Note that: because of new_max_key more likely less than the old_max_key,
     * so mass of parent internal node cells may be happer.
     * We'll resort parent internal node cells lately. 
     * */
    if (NodeIsRoot(internal_node)) 
        BtreeInsertForInternalNodeUpgradeRoot(oid, internal_node, new_internal_node, next_page_num);
    else {
        uint32_t parent_num;
        void *child_key, *old_new_key;

        parent_num = NodeGetParentNum(internal_node);
        old_new_key = NodeGetHighKey(table, internal_node);
        child_key = NodeGetHighKey(table, new_internal_node);

        /* Insert new internal node to parent. */
        BtreeInsertForInternalNodeInsertCell(oid, parent_num, high_key, old_new_key, child_key, next_page_num);
    }

    dfree(high_key);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}

/* Insert item into the internal node no split. */
static void BtreeInsertForInternalNodeNoSplit(Oid oid, void *internal_node, 
                                              void *old_child_key, void *old_new_key, 
                                              void *new_child_key, uint32_t new_child_page) {
    Table *table;
    DataType ptype;
    void *high_key;
    uint32_t keys_num;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    high_key = NodeGetHighKey(table, internal_node);
    keys_num = InternalNodeGetKeysNum(internal_node, table->heap_value_len);

    /* We need to deal with two case:
     * (1) The new child key is greater than or equal to high key, which means should be the right child of the internal node. 
     * (2) Otherwise, the new child should be in cells of the internal node. */
    if (GE(GetComparableValue(new_child_key, ptype), 
           GetComparableValue(high_key, ptype), 
           ptype)
    ) {
        Assert(EQ(GetComparableValue(old_child_key, ptype), GetComparableValue(high_key, ptype), ptype));
        InternalNodeSetCellKey(internal_node, table->key_len, table->heap_value_len, keys_num, old_new_key);
        InternalNodeSetCellValue(internal_node, table->key_len, table->heap_value_len, keys_num, 
                                 InternalNodeGetRightNum(internal_node, table->heap_value_len));
        InternalNodeSetRightKey(internal_node, table->key_len, table->heap_value_len, new_child_key);
        InternalNodeSetRightNum(internal_node, table->heap_value_len, new_child_page); 

        /* If current internal node is not root, 
         * should update it`s parent cell key. */
        if (!NodeIsRoot(internal_node)) {
            uint32_t parent_num = NodeGetParentNum(internal_node);
            BtreeInsertForInternalNodeUpdateCellKey(oid, parent_num, old_child_key, new_child_key);
        }
    } else {
        uint32_t old_target_index, new_target_index, i;
        void *temp;
        
        /* Change the old key. */
        old_target_index = InternalNodeFindCellNum(oid, old_child_key, internal_node);
        temp = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, old_target_index);
        if (NE(GetComparableValue(old_child_key, ptype), GetComparableValue(temp, ptype), ptype))
            Assert(EQ(GetComparableValue(old_child_key, ptype), 
                      GetComparableValue(temp, ptype), 
                      ptype));
        InternalNodeSetCellKey(internal_node, table->key_len, table->heap_value_len, old_target_index, old_new_key);

        /* Append new child. */
        new_target_index = InternalNodeFindCellNum(oid, new_child_key, internal_node);
        for (i = keys_num; i > new_target_index; i--) {
            InternalNodeSetCellKey(internal_node, table->key_len, table->heap_value_len,i, 
                                   InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, i - 1));
            InternalNodeSetCellValue(internal_node, table->key_len, table->heap_value_len, i, 
                                     InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, i - 1));
        }

        /* Set new child cell. */
        InternalNodeSetCellKey(internal_node, table->key_len, table->heap_value_len, new_target_index, new_child_key);
        InternalNodeSetCellValue(internal_node, table->key_len, table->heap_value_len, new_target_index, new_child_page);
    }

    /* Increase keys num. */
    InternalNodeIncreaseKeysNum(internal_node, table->heap_value_len);
}

/* Insert item into the internal node. */
static void BtreeInsertForInternalNodeInsertCell(Oid oid, uint32_t page_num, 
                                                 void *old_child_key, void *old_new_key, 
                                                 void *new_child_key, uint32_t new_child_page) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *internal_node, *high_key;
    uint32_t keys_num;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);

    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    internal_node = GetBufferPage(buffer);
    high_key = NodeGetHighKey(table, internal_node);
    keys_num = InternalNodeGetKeysNum(internal_node, table->heap_value_len);
    
    /* Only one condition to move to sibling:
     * The old child key is more than high key. */
    if (GT(GetComparableValue(old_child_key, ptype), GetComparableValue(high_key, ptype), ptype)) {
        uint32_t next_sibling = NodeGetNextSibling(table, internal_node);
        Assert(next_sibling != 0);
        BtreeInsertForInternalNodeInsertCell(oid, next_sibling, old_child_key, old_new_key, new_child_key, new_child_page);
    } else {
        /* If current is safe, just insert new cell, not split.
         * Otherwise, split first and then insert new cell. */
        if (BtreeInsertForInternalNodeSafe(internal_node, keys_num, table->key_len, table->heap_value_len))
            BtreeInsertForInternalNodeNoSplit(oid, internal_node, old_child_key, old_new_key, new_child_key, new_child_page);
        else
            BtreeInsertForInternalNodeSplit(oid, internal_node, old_child_key, old_new_key, new_child_key, new_child_page);
            
        /* Make buffer dirty. */
        MakeBufferDirty(buffer);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert item into the btree for internal node. 
 * ---------------------------------------------
 * In this function, we will use binary search to find the target cell.
 * */
static void BtreeInsertForInternalNodeExtend(Oid oid, void *key, void *value, void *internal_node, Refer *refer) {
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
        BtreeInsertInner(oid, key, boundary_key, value, target_page, refer);
    } else {
        /* The target cell in cells. */
        boundary_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, min_index);
        target_page = InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, min_index);
        BtreeInsertInner(oid, key, boundary_key, value, target_page, refer);
    }
}

/* Insert item into the btree for internal node. 
 * ---------------------------------------------
 * Insert for internal node, we just lock the node using reader lock.
 * Because, we do not change it.
 * */
static void BtreeInsertForInternalNode(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer) {
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
        BtreeInsertForInternalNode(oid, key, boundary_key, value, next_sibling, refer);
    } else
        BtreeInsertForInternalNodeExtend(oid, key, value, internal_node, refer);    

    dfree(internal_node);
}


/* Root Leaf node upgrade to Root Internal node. */
static void BtreeInsertForLeafNodeUpgradeRoot(Oid oid, void *root, void *right_node, uint32_t right_page, Refer *refer) {
    Table *table;
    uint32_t cell_num, column_size, next_page_num, i;
    Buffer new_buffer;
    void *new_leaf_node;

    table = open_table_inner(oid);
    cell_num = LeafNodeGetCellNum(root, table->heap_value_len);
    column_size = RootNodeGetColumnSize(root);

    next_page_num = GetNextUnusedPageNum(table);
    new_buffer = ReadBuffer(oid, next_page_num);
    new_leaf_node = GetBufferPage(new_buffer);
    
    /* Initialize leaf node. */
    LeafNodeInitialize(new_leaf_node, table->heap_value_len, false);
    /* Set cell num. */
    LeafNodeSetCellNum(new_leaf_node, table->heap_value_len, cell_num);
    /* Set sibling. */
    NodeSetNextSibling(table, new_leaf_node, right_page);

    /* Copy each cell. */
    for (i = 0; i < cell_num; i++) {
        LeafNodeSetCellKey(new_leaf_node, table->key_len, table->index_value_len, table->heap_value_len, i, 
                           LeafNodeGetCellKey(root, table->key_len, table->index_value_len, table->heap_value_len, i));
        LeafNodeSetCellValue(new_leaf_node, table->key_len, table->index_value_len, table->heap_value_len, i, 
                             LeafNodeGetCellValue(root, table->key_len, table->index_value_len, table->heap_value_len, i));
        /* Update refer. */
        update_refer(oid, ROOT_PAGE_NUM, i, next_page_num, i);
    }

    /* Set parent. */
    NodeSetParentNum(new_leaf_node, ROOT_PAGE_NUM);
    NodeSetParentNum(right_node, ROOT_PAGE_NUM);

    /* Make clear outsides header. */
    uint32_t ROOT_LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + ROOT_NODE_META_COLUMN_SIZE_SIZE 
                                        + ROOT_NODE_META_COLUMN_SIZE * column_size;
    memset(root + ROOT_LEAF_NODE_HEADER_SIZE, 0, PAGE_SIZE - ROOT_LEAF_NODE_HEADER_SIZE);
    
    /* upgrade to internal node. */
    SetNodeType(root, INTERNAL_NODE);

    /* Set keys num. */
    InternalNodeSetKeysNum(root, table->heap_value_len, 1);

    /* Register leaf node to root. */
    InternalNodeSetCellKey(root, table->key_len, table->heap_value_len, 0, NodeGetHighKey(table, new_leaf_node));
    InternalNodeSetCellValue(root, table->key_len, table->heap_value_len, 0, next_page_num);
    InternalNodeSetRightKey(root, table->key_len, table->heap_value_len, NodeGetHighKey(table, right_node));
    InternalNodeSetRightNum(root, table->heap_value_len, right_page);

    /* If refer page num is root page, it need update.*/
    if (refer->page_num == ROOT_PAGE_NUM) 
        refer->page_num = next_page_num;

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}

/* Check if leaf node is safe when inserting new item. */
static bool BtreeInsertForLeafNodeSafe(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t cell_num) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return COMMON_NODE_HEADER_SIZE + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size + 
               default_value_len + CELL_NUM_SIZE + LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * (cell_num + 1) <= PAGE_SIZE;
    } else {
        return LEAF_NODE_HEAD_SIZE + cell_len *(cell_num + 1) <= PAGE_SIZE;
    }
}

/* Insert item into the btree and split. */
static void BtreeInsertForLeafNodeSplit(Oid oid, void *key, void *value, Buffer buffer, Refer *refer) {
    Table *table;
    DataType ptype;
    uint32_t target_index, cell_num, next_page_num, cell_len, RIGHT_SPLIT_COUNT, LEFT_SPLIT_COUNT;
    Buffer new_buffer;
    void *leaf_node, *cell_key, *high_key, *new_leaf_node;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    leaf_node = GetBufferPage(buffer);
    cell_num = LeafNodeGetCellNum(leaf_node, table->heap_value_len);
    cell_len = table->key_len + table->index_value_len;
    high_key = copy_value(NodeGetHighKey(table, leaf_node), ptype);
    target_index = LeafNodeFindCellNum(oid, key, leaf_node);
    cell_key = LeafNodeGetCellKey(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);

    /* Avoid duplicate key. */
    if (EQ(GetComparableValue(key, ptype), GetComparableValue(cell_key, ptype), ptype)) {
        uint32_t predicate;
        Xid current_xid, created_xid, expired_xid;

        current_xid = GetCurrentXid();
        created_xid = LeafNodeGetCellCreatedXid(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);
        expired_xid = LeafNodeGetCellExpiredXid(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);
        predicate = BtreeInsertDuplicateKeyPredicate(current_xid, created_xid, expired_xid);

        switch (predicate) {
            case OK:
                break;
            case WAIT: {
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                return BtreeInsertWaitForRetry(oid, key, value, created_xid, expired_xid);
            }
            case ERRO: {
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                db_log(ERROR, "key '%s' in table '%s' already exists, not allow duplicate key.", 
                       KeyGetSysStrValue(key, ptype), GET_TABLE_NAME(table));
                break;
            }
        }

    }

    next_page_num = GetNextUnusedPageNum(table);
    new_buffer = ReadBuffer(oid, next_page_num);
    new_leaf_node = GetBufferPage(new_buffer);
    
    /* Initialize leaf node. */
    LeafNodeInitialize(new_leaf_node, table->heap_value_len, false);
    /* Switch sibling. */
    NodeSetNextSibling(table, new_leaf_node, NodeGetNextSibling(table, leaf_node));
    NodeSetNextSibling(table, leaf_node, next_page_num);

    /* Set parent. */
    NodeSetParentNum(new_leaf_node, NodeGetParentNum(leaf_node));

    /* All existing keys plus new key should should be divided 
     * evenly between old (left) and new (right) nodes.
     * Starting from the right, move each key to correct position. */
    RIGHT_SPLIT_COUNT = (cell_num + 1) / 2;
    LEFT_SPLIT_COUNT = (cell_num + 1) - RIGHT_SPLIT_COUNT;

    /* Notice, cant make i uint32_t when i decrease, 
     * when i = 0 and decrease, it still satisfy i >= 0. */
    int i; 
    for (i = cell_num; i >= 0; i--) {
        uint32_t destination_page, new_index;
        void *destination_node, *destination;

        /* If index GT than LEAF_SPLIT_COUNT, destination is new old, 
         * othersize, stay in the old node. */
        destination_page = (i >= LEFT_SPLIT_COUNT) 
                            ? next_page_num 
                            : refer->page_num;
        destination_node = (i >= LEFT_SPLIT_COUNT) 
                            ? new_leaf_node 
                            : leaf_node;
        /* New position. */
        new_index = i % LEFT_SPLIT_COUNT;
        destination = LeafNodeGetCellValue(destination_node, table->key_len, table->index_value_len, table->heap_value_len, new_index);

        /* The cursor rigth cells should move one cell to the right to make space for the cursor, 
         * include the cell having the old same num as cursor. The cursor leaf cells don`t need to make space.
         * Because i start with cell number and decrease, right cells firstly move and make space. */
        if (i == target_index) {
            /* Deposit cursor. */
            LeafNodeSetCellKey(destination_node, table->key_len, table->index_value_len, table->heap_value_len, new_index, key);
            LeafNodeSetCellValue(destination_node, table->key_len, table->index_value_len, table->heap_value_len, new_index, value);
            
            /* Redefine the refer info. */
            refer->cell_num = target_index;
            refer->page_num = destination_page;
        } else if (i > refer->cell_num) {
            /* Define new position, and right cells make cell space. */
            memcpy(destination, LeafNodeGetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, i - 1), cell_len);
            /* Update refer. */
            update_refer(oid, refer->page_num, i - 1, destination_page, new_index);
        } else {
            /* Define new position. */
            memcpy(destination, LeafNodeGetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, i), cell_len);
            /* Update refer. */
            update_refer(oid, refer->page_num, i, destination_page, new_index);
        }
    }

    /* Reset cell num. */
    LeafNodeSetCellNum(leaf_node, table->heap_value_len, LEFT_SPLIT_COUNT);
    LeafNodeSetCellNum(new_leaf_node, table->heap_value_len, RIGHT_SPLIT_COUNT);

    /* If current is root, it need to upgrade to internal node. 
     * Otherwise, it is a normal leaf node, maybe the max key change, need update max key in parent internal node. 
     * Note that: because of new_max_key more likely less than the old_max_key,
     * so mass of parent internal node cells may be happen. We'll resort parent internal node cells lately.
     * */
    if (NodeIsRoot(leaf_node))
        BtreeInsertForLeafNodeUpgradeRoot(oid, leaf_node, new_leaf_node, next_page_num, refer);
    else {
        uint32_t parent_num;
        void *new_hight_key, *child_key;

        parent_num = NodeGetParentNum(leaf_node);
        new_hight_key = NodeGetHighKey(table, leaf_node); 
        child_key = NodeGetHighKey(table, new_leaf_node);
        
        /* Insert new leaf into parent. */
        BtreeInsertForInternalNodeInsertCell(oid, parent_num, high_key, new_hight_key, child_key, next_page_num);
    }

    dfree(high_key);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert item into the btree no split. */
static void BtreeInsertForLeafNodeNoSplit(Oid oid, void *key, void *value, Buffer buffer, Refer *refer) {
    Table *table;
    DataType ptype;
    uint32_t cell_len, cell_num, target_index;
    void *leaf_node, *cell_key;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    cell_len = table->key_len + table->index_value_len;
    leaf_node = GetBufferPage(buffer);
    cell_num = LeafNodeGetCellNum(leaf_node, table->heap_value_len);
    target_index = LeafNodeFindCellNum(oid, key, leaf_node);
    refer->cell_num = target_index;
    cell_key = LeafNodeGetCellKey(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);

    /* Avoid duplicate key. */
    if (EQ(GetComparableValue(key, ptype), GetComparableValue(cell_key, ptype), ptype)) {
        uint32_t predicate;
        Xid current_xid, created_xid, expired_xid;
    
        current_xid = GetCurrentXid();
        created_xid = LeafNodeGetCellCreatedXid(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);
        expired_xid = LeafNodeGetCellExpiredXid(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);
        predicate = BtreeInsertDuplicateKeyPredicate(current_xid, created_xid, expired_xid);

        switch (predicate) {
            case OK:
                break;
            case WAIT: {
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                return BtreeInsertWaitForRetry(oid, key, value, created_xid, expired_xid);
            }
            case ERRO: {
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                db_log(ERROR, "key '%s' in table '%s' already exists, not allow duplicate key.", 
                       KeyGetSysStrValue(key, ptype), GET_TABLE_NAME(table));
                break;
            }
        }
    }

    /* If need to move sibling cells. */
    if (target_index < cell_num) {
        /* Make sure move sibling cell from right to left. */
        int i;
        for (i = cell_num; i > target_index; i--) {
            /* Movement. */
            memcpy(
                LeafNodeGetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, i), 
                LeafNodeGetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, i - 1), 
                cell_len
            );
            /* Update refer. */
            update_refer(oid, refer->page_num, i - 1, refer->page_num, i);
        }
    }
    
    /* Set cell key. */
    LeafNodeSetCellKey(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index, key);
    /* Set cell value. */
    LeafNodeSetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index, value);
    /* Increase cell num. */
    LeafNodeIncreaseCellNum(leaf_node, table->heap_value_len);
    
    /* Maybe insertion cause high key change. 
     * If it does, need to update parent key. */
    if (!NodeIsRoot(leaf_node) && target_index == cell_num) {
        uint32_t parent_num;
        void *old_key;

        parent_num = NodeGetParentNum(leaf_node);
        old_key = LeafNodeGetCellKey(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, cell_num - 1);

        BtreeInsertForInternalNodeUpdateCellKey(oid, parent_num, old_key, key);
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert item into the leaf node. */
static void BtreeInsertForLeafNodeInsertCell(Oid oid, void *key, void *value, Buffer buffer, Refer *refer) {
    Table *table;
    void *leaf_node;
    uint32_t cell_num;

    table = open_table_inner(oid);
    leaf_node = GetBufferPage(buffer);
    cell_num = LeafNodeGetCellNum(leaf_node, table->heap_value_len);

    /* If current is safe, just insert new cell, not split.
     * Otherwise, split first and then insert new cell. */
    if (BtreeInsertForLeafNodeSafe(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, cell_num))
        BtreeInsertForLeafNodeNoSplit(oid, key, value, buffer, refer);
    else
        BtreeInsertForLeafNodeSplit(oid, key, value, buffer, refer);
}

/* Insert item into the btree for leaf node. */
static void BtreeInsertForLeafNode(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer) {
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
        BtreeInsertForLeafNode(oid, key, boundary_key, value, next_sibling, refer);
    } else {
        refer->page_num = page_num;
        BtreeInsertForLeafNodeInsertCell(oid, key, value, buffer, refer);    
    }

}

/* Btree insert. 
 * --------------
 * This function just defines to go to leaf node or internal node.
 * */
static void BtreeInsertInner(Oid oid, void *key, void *boundary_key, void *value, uint32_t page_num, Refer *refer) {
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
            BtreeInsertForLeafNode(oid, key, boundary_key, value, page_num, refer);
            break;
        case INTERNAL_NODE:
            BtreeInsertForInternalNode(oid, key, boundary_key, value, page_num, refer);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}

/* Insert item into the btree. */
Refer *BtreeInsert(Oid oid, void *key, void *value) {
    Assert(key != NULL);
    Assert(value != NULL);
    Refer *refer = new_refer(oid, -1, -1);
    BtreeInsertInner(oid, key, NULL, value, ROOT_PAGE_NUM, refer);
    return refer;
}
