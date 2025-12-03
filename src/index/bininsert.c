#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "bininsert.h"
#include "bin.h"
#include "index.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "log.h"
#include "trans.h"
#include "heaptable.h"
#include "table.h"
#include "tuple.h"
#include "copy.h"

#define OK   1
#define WAIT 0
#define ERRO -1

static void BinInsertInner(MetaIndex *meta_index, void *key, void *boundary_key, Refer *value, uint32_t page_num);
static void BinInsertForInternalNodeInsertCell(MetaIndex *meta_index, uint32_t page_num, void *old_child_key, void *old_new_key, void *new_child_key, uint32_t new_child_page);

/* Predicate duplicate key.
 * --------------------------
 * These are three duplicate key type.
 * (1) OK: A deleted duplicate key which does not need to care about.
 * (2) WAIT: An un-commited duplicate key which need to wait for commit.
 * (3) ERRO: A commited duplicate key which case duplicate key issue. 
 * */
static int BinInsertDuplicateKeyPredicate(Xid current_xid, Xid created_xid, Xid expired_xid) {
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

static void BinInsertWaitForRetry(MetaIndex *meta_index, void *key, void *value, Xid created_xid, Xid expired_xid) {
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
    BtreeIndexInsert(meta_index, key, value);
}

/* Bin insert for internal node to update its cell key. */
static void BinInsertForInternalNodeUpdateCellKey(MetaIndex *meta_index, uint32_t page_num, void *old_key, void *new_key) {
    Oid oid;
    Buffer buffer;
    void *internal_node, *high_key;

    oid = meta_index->oid;
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    internal_node = GetBufferPage(buffer);
    high_key = BinNodeGetHighKey(internal_node, meta_index->key_len, meta_index->value_len);

    
    /* These are three cases:
     * (1) Old key is less than high key, which means it is in the cells of the internal node. 
     * (2) Old key is equals to high key, which means it is the right child.
     * (3) Old key is more than high key, which means the old internal node has spliten, and need to move to next sibling to search. */
    if (CompareKey(meta_index, old_key, high_key) < 0) {
        uint32_t index;
        void *cell_key;

        index = BinInternalNodeFindCellNum(meta_index, internal_node, old_key);
        cell_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, index);
        /* Theoretically EQ, just for check. Lots of tricky bugs are caught by the check. */
        Assert(CompareKey(meta_index, old_key, cell_key) == 0);
        BinInternalNodeSetCellKey(internal_node, meta_index->key_len, index, new_key);
    } else if (CompareKey(meta_index, old_key, high_key) == 0) {
        BinInternalNodeSetRightKey(internal_node, meta_index->key_len, new_key);
    } else {
        uint32_t next_sibling = BinInternalNodeGetNextSibling(internal_node); 
        Assert(next_sibling != 0);
        BinInsertForInternalNodeUpdateCellKey(meta_index, next_sibling, old_key, new_key);
        goto DirectExit; 
    }

    MakeBufferDirty(buffer);
    
    /* Update current internal node parent. */
    if (!NodeIsRoot(internal_node) && CompareKey(meta_index, new_key, high_key) == 0) {
        uint32_t parent_num;
        parent_num = NodeGetParentNum(internal_node);
        BinInsertForInternalNodeUpdateCellKey(meta_index, parent_num, old_key, new_key);
    }

DirectExit:
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Bin update internal node chidren parent. 
 * ------------------------------------
 * Indeed, update the child node parent, should lock it and update it.
 * But, we do not use any lock, and think it` ok. The reason as follow:
 * There are only two operations to use parent num, insert new item to parent 
 * and update parent cell key. Search operation does not use parent num. 
 * In both the two operations, We use sibling link to make sure find the right target parent.
 * */
static void BinInsertForInternalNodeUpdateChildrenParent(MetaIndex *meta_index, void *internal_node, uint32_t parent_num) {
    uint32_t keys_num, i, right_child_page;
    Buffer right_buffer;
    void *right_child;

    keys_num = BinInternalNodeGetKeysNum(internal_node);
    for (i = 0; i < keys_num; i++) {
        uint32_t child_page;
        Buffer child_buffer;
        void *child_node;

        child_page = BinInternalNodeGetCellValue(internal_node, meta_index->key_len, i);
        child_buffer = ReadBuffer(meta_index->oid, child_page);
        child_node = GetBufferPage(child_buffer);
        NodeSetParentNum(child_node, parent_num);
        
        MakeBufferDirty(child_buffer);
        ReleaseBuffer(child_buffer);
    }

    right_child_page = BinInternalNodeGetRightNum(internal_node);
    right_buffer = ReadBuffer(meta_index->oid, right_child_page);
    right_child = GetBufferPage(right_buffer);
    NodeSetParentNum(right_child, parent_num);

    MakeBufferDirty(right_buffer);
    ReleaseBuffer(right_buffer);
}

/* Bin upgrade root internal node. */
static void BinInsertForInternalNodeUpgradeRoot(MetaIndex *meta_index, void *root, void *right_child, uint32_t right_child_page) {
    uint32_t next_page_num, keys_num, i;
    Buffer new_buffer;
    void *new_internal_node;

    keys_num = BinInternalNodeGetKeysNum(root);
    next_page_num = IndexGetNextUnusedPageNum(meta_index);
    new_buffer = ReadBuffer(meta_index->oid, next_page_num);
    new_internal_node = GetBufferPage(new_buffer);

    /* Initialize new internal node. */
    BinInternalNodeInitialize(new_internal_node, false);
    /* Set keys num, */
    BinInternalNodeSetKeysNum(new_internal_node, keys_num);
    /* Set sibling. */
    BinInternalNodeSetNextSibling(new_internal_node, right_child_page);
    /* Set right child*/
    BinInternalNodeSetRightKey(new_internal_node, meta_index->key_len, 
                               BinInternalNodeGetRightKey(root));
    BinInternalNodeSetRightNum(new_internal_node, 
                               BinInternalNodeGetRightNum(root));

    /* Set cells. */
    for (i = 0; i < keys_num; i++) {
        BinInternalNodeSetCellKey(new_internal_node, meta_index->key_len, i,
                                  BinInternalNodeGetCellKey(root, meta_index->key_len, i));
        BinInternalNodeSetCellValue(new_internal_node, meta_index->key_len, i,
                                    BinInternalNodeGetCellValue(root, meta_index->key_len, i));
    }

    /* Update chidren parent of the new internal node. */
    BinInsertForInternalNodeUpdateChildrenParent(meta_index, new_internal_node, next_page_num);

    /* Set keys num. */
    BinInternalNodeSetKeysNum(root, 1);

    /* Register leaf node to root. */
    BinInternalNodeSetCellKey(root, meta_index->key_len, 0, BinNodeGetHighKey(new_internal_node, meta_index->key_len, meta_index->value_len));
    BinInternalNodeSetCellValue(root, meta_index->key_len, 0, next_page_num);
    BinInternalNodeSetRightKey(root, meta_index->key_len, BinNodeGetHighKey(right_child, meta_index->key_len, meta_index->value_len));
    BinInternalNodeSetRightNum(root, right_child_page);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}

/* Check if bin internal node is safe when inserting new item. */
static bool BinInsertForInternalNodeSafe(void *internal_node, uint32_t key_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    uint32_t keys_num = BinInternalNodeGetKeysNum(internal_node);
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        return (COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * (keys_num + 1)) <= PAGE_SIZE;
    } else
        return (COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * (keys_num + 1)) <= PAGE_SIZE;
}

/* Bin insert item into internal node and split. */
static void BinInsertForInternalNodeSplit(MetaIndex *meta_index, void *internal_node, void *old_child_key, void *old_new_key, void *new_child_key, uint32_t new_child_page) {
    Buffer new_buffer;
    void *new_internal_node, *high_key;
    uint32_t keys_num, right_child_page, next_page_num, target_index, LEFT_SPLIT_COUNT, RIGHT_SPLIT_COUNT;
    
    high_key = copy_block(BinNodeGetHighKey(internal_node, meta_index->key_len, meta_index->value_len), meta_index->key_len);
    keys_num = BinInternalNodeGetKeysNum(internal_node);
    right_child_page = BinInternalNodeGetRightNum(internal_node);

    next_page_num = IndexGetNextUnusedPageNum(meta_index);
    new_buffer = ReadBuffer(meta_index->oid, next_page_num);
    new_internal_node = GetBufferPage(new_buffer);

    /* Initialize new internal node. */
    BinInternalNodeInitialize(new_internal_node, false);
    /* Switch sibling. */
    BinInternalNodeSetNextSibling(new_internal_node, BinInternalNodeGetNextSibling(internal_node));
    BinInternalNodeSetNextSibling(internal_node, next_page_num);

    /* Set parent. */
    NodeSetParentNum(new_internal_node, NodeGetParentNum(internal_node));

    /* We need to deal with two case:
     * (1) The new child key is greater than or equal to high key, which means should be the right child of the internal node. 
     * (2) Otherwise, the new child should be in cells of the internal node. */
    if (CompareKey(meta_index, new_child_key, high_key) >= 0) {
        Assert(CompareKey(meta_index, old_child_key, high_key) == 0);
        BinInternalNodeSetRightKey(internal_node, meta_index->key_len, new_child_key);
        BinInternalNodeSetRightNum(internal_node, new_child_page);

        /* Use the old right child as the new child. */
        new_child_page = right_child_page;
        new_child_key = old_new_key;
        /* Get target index. */
        target_index = keys_num;

        /* Notice: should we update parent internal cell key for the high_key has changed? Yes we should, but not here.
         * Actually, at the function end, we use <BtreeInsertForInternalNodeInsertCell> to update parent internal cell key. */
    } else {
        uint32_t old_target_index;

        old_target_index = BinInternalNodeFindCellNum(meta_index, internal_node, old_child_key);
        Assert(CompareKey(meta_index, old_child_key, BinInternalNodeGetCellKey(internal_node, meta_index->key_len, old_target_index)) == 0); 
        BinInternalNodeSetCellKey(internal_node, meta_index->key_len, old_target_index, old_new_key);
        /* Get target index. */
        target_index = BinInternalNodeFindCellNum(meta_index, internal_node, new_child_key);
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
            BinInternalNodeSetCellKey(destination_node, meta_index->key_len, new_index, new_child_key); 
            BinInternalNodeSetCellValue(destination_node, meta_index->key_len, new_index, new_child_page);
        } else if (i > target_index) {
            /* Right cells make cell space. */
            BinInternalNodeSetCellKey(destination_node, meta_index->key_len, new_index, 
                                      BinInternalNodeGetCellKey(internal_node, meta_index->key_len, i - 1));
            BinInternalNodeSetCellValue(destination_node, meta_index->key_len, new_index, 
                                        BinInternalNodeGetCellValue(internal_node, meta_index->key_len, i - 1));
        } else {
            BinInternalNodeSetCellKey(destination_node, meta_index->key_len, new_index, 
                                      BinInternalNodeGetCellKey(internal_node, meta_index->key_len, i));
            BinInternalNodeSetCellValue(destination_node, meta_index->key_len, new_index, 
                                        BinInternalNodeGetCellValue(internal_node, meta_index->key_len, i));
        }
    }

    /* Set new internal node keys num. */
    BinInternalNodeSetKeysNum(new_internal_node, RIGHT_SPLIT_COUNT);
    /* Set new internal right child. */
    BinInternalNodeSetRightKey(new_internal_node, meta_index->key_len, 
                               BinInternalNodeGetRightKey(internal_node));
    BinInternalNodeSetRightNum(new_internal_node, 
                               BinInternalNodeGetRightNum(internal_node));
    /* Update chidren parent of the new internal node. */
    BinInsertForInternalNodeUpdateChildrenParent(meta_index, new_internal_node, next_page_num);


    /* Set old internal node keys num. */
    BinInternalNodeSetKeysNum(internal_node, LEFT_SPLIT_COUNT - 1);
    /* Set old internal node right child. */
    BinInternalNodeSetRightKey(internal_node, meta_index->key_len, 
                               BinInternalNodeGetCellKey(internal_node, meta_index->key_len, LEFT_SPLIT_COUNT - 1));
    BinInternalNodeSetRightNum(internal_node, 
                               BinInternalNodeGetCellValue(internal_node, meta_index->key_len, LEFT_SPLIT_COUNT -1));


    /* If old internal is root, need to upgrade. 
     * Otherwise, it`s a normal internal node. 
     * Maybe the max key change, need update max key in parent internal node. 
     * Note that: because of new_max_key more likely less than the old_max_key,
     * so mass of parent internal node cells may be happer.
     * We'll resort parent internal node cells lately. 
     * */
    if (NodeIsRoot(internal_node)) 
        BinInsertForInternalNodeUpgradeRoot(meta_index, internal_node, new_internal_node, next_page_num);
    else {
        uint32_t parent_num;
        void *child_key, *old_new_key;

        parent_num = NodeGetParentNum(internal_node);
        old_new_key = BinNodeGetHighKey(internal_node, meta_index->key_len, meta_index->value_len);
        child_key = BinNodeGetHighKey(new_internal_node, meta_index->key_len, meta_index->value_len);

        /* Insert new internal node to parent. */
        BinInsertForInternalNodeInsertCell(meta_index, parent_num, high_key, old_new_key, child_key, next_page_num);
    }

    dfree(high_key);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}


/* Bin insert item into internal node without split. */
static void BinInsertForInternalNodeNoSplit(MetaIndex *meta_index, void *internal_node, void *old_child_key, void *old_new_key, void *new_child_key, uint32_t new_child_page) {
    uint32_t keys_num;
    void *high_key;

    keys_num = BinInternalNodeGetKeysNum(internal_node);
    high_key = BinNodeGetHighKey(internal_node, meta_index->key_len, meta_index->value_len);

    /* We need to deal with two case:
     * (1) The new child key is greater than or equal to high key, which means should be the right child of the internal node. 
     * (2) Otherwise, the new child should be in cells of the internal node. */
    if (CompareKey(meta_index, new_child_key, high_key) >= 0) {
        Assert(CompareKey(meta_index, old_child_key, high_key) == 0);
        BinInternalNodeSetCellKey(internal_node, meta_index->key_len, keys_num, old_new_key);
        BinInternalNodeSetCellValue(internal_node, meta_index->key_len, keys_num, 
                                    BinInternalNodeGetRightNum(internal_node));
        BinInternalNodeSetRightKey(internal_node, meta_index->key_len, new_child_key);
        BinInternalNodeSetRightNum(internal_node, new_child_page); 

        /* If current internal node is not root, 
         * should update it`s parent cell key. */
        if (!NodeIsRoot(internal_node)) {
            uint32_t parent_num = NodeGetParentNum(internal_node);
            BinInsertForInternalNodeUpdateCellKey(meta_index, parent_num, old_child_key, new_child_key);
        }
    } else {
        uint32_t old_target_index, new_target_index, i;
        void *temp;
        
        /* Change the old key. */
        old_target_index = BinInternalNodeFindCellNum(meta_index, internal_node, old_child_key);
        temp = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, old_target_index);
        Assert(CompareKey(meta_index, old_child_key, temp) == 0);
        BinInternalNodeSetCellKey(internal_node, meta_index->key_len, old_target_index, old_new_key);

        /* Append new child. */
        new_target_index = BinInternalNodeFindCellNum(meta_index, internal_node, new_child_key);
        for (i = keys_num; i > new_target_index; i--) {
            BinInternalNodeSetCellKey(internal_node, meta_index->key_len, i, 
                                      BinInternalNodeGetCellKey(internal_node, meta_index->key_len, i - 1));
            BinInternalNodeSetCellValue(internal_node, meta_index->key_len, i, 
                                        BinInternalNodeGetCellValue(internal_node, meta_index->key_len, i - 1));
        }

        /* Set new child cell. */
        BinInternalNodeSetCellKey(internal_node, meta_index->key_len, new_target_index, new_child_key);
        BinInternalNodeSetCellValue(internal_node, meta_index->key_len, new_target_index, new_child_page);
    }

    /* Increase keys num. */
    BinInternalNodeIncreaseKeysNum(internal_node);
}

/* Bin insert item into the internal node.
 * ------------------------------------
 * old_child_key: The old child old cell key.
 * old_new_key: The old child new cell key.
 * new_child_key: The new child key.
 * new_child_page: The new child page.
 * */
static void BinInsertForInternalNodeInsertCell(MetaIndex *meta_index, uint32_t page_num, void *old_child_key, void *old_new_key, void *new_child_key, uint32_t new_child_page) {
    Buffer buffer;
    void *internal_node, *high_key;

    buffer = ReadBuffer(meta_index->oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    internal_node = GetBufferPage(buffer);
    high_key = BinNodeGetHighKey(internal_node, meta_index->key_len, meta_index->value_len);

    /* Only one condition to move to sibling:
     * The old child key is more than high key. */
    if (CompareKey(meta_index, old_child_key, high_key) > 0) {
        uint32_t next_sibling = BinInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        BinInsertForInternalNodeInsertCell(meta_index, next_sibling, old_child_key, old_new_key, new_child_key, new_child_page);
    } else {
        /* If current is safe, just insert new cell, not split.
         * Otherwise, split first and then insert new cell. */
        if (BinInsertForInternalNodeSafe(internal_node, meta_index->key_len))
            BinInsertForInternalNodeNoSplit(meta_index, internal_node, old_child_key, old_new_key, new_child_key, new_child_page);
        else
            BinInsertForInternalNodeSplit(meta_index, internal_node, old_child_key, old_new_key, new_child_key, new_child_page);
            
        /* Make buffer dirty. */
        MakeBufferDirty(buffer);
    }


    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

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
        if (CompareKey(meta_index, cell_key, key) >= 0) 
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
    if (CompareKey(meta_index, boundary_key, high_key) > 0 &&
        CompareKey(meta_index, key, high_key) > 0
    ) {
        uint32_t next_sibling = BinInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        BinInsertForInternalNode(meta_index, key, boundary_key, value, next_sibling);
    } else
        BinInsertForInternalNodeExtend(meta_index, key, value, internal_node);

    dfree(internal_node);
}


/* Root bin leaf node upgrade to root internal node. */
static void BinInsertForLeafNodeUpgradeRoot(MetaIndex *meta_index, void *root, void *right_child, uint32_t right_child_page) {
    Buffer new_buffer;   
    uint32_t cell_num, next_page_num, i;
    void *new_leaf_node;

    cell_num = BinLeafNodeGetCellNum(root);
    next_page_num = IndexGetNextUnusedPageNum(meta_index);
    new_buffer = ReadBuffer(meta_index->oid, next_page_num);
    new_leaf_node = GetBufferPage(new_buffer);
    
    /* Initialize leaf node. */
    BinLeafNodeInitialize(new_leaf_node, false);
    /* Set cell num. */
    BinLeafNodeSetCellNum(new_leaf_node, cell_num);
    /* Set sibling. */
    BinLeafNodeSetNextSibling(new_leaf_node, right_child_page);

    /* Copy each cell. */
    for (i = 0; i < cell_num; i++) {
        BinLeafNodeSetCellKey(new_leaf_node, meta_index->key_len, meta_index->value_len, i,
                              BinLeafNodeGetCellKey(root, meta_index->key_len, meta_index->value_len, i));
        BinLeafNodeSetCellValue(new_leaf_node, meta_index->key_len, meta_index->value_len, i, 
                                BinLeafNodeGetCellValue(root, meta_index->key_len, meta_index->value_len, i));
    }
    
    /* Set parent. */
    NodeSetParentNum(new_leaf_node, ROOT_PAGE_NUM);
    NodeSetParentNum(right_child, ROOT_PAGE_NUM);

    /* Make clear outsides header. */
    uint32_t ROOT_LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE + 
                                          BIN_ROOT_NODE_COLUMN_SIZE_SIZE + BIN_ROOT_NODE_COLUMN_NAME_SIZE * meta_index->column_size;
    memset(root + ROOT_LEAF_NODE_HEADER_SIZE, 0, PAGE_SIZE - ROOT_LEAF_NODE_HEADER_SIZE);
    
    /* upgrade to internal node. */
    SetNodeType(root, INTERNAL_NODE);

    /* Set keys num. */
    BinInternalNodeSetKeysNum(root, 1);

    /* Register leaf node to root. */
    BinInternalNodeSetCellKey(root, meta_index->key_len, 0, BinNodeGetHighKey(new_leaf_node, meta_index->key_len, meta_index->value_len));
    BinInternalNodeSetCellValue(root, meta_index->key_len, 0, next_page_num);
    BinInternalNodeSetRightKey(root, meta_index->key_len, BinNodeGetHighKey(right_child, meta_index->key_len, meta_index->value_len));
    BinInternalNodeSetRightNum(root, right_child_page);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
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

/* Bin insert into a cell and split leaf node. */
static void BinInsertForLeafNodeSplit(MetaIndex *meta_index, void *key, void *value, Buffer buffer) {
    Buffer new_buffer;
    uint32_t cell_num, target_index, next_page_num, LEFT_SPLIT_COUNT, RIGHT_SPLIT_COUNT;
    void *leaf_node, *high_key, *cell_key, *new_leaf_node;

    leaf_node = GetBufferPage(buffer);
    cell_num = BinLeafNodeGetCellNum(leaf_node);
    high_key = copy_block(BinNodeGetHighKey(leaf_node, meta_index->key_len, meta_index->value_len), meta_index->key_len);
    target_index = BinLeafNodeFindCellNum(meta_index, leaf_node, key);
    cell_key = BinLeafNodeGetCellKey(leaf_node, meta_index->key_len, meta_index->value_len, target_index);

    /* Avoid duplicate key, two conditions:
     * (1) The index is unique.
     * (2) Duplicate key already exists. */
    if (meta_index->is_unique && CompareKey(meta_index, key, cell_key) == 0) {
        Table *table;
        Refer *refer;
        void *tuple;
        int predicate;
        Xid current_xid, created_xid, expired_xid;
        
        table = open_table_inner(meta_index->tid);
        refer = BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, target_index);
        tuple = HeapTableLookupTuple(meta_index->tid, refer);

        current_xid = GetCurrentXid();
        created_xid = TupleFindCreatedXid(tuple, table->meta_table);
        expired_xid = TupleFindExpiredXid(tuple, table->meta_table);

        predicate = BinInsertDuplicateKeyPredicate(current_xid, created_xid, expired_xid);

        switch (predicate) {
            case OK:
                break;
            case WAIT:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                return BinInsertWaitForRetry(meta_index, key, value, created_xid, expired_xid);
            case ERRO:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                db_log(ERROR, "Not allow duplicate key.");
                break;
        }
    }

    next_page_num = IndexGetNextUnusedPageNum(meta_index);
    new_buffer = ReadBuffer(meta_index->oid, next_page_num);
    new_leaf_node = GetBufferPage(new_buffer);

    /* Initialize leaf node. */
    BinLeafNodeInitialize(new_leaf_node, false);
    /* Switch sibling. */
    BinLeafNodeSetNextSibling(new_leaf_node, BinLeafNodeGetNextSibling(leaf_node));
    BinLeafNodeSetNextSibling(leaf_node, next_page_num);

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
        uint32_t new_index;
        void *destination_node, *destination;

        /* If index GT than LEAF_SPLIT_COUNT, destination is new old, 
         * othersize, stay in the old node. */
        destination_node = (i >= LEFT_SPLIT_COUNT) 
                            ? new_leaf_node 
                            : leaf_node;
        /* New position. */
        new_index = i % LEFT_SPLIT_COUNT;
        destination = BinLeafNodeGetCellValue(destination_node, meta_index->key_len, meta_index->value_len, new_index);

        /* The cursor rigth cells should move one cell to the right to make space for the cursor, 
         * include the cell having the old same num as cursor. The cursor leaf cells don`t need to make space.
         * Because i start with cell number and decrease, right cells firstly move and make space. */
        if (i == target_index) {
            /* Deposit cursor. */
            BinLeafNodeSetCellKey(destination_node, meta_index->key_len, meta_index->value_len, new_index, key);
            BinLeafNodeSetCellValue(destination_node, meta_index->key_len, meta_index->value_len,  new_index, value);
        } else if (i > target_index) 
            /* Define new position, and right cells make cell space. */
            memcpy(destination, 
                   BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, i - 1), 
                   meta_index->key_len + meta_index->value_len);
        else
            /* Define new position. */
            memcpy(destination, 
                   BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, i), 
                   meta_index->key_len + meta_index->value_len);
    }

    /* Reset cell num. */
    BinLeafNodeSetCellNum(leaf_node, LEFT_SPLIT_COUNT);
    BinLeafNodeSetCellNum(new_leaf_node, RIGHT_SPLIT_COUNT);

    /* If current is root, it need to upgrade to internal root node. 
     * Otherwise, it is a normal leaf node, maybe the max key change, need update max key in parent internal node. 
     * */
    if (NodeIsRoot(leaf_node))
        BinInsertForLeafNodeUpgradeRoot(meta_index, leaf_node, new_leaf_node, next_page_num);
    else {
        uint32_t parent_num;
        void *new_hight_key, *child_key;

        parent_num = NodeGetParentNum(leaf_node);
        new_hight_key = BinNodeGetHighKey(leaf_node, meta_index->key_len, meta_index->value_len); 
        child_key = BinNodeGetHighKey(new_leaf_node, meta_index->key_len, meta_index->value_len);
        
        /* Insert new leaf into parent. */
        BinInsertForInternalNodeInsertCell(meta_index, parent_num, high_key, new_hight_key, child_key, next_page_num);
    }

    dfree(high_key);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Bin insert into a cell and not split leaf node. */
static void BinInsertForLeafNodeNoSplit(MetaIndex *meta_index, void *key, void *value, Buffer buffer) {
    uint32_t cell_num, target_index;
    void *leaf_node, *cell_key;
    
    leaf_node = GetBufferPage(buffer);
    cell_num = BinLeafNodeGetCellNum(leaf_node);
    target_index = BinLeafNodeFindCellNum(meta_index, leaf_node, key);
    cell_key = BinLeafNodeGetCellKey(leaf_node, meta_index->key_len, meta_index->value_len, target_index);

    /* Avoid duplicate key, two conditions:
     * (1) The index is unique.
     * (2) Duplicate key already exists. */
    if (meta_index->is_unique && CompareKey(meta_index, key, cell_key) == 0) {
        Table *table;
        Refer *refer;
        void *tuple;
        int predicate;
        Xid current_xid, created_xid, expired_xid;
        
        table = open_table_inner(meta_index->tid);
        refer = BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, target_index);
        tuple = HeapTableLookupTuple(meta_index->tid, refer);

        current_xid = GetCurrentXid();
        created_xid = TupleFindCreatedXid(tuple, table->meta_table);
        expired_xid = TupleFindExpiredXid(tuple, table->meta_table);

        predicate = BinInsertDuplicateKeyPredicate(current_xid, created_xid, expired_xid);

        switch (predicate) {
            case OK:
                break;
            case WAIT:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                return BinInsertWaitForRetry(meta_index, key, value, created_xid, expired_xid);
            case ERRO:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                db_log(ERROR, "Not allow duplicate key.");
                break;
        }
    }

    /* If need to move sibling cells.*/
    if (target_index < cell_num) {
        /* Make sure move sibling cell from right to left. */
        int i;
        for (i = cell_num; i > target_index; i--) {
            /* Movement. */
            memcpy(
                BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, i),
                BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, i - 1),
                meta_index->key_len + meta_index->value_len
            );
        }
    }

    /* Set cell key. */
    BinLeafNodeSetCellKey(leaf_node, meta_index->key_len, meta_index->value_len, target_index, key);
    /* Set cell value. */
    BinLeafNodeSetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, target_index, value);
    /* Increase cell num. */
    BinLeafNodeIncreaseCellNum(leaf_node);
    
    /* Maybe insertion cause high key change. 
     * If it does, need to update parent key. */
    if (!NodeIsRoot(leaf_node) && target_index == cell_num) {
        uint32_t parent_num;
        void *old_key;

        parent_num = NodeGetParentNum(leaf_node);
        old_key = BinLeafNodeGetCellKey(leaf_node, meta_index->key_len, meta_index->value_len, cell_num - 1);

        BinInsertForInternalNodeUpdateCellKey(meta_index, parent_num, old_key, key);
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
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
    if (CompareKey(meta_index, boundary_key, high_key) > 0 &&
        CompareKey(meta_index, key, high_key) > 0
    ) {
        uint32_t next_sibling = BinLeafNodeGetNextSibling(leaf_node);
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
