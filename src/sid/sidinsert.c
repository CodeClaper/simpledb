#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "sidinsert.h"
#include "sidbase.h"
#include "ltbase.h"
#include "bufmgr.h"
#include "compare.h"
#include "const.h"
#include "systable.h"
#include "heaptable.h"
#include "table.h"
#include "trans.h"
#include "tuple.h"
#include "pager.h"
#include "log.h"
#include "mmgr.h"

#define OK   1
#define WAIT 0
#define ERRO -1

static void SidInsertForInternalNodeInsertCell(Table *table, uint32_t page_num, Sid old_child_key, Sid old_new_key, Sid new_child_key, uint32_t new_child_page);
static void SidInsertInner(Table *table, Sid key, Sid boundary_key, Refer *refer, uint32_t page_num);

/* Predicate duplicate key.
 * --------------------------
 * These are three duplicate key type.
 * (1) OK: A deleted duplicate key which does not need to care about.
 * (2) WAIT: An un-commited duplicate key which need to wait for commit.
 * (3) ERRO: A commited duplicate key which case duplicate key issue. 
 * */
static int SidInsertDuplicateKeyPredicate(Xid current_xid, Xid created_xid, Xid expired_xid) {
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

static void SidInsertWaitForRetry(Oid soid, Sid key, Refer *refer, Xid created_xid, Xid expired_xid) {
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
    SidInsert(soid, key, refer);
}

/* Sid insert for internal node to update its cell key. */
static void SidInsertForInternalNodeUpdateCellKey(Oid soid, uint32_t page_num, Sid old_key, Sid new_key) {
    Buffer buffer;
    void *internal_node;
    Sid high_key;

    buffer = ReadBuffer(soid, page_num);
    LockBuffer(buffer, RW_WRITER);
    internal_node = GetBufferPage(buffer);
    high_key = SidNodeGetHighKey(internal_node);


    /* These are three cases:
     * (1) Old key is less than high key, which means it is in the cells of the internal node. 
     * (2) Old key is equals to high key, which means it is the right child.
     * (3) Old key is more than high key, which means the old internal node has spliten, and need to move to next sibling to search. */
    if (old_key < high_key) {
        uint32_t index;
        Sid cell_key;

        index = SidInternalNodeFindCellNum(internal_node, old_key);
        cell_key = SidInternalNodeGetCellKey(internal_node, index);
        /* Theoretically EQ, just for check. Lots of tricky bugs are caught by the check. */
        Assert(old_key == cell_key);
        SidInternalNodeSetCellKey(internal_node, index, new_key);
    } else if (old_key == high_key) {
        SidInternalNodeSetRightKey(internal_node, new_key);
    } else {
        uint32_t next_sibling = SidInternalNodeGetNextSibling(internal_node); 
        Assert(next_sibling != 0);
        SidInsertForInternalNodeUpdateCellKey(soid, next_sibling, old_key, new_key);
        goto DirectExit; 
    }

    MakeBufferDirty(buffer);
    
    /* Update current internal node parent. */
    if (!NodeIsRoot(internal_node) && new_key == high_key) {
        uint32_t parent_num;
        parent_num = NodeGetParentNum(internal_node);
        SidInsertForInternalNodeUpdateCellKey(soid, parent_num, old_key, new_key);
    }

DirectExit:
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}


/* Sid update internal node chidren parent. 
 * ------------------------------------
 * Indeed, update the child node parent, should lock it and update it.
 * But, we do not use any lock, and think it` ok. The reason as follow:
 * There are only two operations to use parent num, insert new item to parent 
 * and update parent cell key. Search operation does not use parent num. 
 * In both the two operations, We use sibling link to make sure find the right target parent.
 * */
static void SidInsertForInternalNodeUpdateChildrenParent(Oid soid, void *internal_node, uint32_t parent_num) {
    uint32_t keys_num, i, right_child_page;
    Buffer right_buffer;
    void *right_child;

    keys_num = SidInternalNodeGetKeysNum(internal_node);
    for (i = 0; i < keys_num; i++) {
        uint32_t child_page;
        Buffer child_buffer;
        void *child_node;

        child_page = SidInternalNodeGetCellValue(internal_node, i);
        child_buffer = ReadBuffer(soid, child_page);
        child_node = GetBufferPage(child_buffer);
        NodeSetParentNum(child_node, parent_num);
        
        MakeBufferDirty(child_buffer);
        ReleaseBuffer(child_buffer);
    }

    right_child_page = SidInternalNodeGetRightNum(internal_node);
    right_buffer = ReadBuffer(soid, right_child_page);
    right_child = GetBufferPage(right_buffer);
    NodeSetParentNum(right_child, parent_num);

    MakeBufferDirty(right_buffer);
    ReleaseBuffer(right_buffer);
}

/* Root sid internal node upgrade to root internal node. */
static void SidInsertForInternalNodeUpgradeRoot(Table *table, void *root, void *right_child, uint32_t right_child_page) {
    uint32_t next_page_num, keys_num, i;
    Buffer new_buffer;
    void *new_internal_node;

    keys_num = SidInternalNodeGetKeysNum(root);
    next_page_num = GetNextUnusedSidPageNum(table);
    new_buffer = ReadBuffer(table->soid, next_page_num);
    new_internal_node = GetBufferPage(new_buffer);

    /* Initialize new internal node. */
    SidInternalNodeInitialize(new_internal_node, false);
    /* Set keys num, */
    SidInternalNodeSetKeysNum(new_internal_node, keys_num);
    /* Set sibling. */
    SidInternalNodeSetNextSibling(new_internal_node, right_child_page);
    /* Set right child*/
    SidInternalNodeSetRightKey(new_internal_node, SidInternalNodeGetRightKey(root));
    SidInternalNodeSetRightNum(new_internal_node, SidInternalNodeGetRightNum(root));

    /* Set cells. */
    for (i = 0; i < keys_num; i++) {
        SidInternalNodeSetCellKey(new_internal_node, i, SidInternalNodeGetCellKey(root, i));
        SidInternalNodeSetCellValue(new_internal_node, i, SidInternalNodeGetCellValue(root, i));
    }

    /* Update chidren parent of the new internal node. */
    SidInsertForInternalNodeUpdateChildrenParent(table->soid, new_internal_node, next_page_num);

    /* Set keys num. */
    SidInternalNodeSetKeysNum(root, 1);

    /* Register leaf node to root. */
    SidInternalNodeSetCellKey(root, 0, SidNodeGetHighKey(new_internal_node));
    SidInternalNodeSetCellValue(root, 0, next_page_num);
    SidInternalNodeSetRightKey(root, SidNodeGetHighKey(right_child));
    SidInternalNodeSetRightNum(root, right_child_page);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}

/* Check if sid internal node is safe when inserting new item. */
static bool SidInsertForInternalNodeSafe(void *internal_node) {
    uint32_t keys_num = SidInternalNodeGetKeysNum(internal_node);
    return (COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + SID_KEY_LENGTH + SID_INTERNAL_NODE_CELL_LENGTH * (keys_num + 1)) <= PAGE_SIZE;
}

/* Sid insert item into internal node and split. */
static void SidInsertForInternalNodeSplit(Table *table, void *internal_node, Sid old_child_key, Sid old_new_key, Sid new_child_key, uint32_t new_child_page) {
    Buffer new_buffer;
    void *new_internal_node;
    Sid high_key;
    uint32_t keys_num, right_child_page, next_page_num, target_index, LEFT_SPLIT_COUNT, RIGHT_SPLIT_COUNT;
    
    high_key = SidNodeGetHighKey(internal_node);
    keys_num = SidInternalNodeGetKeysNum(internal_node);
    right_child_page = SidInternalNodeGetRightNum(internal_node);

    next_page_num = GetNextUnusedSidPageNum(table);
    new_buffer = ReadBuffer(table->soid, next_page_num);
    new_internal_node = GetBufferPage(new_buffer);

    /* Initialize new internal node. */
    SidInternalNodeInitialize(new_internal_node, false);
    /* Switch sibling. */
    SidInternalNodeSetNextSibling(new_internal_node, SidInternalNodeGetNextSibling(internal_node));
    SidInternalNodeSetNextSibling(internal_node, next_page_num);

    /* Set parent. */
    NodeSetParentNum(new_internal_node, NodeGetParentNum(internal_node));

    /* We need to deal with two case:
     * (1) The new child key is greater than or equal to high key, which means should be the right child of the internal node. 
     * (2) Otherwise, the new child should be in cells of the internal node. */
    if (new_child_key >= high_key) {
        Assert(old_child_key >= high_key);
        SidInternalNodeSetRightKey(internal_node, new_child_key);
        SidInternalNodeSetRightNum(internal_node, new_child_page);

        /* Use the old right child as the new child. */
        new_child_page = right_child_page;
        new_child_key = old_new_key;
        /* Get target index. */
        target_index = keys_num;

        /* Notice: should we update parent internal cell key for the high_key has changed? Yes we should, but not here.
         * Actually, at the function end, we use <BtreeInsertForInternalNodeInsertCell> to update parent internal cell key. */
    } else {
        uint32_t old_target_index;

        old_target_index = SidInternalNodeFindCellNum(internal_node, old_child_key);
        Assert(old_child_key == SidInternalNodeGetCellKey(internal_node, old_target_index)); 
        SidInternalNodeSetCellKey(internal_node, old_target_index, old_new_key);
        /* Get target index. */
        target_index = SidInternalNodeFindCellNum(internal_node, new_child_key);
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
            SidInternalNodeSetCellKey(destination_node, new_index, new_child_key); 
            SidInternalNodeSetCellValue(destination_node, new_index, new_child_page);
        } else if (i > target_index) {
            /* Right cells make cell space. */
            SidInternalNodeSetCellKey(destination_node, new_index, SidInternalNodeGetCellKey(internal_node, i - 1));
            SidInternalNodeSetCellValue(destination_node, new_index, SidInternalNodeGetCellValue(internal_node, i - 1));
        } else {
            SidInternalNodeSetCellKey(destination_node, new_index, SidInternalNodeGetCellKey(internal_node, i));
            SidInternalNodeSetCellValue(destination_node, new_index, SidInternalNodeGetCellValue(internal_node, i));
        }
    }

    /* Set new internal node keys num. */
    SidInternalNodeSetKeysNum(new_internal_node, RIGHT_SPLIT_COUNT);
    /* Set new internal right child. */
    SidInternalNodeSetRightKey(new_internal_node, SidInternalNodeGetRightKey(internal_node));
    SidInternalNodeSetRightNum(new_internal_node, SidInternalNodeGetRightNum(internal_node));
    /* Update chidren parent of the new internal node. */
    SidInsertForInternalNodeUpdateChildrenParent(table->soid, new_internal_node, next_page_num);


    /* Set old internal node keys num. */
    SidInternalNodeSetKeysNum(internal_node, LEFT_SPLIT_COUNT - 1);
    /* Set old internal node right child. */
    SidInternalNodeSetRightKey(internal_node, SidInternalNodeGetCellKey(internal_node,  LEFT_SPLIT_COUNT - 1));
    SidInternalNodeSetRightNum(internal_node, SidInternalNodeGetCellValue(internal_node,  LEFT_SPLIT_COUNT -1));


    /* If old internal is root, need to upgrade. 
     * Otherwise, it`s a normal internal node. 
     * Maybe the max key change, need update max key in parent internal node. 
     * Note that: because of new_max_key more likely less than the old_max_key,
     * so mass of parent internal node cells may be happer.
     * We'll resort parent internal node cells lately. 
     * */
    if (NodeIsRoot(internal_node)) 
        SidInsertForInternalNodeUpgradeRoot(table, internal_node, new_internal_node, next_page_num);
    else {
        uint32_t parent_num;
        Sid child_key, old_new_key;

        parent_num = NodeGetParentNum(internal_node);
        old_new_key = SidNodeGetHighKey(internal_node);
        child_key = SidNodeGetHighKey(new_internal_node);

        /* Insert new internal node to parent. */
        SidInsertForInternalNodeInsertCell(table, parent_num, high_key, old_new_key, child_key, next_page_num);
    }


    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}

/* Sid insert item to internal node without split. */
static void SidInsertForInternalNodeNoSplit(Table *table, void *internal_node, Sid old_child_key, Sid old_new_key, Sid new_child_key, uint32_t new_child_page) {
    uint32_t keys_num;
    Sid high_key;

    keys_num = SidInternalNodeGetKeysNum(internal_node);
    high_key = SidNodeGetHighKey(internal_node);

    /* We need to deal with two case:
     * (1) The new child key is greater than or equal to high key, which means should be the right child of the internal node. 
     * (2) Otherwise, the new child should be in cells of the internal node. */
    if (new_child_key >= high_key) {
        Assert(old_child_key == high_key);
        SidInternalNodeSetCellKey(internal_node, keys_num, old_new_key);
        SidInternalNodeSetCellValue(internal_node, keys_num, SidInternalNodeGetRightNum(internal_node));
        SidInternalNodeSetRightKey(internal_node, new_child_key);
        SidInternalNodeSetRightNum(internal_node, new_child_page); 

        /* If current internal node is not root, 
         * should update it`s parent cell key. */
        if (!NodeIsRoot(internal_node)) {
            uint32_t parent_num = NodeGetParentNum(internal_node);
            SidInsertForInternalNodeUpdateCellKey(table->soid, parent_num, old_child_key, new_child_key);
        }
    } else {
        uint32_t old_target_index, new_target_index, i;
        Sid temp;
        
        /* Change the old key. */
        old_target_index = SidInternalNodeFindCellNum(internal_node, old_child_key);
        temp = SidInternalNodeGetCellKey(internal_node, old_target_index);
        Assert(old_child_key == temp);
        SidInternalNodeSetCellKey(internal_node, old_target_index, old_new_key);

        /* Append new child. */
        new_target_index = SidInternalNodeFindCellNum(internal_node, new_child_key);
        for (i = keys_num; i > new_target_index; i--) {
            SidInternalNodeSetCellKey(internal_node, i, SidInternalNodeGetCellKey(internal_node, i - 1));
            SidInternalNodeSetCellValue(internal_node, i, SidInternalNodeGetCellValue(internal_node, i - 1));
        }

        /* Set new child cell. */
        SidInternalNodeSetCellKey(internal_node, new_target_index, new_child_key);
        SidInternalNodeSetCellValue(internal_node, new_target_index, new_child_page);
    }

    /* Increase keys num. */
    SidInternalNodeIncreaseKeysNum(internal_node);
}

/* Sid insert item into the internal node.
 * ------------------------------------
 * old_child_key: The old child old cell key.
 * old_new_key: The old child new cell key.
 * new_child_key: The new child key.
 * new_child_page: The new child page.
 * */
static void SidInsertForInternalNodeInsertCell(Table *table, uint32_t page_num, Sid old_child_key, Sid old_new_key, Sid new_child_key, uint32_t new_child_page) {
    Buffer buffer;
    void *internal_node;
    Sid high_key;

    buffer = ReadBuffer(table->soid, page_num);
    LockBuffer(buffer, RW_WRITER);
    internal_node = GetBufferPage(buffer);
    high_key = SidNodeGetHighKey(internal_node);

    /* Only one condition to move to sibling:
     * The old child key is more than high key. */
    if (old_child_key > high_key) {
        uint32_t next_sibling = SidInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        SidInsertForInternalNodeInsertCell(table, next_sibling, old_child_key, old_new_key, new_child_key, new_child_page);
    } else {
        /* If current is safe, just insert new cell, not split.
         * Otherwise, split first and then insert new cell. */
        if (SidInsertForInternalNodeSafe(internal_node))
            SidInsertForInternalNodeNoSplit(table, internal_node, old_child_key, old_new_key, new_child_key, new_child_page);
        else
            SidInsertForInternalNodeSplit(table, internal_node, old_child_key, old_new_key, new_child_key, new_child_page);
            
        /* Make buffer dirty. */
        MakeBufferDirty(buffer);
    }


    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Sid insert for internal node. 
 * ----------------------------
 * Will use binnary seach to find the next level child cell.
 * */
static void SidInsertForInternalNodeExtend(Table *table, Sid key, Refer *refer, void *internal_node) {
    Sid boundary_key;
    uint32_t keys_num, min_index, max_index, target_page;
    
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
        db_log(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, 
               keys_num);
    else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = SidInternalNodeGetRightKey(internal_node);
        target_page = SidInternalNodeGetRightNum(internal_node);
        SidInsertInner(table, key, boundary_key, refer, target_page);
    } else {
        /* The target cell in cells. */
        boundary_key = SidInternalNodeGetCellKey(internal_node, min_index);
        target_page = SidInternalNodeGetCellValue(internal_node, min_index);
        SidInsertInner(table, key, boundary_key, refer, target_page);
    }
}

/* Sid insert for internal node. */
static void SidInsertForInternalNode(Table *table, Sid key, Sid boundary_key, Refer *refer, uint32_t page_num) {
    Buffer buffer;
    void *internal_node;
    Sid high_key;

    buffer = ReadBuffer(table->soid, page_num);
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
        SidInsertForInternalNode(table, key, boundary_key, refer, next_sibling);
    } else
        SidInsertForInternalNodeExtend(table, key, refer, internal_node);

    dfree(internal_node);
}


/* Root sid leaf node upgrade to root internal node. */
static void SidInsertForLeafNodeUpgradeRoot(Table *table, void *root, void *right_child, uint32_t right_child_page) {
    Buffer new_buffer;   
    uint32_t cell_num, next_page_num, i;
    void *new_leaf_node;

    cell_num = SidLeafNodeGetCellNum(root);
    next_page_num = GetNextUnusedSidPageNum(table);
    new_buffer = ReadBuffer(table->soid, next_page_num);
    new_leaf_node = GetBufferPage(new_buffer);
    
    /* Initialize leaf node. */
    SidLeafNodeInitialize(new_leaf_node, false);
    /* Set cell num. */
    SidLeafNodeSetCellNum(new_leaf_node, cell_num);
    /* Set sibling. */
    SidLeafNodeSetNextSibling(new_leaf_node, right_child_page);

    /* Copy each cell. */
    for (i = 0; i < cell_num; i++) {
        SidLeafNodeSetCellKey(new_leaf_node, i, SidLeafNodeGetCellKey(root, i));
        SidLeafNodeSetCellValue(new_leaf_node, i, SidLeafNodeGetCellValue(root, i));
    }
    
    /* Set parent. */
    NodeSetParentNum(new_leaf_node, ROOT_PAGE_NUM);
    NodeSetParentNum(right_child, ROOT_PAGE_NUM);

    /* Make clear outsides header. */
    memset(root + LEAF_NODE_HEAD_SIZE, 0, PAGE_SIZE - LEAF_NODE_HEAD_SIZE);
    
    /* upgrade to internal node. */
    SetNodeType(root, INTERNAL_NODE);

    /* Set keys num. */
    SidInternalNodeSetKeysNum(root, 1);

    /* Register leaf node to root. */
    SidInternalNodeSetCellKey(root, 0, SidNodeGetHighKey(new_leaf_node));
    SidInternalNodeSetCellValue(root, 0, next_page_num);
    SidInternalNodeSetRightKey(root, SidNodeGetHighKey(right_child));
    SidInternalNodeSetRightNum(root, right_child_page);

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);
}

/* Check if sid leaf node is safe afer inserting new cell. */
static bool SidInsertForLeafNodeSafe(void *leaf_node) {
    uint32_t cell_num = SidLeafNodeGetCellNum(leaf_node);
    return LEAF_NODE_HEAD_SIZE + SID_LEAF_NODE_CELL_LENGTH * (cell_num + 1) <= PAGE_SIZE;
}

/* Sid insert a cell into leaf node and split it. */
static void SidInsertForLeafNodeSplit(Table *table, Buffer buffer, Sid key, Refer *refer) {
    Buffer new_buffer;
    void *leaf_node, *new_leaf_node;
    uint32_t cell_num, target_index, next_page_num, LEFT_SPLIT_COUNT, RIGHT_SPLIT_COUNT;
    Sid cell_key, high_key;

    leaf_node = GetBufferPage(buffer);
    cell_num = SidLeafNodeGetCellNum(leaf_node);
    target_index = SidLeafNodeFindCellNum(leaf_node, key);
    high_key = SidNodeGetHighKey(leaf_node);
    cell_key = SidLeafNodeGetCellKey(leaf_node, target_index);

    /* Avoid duplicate key. */
    if (key == cell_key) {
        Refer *target;
        void *tuple;
        int predicate;
        Xid current_xid, created_xid, expired_xid;

        target = SidLeafNodeGetCellValue(leaf_node, target_index);
        tuple = HeapTableLookupTuple(table->oid, target);

        current_xid = GetCurrentXid();
        created_xid = TupleFindCreatedXid(tuple, table->meta_table);
        expired_xid = TupleFindExpiredXid(tuple, table->meta_table);

        predicate = SidInsertDuplicateKeyPredicate(current_xid, created_xid, expired_xid);

        switch (predicate) {
            case OK:
                break;
            case WAIT:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                return SidInsertWaitForRetry(table->soid, key, refer, created_xid, expired_xid);
            case ERRO:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                db_log(ERROR, "Not allow duplicate key.");
                break;
        }
    }

    next_page_num = GetNextUnusedSidPageNum(table);
    new_buffer = ReadBuffer(table->soid, next_page_num);
    new_leaf_node = GetBufferPage(new_buffer);

    /* Initialize leaf node. */
    SidLeafNodeInitialize(new_leaf_node, false);
    /* Switch sibling. */
    SidLeafNodeSetNextSibling(new_leaf_node, SidLeafNodeGetNextSibling(leaf_node));
    SidLeafNodeSetNextSibling(leaf_node, next_page_num);

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
        destination = SidLeafNodeGetCellValue(destination_node, new_index);

        /* The cursor rigth cells should move one cell to the right to make space for the cursor, 
         * include the cell having the old same num as cursor. The cursor leaf cells don`t need to make space.
         * Because i start with cell number and decrease, right cells firstly move and make space. */
        if (i == target_index) {
            /* Deposit cursor. */
            SidLeafNodeSetCellKey(destination_node, new_index, key);
            SidLeafNodeSetCellValue(destination_node, new_index, refer);
        } else if (i > target_index) 
            /* Define new position, and right cells make cell space. */
            memcpy(destination, 
                   SidLeafNodeGetCellValue(leaf_node, i - 1), 
                   SID_LEAF_NODE_CELL_LENGTH);
        else
            /* Define new position. */
            memcpy(destination, 
                   SidLeafNodeGetCellValue(leaf_node, i), 
                   SID_LEAF_NODE_CELL_LENGTH);
    }

    /* Reset cell num. */
    SidLeafNodeSetCellNum(leaf_node, LEFT_SPLIT_COUNT);
    SidLeafNodeSetCellNum(new_leaf_node, RIGHT_SPLIT_COUNT);

    /* If current is root, it need to upgrade to internal root node. 
     * Otherwise, it is a normal leaf node, maybe the max key change, need update max key in parent internal node. 
     * */
    if (NodeIsRoot(leaf_node))
        SidInsertForLeafNodeUpgradeRoot(table, leaf_node, new_leaf_node, next_page_num);
    else {
        uint32_t parent_num;
        Sid new_hight_key, child_key;

        parent_num = NodeGetParentNum(leaf_node);
        new_hight_key = SidNodeGetHighKey(leaf_node); 
        child_key = SidNodeGetHighKey(new_leaf_node);
        
        /* Insert new leaf into parent. */
        SidInsertForInternalNodeInsertCell(table, parent_num, high_key, new_hight_key, child_key, next_page_num);
    }

    MakeBufferDirty(new_buffer);
    ReleaseBuffer(new_buffer);

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Sid insert a cell into leaf node, not split. */
static void SidInsertForLeafNodeNoSplit(Table *table, Buffer buffer, Sid key, Refer *refer) {
    void *leaf_node;
    uint32_t cell_num, target_index;
    Sid cell_key;

    leaf_node = GetBufferPage(buffer);
    cell_num = SidLeafNodeGetCellNum(leaf_node);
    target_index = SidLeafNodeFindCellNum(leaf_node, key);
    cell_key = SidLeafNodeGetCellKey(leaf_node, target_index);

    /* Avoid duplicate key. */
    if (key == cell_key) {
        Refer *target;
        void *tuple;
        int predicate;
        Xid current_xid, created_xid, expired_xid;

        target = SidLeafNodeGetCellValue(leaf_node, target_index);
        tuple = HeapTableLookupTuple(table->oid, target);

        current_xid = GetCurrentXid();
        created_xid = TupleFindCreatedXid(tuple, table->meta_table);
        expired_xid = TupleFindExpiredXid(tuple, table->meta_table);

        predicate = SidInsertDuplicateKeyPredicate(current_xid, created_xid, expired_xid);

        switch (predicate) {
            case OK:
                break;
            case WAIT:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                return SidInsertWaitForRetry(table->soid, key, refer, created_xid, expired_xid);
            case ERRO:
                UnlockBuffer(buffer);
                ReleaseBuffer(buffer);
                db_log(ERROR, "Not allow duplicate key.");
                break;
        }
    }

    /* If need to move siling cells. */
    if (target_index < cell_num) {
        /* Make sure move sibling cell from right to left. */
        int i;
        for (i = cell_num; i > target_index; i--) {
            /* Movement. */
            memcpy(
                SidLeafNodeGetCellValue(leaf_node, i),
                SidLeafNodeGetCellValue(leaf_node, i - 1),
                SID_LEAF_NODE_CELL_LENGTH
            );
        }
    }

    /* Set cell key. */
    SidLeafNodeSetCellKey(leaf_node, target_index, key);
    /* Set cell value. */
    SidLeafNodeSetCellValue(leaf_node, target_index, refer);
    /* Increase cell num. */
    SidLeafNodeIncreaseCellNum(leaf_node);

    /* Maybe insertion cause high key change. 
     * If it does, need to update parent key. */
    if (!NodeIsRoot(leaf_node) && target_index == cell_num) {
        uint32_t parent_num;
        Sid old_key;

        parent_num = NodeGetParentNum(leaf_node);
        old_key = SidLeafNodeGetCellKey(leaf_node, cell_num - 1);

        SidInsertForInternalNodeUpdateCellKey(table->soid, parent_num, old_key, key);
    }

    MakeBufferDirty(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Sid insert a cell into leaf node. */
static void SidInsertForLeafNodeInsertCell(Table *table, Sid key, Refer *refer, Buffer buffer) {
    void *leaf_node = GetBufferPage(buffer);

    if (SidInsertForLeafNodeSafe(leaf_node))
        SidInsertForLeafNodeNoSplit(table, buffer, key, refer);
    else
        SidInsertForLeafNodeSplit(table, buffer, key, refer);
}

/* Sid insert new item for leaf node. */
static void SidInsertForLeafNode(Table *table, Sid key, Sid boundary_key, Refer *refer, uint32_t page_num) {
    Buffer buffer;
    void *leaf_node;
    Sid high_key;

    buffer = ReadBuffer(table->soid, page_num);
    LockBuffer(buffer, RW_WRITER);
    leaf_node = GetBufferPage(buffer);
    high_key = SidNodeGetHighKey(leaf_node);

    /* Conditions to move to sibling:
     * (1) The target node has spliten.
     * (2) The key is greater the high key, 
     *     which means the target cell not in the current node. */
    if (boundary_key > high_key && key > high_key) {
        uint32_t next_sibling = SidLeafNodeGetNextSibling(leaf_node);
        Assert(next_sibling != 0);
        SidInsertForLeafNode(table, key, boundary_key, refer, page_num);

        UnlockBuffer(buffer);
        ReleaseBuffer(buffer);
    } else
        SidInsertForLeafNodeInsertCell(table, key, refer, buffer);    
}

/* Sid insert inner new item. */
static void SidInsertInner(Table *table, Sid key, Sid boundary_key, Refer *refer, uint32_t page_num) {
    Buffer buffer;
    void *node;

    buffer = ReadBuffer(table->soid, page_num);
    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    switch (GetNodeType(node)) {
        case LEAF_NODE:
            SidInsertForLeafNode(table, key, boundary_key, refer, page_num);
            break;
        case INTERNAL_NODE:
            SidInsertForInternalNode(table, key, boundary_key, refer, page_num);
            break;
        default:
            UNEXPECTED_VALUE(GetNodeType(node));
            break;
    }

}

/* Sid insert new item. */
void SidInsert(Oid soid, Sid key, Refer *refer) {
    Object obj;
    Table *table;

    Assert(NON_ZERO_SID(key));
    Assert(refer != NULL);

    obj = OidFindObject(soid);
    table = open_table_inner(obj.toid);
    Assert(table->soid == soid);

    SidInsertInner(table, key, SID_ZERO, refer, ROOT_PAGE_NUM);
}
