#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "bininsert.h"
#include "bin.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "log.h"
#include "trans.h"
#include "heaptable.h"
#include "table.h"
#include "tuple.h"

#define OK   1
#define WAIT 0
#define ERRO -1

static void BinInsertInner(MetaIndex *meta_index, void *key, void *boundary_key, Refer *value, uint32_t page_num);

/* Predicate duplicate key.
 * These are three duplicate key type.
 * (1) 1: A deleted duplicate key which does not need to care about.
 * (2) 0: An un-commited duplicate key which need to wait for commit.
 * (3) -1: A commited duplicate key which case duplicate key issue. 
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
    if (BinCompareKey(meta_index, old_key, high_key) < 0) {
        uint32_t index;
        void *cell_key;

    } else if (BinCompareKey(meta_index, old_key, high_key) == 0) {
    } else {
    }

    MakeBufferDirty(buffer);
    
    /* Update current internal node parent. */
    if (!NodeIsRoot(internal_node) &&
        BinCompareKey(meta_index, new_key, high_key)
    ) {
        uint32_t parent_num;
        parent_num = NodeGetParentNum(internal_node);
        BinInsertForInternalNodeUpdateCellKey(meta_index, parent_num, old_key, new_key);
    }

DirectExit:
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

/* Bin insert into a cell and split leaf node. */
static void BinInsertForLeafNodeSplit(MetaIndex *meta_index, void *key, void *value, Buffer buffer) {
    Buffer new_buffer;
    uint32_t cell_num, target_index, next_page_num;
    void *leaf_node, *high_key, *cell_key, *new_leaf_node;

    leaf_node = GetBufferPage(buffer);
    cell_num = BinLeafNodeGetCellNum(leaf_node);
    target_index = BinLeafNodeFindCellNum(meta_index, leaf_node, key);
    cell_key = BinLeafNodeGetCellKey(leaf_node, meta_index->key_len, meta_index->value_len, target_index);

    /* Avoid duplicate key, two conditions:
     * (1) The index is unique.
     * (2) Duplicate key already exists. */
    if (meta_index->is_unique && BinCompareKey(meta_index, key, cell_key) == 0) {
        Table *table;
        Refer *refer;
        void *tuple;
        uint32_t predicate;
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
    if (meta_index->is_unique && BinCompareKey(meta_index, key, cell_key) == 0) {
        Table *table;
        Refer *refer;
        void *tuple;
        uint32_t predicate;
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
