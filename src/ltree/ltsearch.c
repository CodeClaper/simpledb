#include <stdint.h>
#include "ltsearch.h"
#include "ltbase.h"
#include "refer.h"
#include "bufmgr.h"
#include "table.h"
#include "meta.h"
#include "compare.h"
#include "instance.h"
#include "mmgr.h"
#include "log.h"
#include "copy.h"

static void BtreeSearchReferInner(Oid oid, void *key, void *boundary_key, uint32_t page_num, Refer *refer);
static void *BtreeSearchValueInner(Oid oid, void *key, void *boundary_key, uint32_t page_num);

static void BtreeSearchReferForInternalNodeExtend(Oid oid, void *key, void *internal_node, Refer *refer) {
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
               min_index, keys_num);
    else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = InternalNodeGetRightKey(internal_node, table->heap_value_len);
        target_page = InternalNodeGetRightNum(internal_node, table->heap_value_len);
        BtreeSearchReferInner(oid, key, boundary_key, target_page, refer);
    } else {
        /* The target cell in cells. */
        boundary_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, min_index);
        target_page = InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, min_index);
        BtreeSearchReferInner(oid, key, boundary_key, target_page, refer);
    }
}

static void BtreeSearchReferForInternalNode(Oid oid, void *key, void *boundary_key, uint32_t page_num, Refer *refer) {
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
        BtreeSearchReferForInternalNode(oid, key, boundary_key, next_sibling, refer);
    } else
        BtreeSearchReferForInternalNodeExtend(oid, key, internal_node, refer);    

    dfree(internal_node);
}

static void BtreeSearchReferForLeafNode(Oid oid, void *key, void *boundary_key, uint32_t page_num, Refer *refer) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *leaf_node, *high_key;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
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
        BtreeSearchReferForLeafNode(oid, key, boundary_key, next_sibling, refer);
    } else {
        refer->page_num = page_num;
        refer->cell_num = LeafNodeFindCellNum(oid, key, leaf_node);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Btree search for refer. */
static void BtreeSearchReferInner(Oid oid, void *key, void *boundary_key, uint32_t page_num, Refer *refer) {
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
            BtreeSearchReferForLeafNode(oid, key, boundary_key, page_num, refer);
            break;
        case INTERNAL_NODE:
            BtreeSearchReferForInternalNode(oid, key, boundary_key, page_num, refer);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}


/* Btree search the key refer. */
Refer *BtreeSearchRefer(Oid oid, void *key) {
    Assert(key != NULL);
    Refer *refer = new_refer(oid, -1, -1);
    BtreeSearchReferInner(oid, key, NULL, ROOT_PAGE_NUM, refer);   
    return refer;
}

static void *BtreeSearchValueForInternalNodeExtend(Oid oid, void *key, void *internal_node) {
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

    if (min_index > keys_num) {
        db_log(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, 
               keys_num);
        return NULL;
    } else if (min_index == keys_num) {
        /* The target cell is right child. */
        boundary_key = InternalNodeGetRightKey(internal_node, table->heap_value_len);
        target_page = InternalNodeGetRightNum(internal_node, table->heap_value_len);
        return BtreeSearchValueInner(oid, key, boundary_key, target_page);
    } else {
        /* The target cell in cells. */
        boundary_key = InternalNodeGetCellKey(internal_node, table->key_len, table->heap_value_len, min_index);
        target_page = InternalNodeGetCellValue(internal_node, table->key_len, table->heap_value_len, min_index);
        return BtreeSearchValueInner(oid, key, boundary_key, target_page);
    }
}

/* Btree search for internal node. */
static void *BtreeSearchValueForInternalNode(Oid oid, void *key, void *boundary_key, uint32_t page_num) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *internal_node, *high_key, *value;

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
        value = BtreeSearchValueForInternalNode(oid, key, boundary_key, next_sibling);
    } else
        value = BtreeSearchValueForInternalNodeExtend(oid, key, internal_node);    

    dfree(internal_node);

    return value;
}

/* Btree search leaf cell value. */
static void *BtreeSearchValueForLeafNodeExtend(Oid oid, void *key, void *leaf_node) {
    Table *table;
    uint32_t target_index;
    void *value;

    table = open_table_inner(oid);
    target_index = LeafNodeFindCellNum(oid, key, leaf_node);
    value = LeafNodeGetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, target_index);

    return copy_block(value, table->index_value_len);
}

/* Btree search for leaf node. */
static void *BtreeSearchValueForLeafNode(Oid oid, void *key, void *boundary_key, uint32_t page_num) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *leaf_node, *high_key, *value;

    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_READERS);
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
        value = BtreeSearchValueForLeafNode(oid, key, boundary_key, next_sibling);
    } else {
        value = BtreeSearchValueForLeafNodeExtend(oid, key, leaf_node);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return value;
}

static void *BtreeSearchValueInner(Oid oid, void *key, void *boundary_key, uint32_t page_num) {
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
            return BtreeSearchValueForLeafNode(oid, key, boundary_key, page_num);
        case INTERNAL_NODE:
            return BtreeSearchValueForInternalNode(oid, key, boundary_key, page_num);
        default:
            UNEXPECTED_VALUE(type);
            return NULL;
    }
}

/* Btree search for value. 
 * ----------------------
 * This function will seach btree and return value which matches the key.
 * Notice, the return value is a duplica, it`s nessary keep value unchanged in concurrency.
 * */
void *BtreeSearchValue(Oid oid, void *key) {
    Assert(key != NULL);
    return BtreeSearchValueInner(oid, key, NULL, ROOT_PAGE_NUM);   
}

/* Btree seach key via refer.*/
void *BtreeSearchKeyViaRefer(Refer *refer) {
    Oid oid;
    Table *table;
    Buffer buffer;
    void *leaf_node, *key;

    oid = refer->oid;
    table = open_table_inner(oid);

    buffer = ReadBuffer(oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);

    key = LeafNodeGetCellKey(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, refer->cell_num);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return key;
}


/* Btree seach value via refer.*/
void *BtreeSearchValueViaRefer(Refer *refer) {
    Oid oid;
    Table *table;
    Buffer buffer;
    void *leaf_node, *value;

    oid = refer->oid;
    table = open_table_inner(oid);

    buffer = ReadBuffer(oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);

    value = LeafNodeGetCellValue(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, refer->cell_num);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return value;
}

/* Btree search key vai ref id. */
void *BtreeSearchKeyViaRefId(Oid oid, Rid ref_id) {
    return NULL;
}

/* Btree search value via ref id. */
void *BtreeSearchValueViaRefId(Oid oid, Rid ref_id) {
    return NULL;
}

