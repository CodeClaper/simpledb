#include <stdint.h>
#include "ltinsert.h"
#include "ltr.h"
#include "table.h"
#include "const.h"
#include "meta.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "compare.h"


/* Insert item into the btree for internal node. */
static void BtreeInsertForInternalNode(Oid oid, void *key, void *search_key, uint32_t page_num) {
    Table *table;
    DataType ptype;
    Buffer buffer;
    void *internal_node, *high_key;
    uint32_t keys_num, i, next_page;

    table = open_table_inner(oid);

    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    internal_node = GetBufferPageCopy(buffer);
    high_key = NodeGetHighKey(table, internal_node);
    keys_num = InternalNodeGetKeysNum(internal_node, table->heap_value_len);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    if (NodeHasSplit(search_key, high_key, ptype) &&
        GT(GetComparableValue(key, ptype), GetComparableValue(high_key, ptype), ptype)
    ) {
        //
    }

    dfree(internal_node);
}

/* Insert item into the btree for leaf node. */
static void BtreeInsertForLeafNode(Oid oid, void *key, void *search_key, uint32_t page_num) {
    Table *table;
    MetaColumn *priamry_meta_coumn;
    Buffer buffer;
    void *leaf_node, *high_key;
    uint32_t cell_num, value_len, key_len, default_value_len;

    table = open_table_inner(oid);
    priamry_meta_coumn = MetaTableFindPrimaryKey(table->meta_table);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    leaf_node = GetBufferPage(buffer);
    high_key = NodeGetHighKey(table, leaf_node);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert item into the btree. */
void BtreeInsert(Oid oid, void *key, void *value) {
    Table *table;
    Buffer root_buffer;
    void *root_node, *search_key;
    NodeType type;

    table = open_table_inner(oid);
    root_buffer = ReadBuffer(oid, ROOT_PAGE_NUM);
    LockBuffer(root_buffer, RW_READERS);
    root_node = GetBufferPageCopy(root_buffer);
    UnlockBuffer(root_buffer);
    ReleaseBuffer(root_buffer);
    
    search_key = NodeGetHighKey(table, root_node);
    type = GetNodeType(root_node);
    switch (type) {
        case LEAF_NODE:
            BtreeInsertForLeafNode(oid, key, search_key, ROOT_PAGE_NUM);
            break;
        case INTERNAL_NODE:
            BtreeInsertForInternalNode(oid, key, search_key, ROOT_PAGE_NUM);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}
