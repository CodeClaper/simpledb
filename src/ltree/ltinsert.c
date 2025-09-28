#include <stdint.h>
#include "ltinsert.h"
#include "table.h"
#include "const.h"
#include "bufmgr.h"

static NodeType GetNodeType(void *node) {
    uint8_t value = *(uint8_t *) (node + NODE_TYPE_OFFSET);
    return (NodeType) value;
}

/* Get high key in the node. */
static void *GetHighKey(Table *table, void *node) {
    
}


/* Insert item into the btree for internal node. */
static void BtreeInsertForInternalNode(Oid oid, void *key, void *high_key, uint32_t page_num) {
    
}

/* Insert item into the btree for leaf node. */
static void BtreeInsertForLeafNode(Oid oid, void *key, void *high_key, uint32_t page_num) {
    Table *table;
    Buffer buffer;
    void *leaf_node;
    uint32_t cell_num, value_len, key_len, default_value_len;

    table = open_table_inner(oid);
    buffer = ReadBuffer(oid, page_num);
    LockBuffer(buffer, RW_WRITER);
    leaf_node = GetBufferPage(buffer);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Insert item into the btree. */
void BtreeInsert(Oid oid, void *key) {
    Table *table;
    Buffer root_buffer;
    void *root_node, *high_key;
    NodeType type;

    table = open_table_inner(oid);
    root_buffer = ReadBuffer(oid, ROOT_PAGE_NUM);
    LockBuffer(root_buffer, RW_READERS);
    root_node = GetBufferPageCopy(root_buffer);
    UnlockBuffer(root_buffer);
    ReleaseBuffer(root_buffer);
    
    high_key = GetHighKey(table, root_node);
    type = GetNodeType(root_node);
    switch (type) {
        case LEAF_NODE:
            BtreeInsertForLeafNode(oid, key, high_key, ROOT_PAGE_NUM);
            break;
        case INTERNAL_NODE:
            BtreeInsertForInternalNode(oid, key, high_key, ROOT_PAGE_NUM);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}
