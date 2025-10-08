#include <stdbool.h>
#include <stdint.h>
#include "data.h"
#include "const.h"
#include "compare.h"
#include "meta.h"

/* Get node type. */
inline NodeType GetNodeType(void *node) {
    uint8_t value = *(uint8_t *) (node + NODE_TYPE_OFFSET);
    return (NodeType) value;
}

/* If a root node */
inline bool NodeIsRoot(void *node) {
    uint8_t value = *(uint8_t *) (node + IS_ROOT_OFFSET);
    return (bool) value;
}

/* Get root node column size. */
uint32_t RootNodeGetColumnSize(void *root_node) {
    return *(uint32_t *) (root_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET);
}

/* Get internal node keys num. */
uint32_t InternalNodeGetKeysNum(void *internal_node, uint32_t default_value_len) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len);
    } else 
        return *(uint32_t *)(internal_node + KEYS_NUM_OFFSET);
}

/* Get right child key in internal node. */
void *InternalNodeGetRightKey(void *internal_node, uint32_t default_value_len) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return (internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE);
    } else 
        return (internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE);
}

/* Get internal node cell key. */
void *InternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return (internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + 
                RIGHT_CHILD_SIZE + key_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE); 
    } else {
        uint32_t internal_head_len = COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + key_len;
        return (internal_node + internal_head_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE);
    }
}

/* Get keaf node cell num. */
uint32_t LeafNodeGetCellNum(void *leaf_node, uint32_t default_value_len) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len);
    } else {
        return *(uint32_t *)(leaf_node + CELL_NUM_OFFSET);
    }
}

/* Get leaf node next sibling. */
uint32_t LeafNodeGetNext(void *leaf_node, uint32_t default_value_len) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE);
    } else 
        return *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_LEAF_OFFSET); 
}

/* Get leaf node cell key. */
void *LeafNodeGetCellKey(void *leaf_node, uint32_t index, uint32_t key_len, uint32_t value_len, uint32_t default_value_len) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return (leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE + 
                LEAF_NODE_NEXT_LEAF_SIZE + cell_len * index + value_len); 
    } else
        return (leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index + value_len);
}


/* Get high key in the node. */
void *NodeGetHighKey(Table *table, void *node) {
    NodeType type = GetNodeType(node);
    switch (type) {
        case INTERNAL_NODE:
            return InternalNodeGetRightKey(node, table->heap_value_len);
        case LEAF_NODE: {
            uint32_t cell_num = LeafNodeGetCellNum(node, table->heap_value_len);
            return LeafNodeGetCellKey(node, cell_num - 1, table->key_len, table->index_value_len, table->heap_value_len);
        }
        default:
            UNEXPECTED_VALUE(type);
            return NULL;
    }
}


/* Get next page in the node. */
uint32_t NodeFindNextPage(void *node) {

}

/* If node has spliten. */
bool NodeHasSplit(void *search_key, void *high_key, DataType ptype) {
    return GT(GetComparableValue(search_key, ptype), GetComparableValue(high_key, ptype), ptype);
}

