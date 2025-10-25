#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "data.h"
#include "const.h"
#include "compare.h"
#include "meta.h"
#include "table.h"
#include "refer.h"

/* If obsolute node. */
bool NodeIsObsolute(void *node) {
    if (node == NULL) return false;
    uint8_t value  = *(uint8_t *)(node + NODE_STATE_SIZE_OFFSET);
    return (NodeState) value == OBSOLETE_STATE;
}

/* If dirty node. */
bool NodeIsDirty(void *node) {
    if (node == NULL) return false;
    uint8_t value  = *(uint8_t *)(node + NODE_STATE_SIZE_OFFSET);
    return (NodeState) value == DIRTY_STATE;
}

/* Get node state. */
NodeState GetNodeState(void *node) {
    uint8_t value = *(uint8_t *) (node + NODE_STATE_SIZE_OFFSET);
    return (NodeState) value;
}

/* Set node state. */
void SetNodeState(void *node, NodeState state) {
    uint8_t value = (uint8_t) state;
    *(uint8_t *) (node + NODE_STATE_SIZE_OFFSET) = value;
}

/* Get node type. */
NodeType GetNodeType(void *node) {
    uint8_t value = *(uint8_t *) (node + NODE_TYPE_OFFSET);
    return (NodeType) value;
}

/* Set node type. */
void SetNodeType(void *node, NodeType type) {
    uint8_t value = (uint8_t) type;
    *(uint8_t *) (node + NODE_TYPE_OFFSET) = value;
}

/* If a root node */
bool NodeIsRoot(void *node) {
    uint8_t value = *(uint8_t *) (node + IS_ROOT_OFFSET);
    return (bool) value;
}

/* Set if root node. */
void NodeSetRoot(void *node, bool is_root) {
    uint8_t value = (uint8_t) is_root;
    *(uint8_t *)(node + IS_ROOT_OFFSET) = value;
}

/* Get node parent page num. */
uint32_t NodeGetParentNum(void *node) {
    return *(uint32_t *) (node + PARENT_POINTER_OFFSET);
}

/* Set node parent page num. */
void NodeSetParentNum(void *node, uint32_t parent_num) {
    *(uint32_t *) (node + PARENT_POINTER_OFFSET) = parent_num;
}

/* Get root node column size. */
uint32_t RootNodeGetColumnSize(void *root_node) {
    Assert(NodeIsRoot(root_node));
    return *(uint32_t *) (root_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET);
}

/* Set root node column size. */
void RootNodeSetColumnSize(void *root_node, uint32_t column_size) {
    Assert(NodeIsRoot(root_node));
    *(uint32_t *) (root_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET) = column_size;
}

/* Get root node meta column. */
void *RootNodeGetMetaColumn(void *root_node, uint32_t index) {
    Assert(NodeIsRoot(root_node));
    return root_node + ROOT_NODE_META_COLUMN_OFFSET + ROOT_NODE_META_COLUMN_SIZE * index;
}

/* Set root node meta column. */
void RootNodeSetMetaColumn(void *root_node, uint32_t index, void *destination) {
    Assert(NodeIsRoot(root_node));
    memcpy(root_node + ROOT_NODE_META_COLUMN_OFFSET + ROOT_NODE_META_COLUMN_SIZE * index, 
           destination, ROOT_NODE_META_COLUMN_SIZE);
}

/* Get root node default value. */
void *RootNodeGetDefaultValue(void *root_node) {
    Assert(NodeIsRoot(root_node));
    uint32_t column_size = RootNodeGetColumnSize(root_node);
    return (root_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size);
}

/* Set root node default value. */
void RootNodeSetDefaultValue(void *root_node, uint32_t default_value_len, void *default_value) {
    Assert(NodeIsRoot(root_node));
    uint32_t column_size = RootNodeGetColumnSize(root_node);
    memcpy(root_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
           ROOT_NODE_META_COLUMN_SIZE * column_size, 
           default_value, default_value_len);
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

/* Set internal node keys num. */
void InternalNodeSetKeysNum(void *internal_node, uint32_t default_value_len, uint32_t keys_num) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len) = keys_num;
    } else 
        *(uint32_t *)(internal_node + KEYS_NUM_OFFSET) = keys_num;
}

/* Increase internal node keys num. */
void InternalNodeIncreaseKeysNum(void *internal_node, uint32_t default_value_len) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        (*(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len))++;
    } else 
        (*(uint32_t *)(internal_node + KEYS_NUM_OFFSET))++;
}

/* Get internal node next sibling. */
static uint32_t InternalNodeGetNextSibling(void *internal_node, uint32_t default_value_len) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE);
    } else {
        return *(uint32_t *)(internal_node + KEYS_NUM_OFFSET + KEYS_NUM_SIZE);
    }
}

/* Set internal node next sibling. */
static void InternalNodeSetNextSibling(void *internal_node, uint32_t default_value_len, uint32_t sibling) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE) = sibling;
    } else {
        *(uint32_t *)(internal_node + KEYS_NUM_OFFSET + KEYS_NUM_SIZE) = sibling;
    }
}

/* Get right child key in internal node. */
void *InternalNodeGetRightKey(void *internal_node, uint32_t default_value_len) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return (internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE);
    } else 
        return (internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE);
}

/* Set right child key in internal node. */
void InternalNodeSetRightKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, void *right_key) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        memcpy(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
               ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE, right_key, key_len);
    } else 
        memcpy(internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE, right_key, key_len);
}


/* Get right child page num in internal node. */
uint32_t InternalNodeGetRightNum(void *internal_node, uint32_t default_value_len) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE);
    } else {
        return *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET);
    }
}

/* Set right child page num in internal node. */
void InternalNodeSetRightNum(void *internal_node, uint32_t default_value_len, uint32_t right_num) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE) = right_num;
    } else {
        *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET) = right_num;
    }
}

/* Get internal node cell key. */
void *InternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return (internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE +
                RIGHT_CHILD_SIZE + key_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE); 
    } else {
        uint32_t internal_head_len = COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len;
        return (internal_node + internal_head_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE);
    }
}

/* Set internal node cell key. */
void InternalNodeSetCellKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index, void *cell_key) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        memcpy(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len +
                cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE, cell_key, key_len);
    } else {
        uint32_t internal_head_len = COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len;
        memcpy(internal_node + internal_head_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE, cell_key, key_len);
    }
}

/* Get internal node cell num. */
uint32_t InternalNodeGetCellValue(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len +
            cell_len * index); 
    } else {
        uint32_t internal_head_len = COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len;
        return *(uint32_t *)(internal_node + internal_head_len + cell_len * index);
    }
}

/* Set internal node cell num. */
void InternalNodeSetCellValue(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index, uint32_t page_num) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = RootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len +
            cell_len * index) = page_num; 
    } else {
        uint32_t internal_head_len = COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len;
        *(uint32_t *)(internal_node + internal_head_len + cell_len * index) = page_num;
    }
}

/* Initialize leaf node. */
void InternalNodeInitialize(void *internal_node, uint32_t default_value_len, bool is_root) {
    SetNodeType(internal_node, INTERNAL_NODE);
    NodeSetRoot(internal_node, is_root);
    InternalNodeSetKeysNum(internal_node, default_value_len, 0);
    InternalNodeSetNextSibling(internal_node, default_value_len, 0); 
}

/* Find the internal node cell num postion. 
 * -----------------------------------------
 * In this function, we will use binary search to find the target cell.
 * */
uint32_t InternalNodeFindCellNum(Oid oid, void *key, void *internal_node) {
    Table *table;
    DataType ptype;
    uint32_t keys_num, min_index, max_index; 
    
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
    
    return min_index;
}

/* Get leaf node cell num. */
uint32_t LeafNodeGetCellNum(void *leaf_node, uint32_t default_value_len) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len);
    } else 
        return *(uint32_t *)(leaf_node + CELL_NUM_OFFSET);
}

/* Set leaf node cell num. */
void LeafNodeSetCellNum(void *leaf_node, uint32_t default_value_len, uint32_t cell_num) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        *(uint32_t *)(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len) = cell_num;
    } else {
        *(uint32_t *)(leaf_node + CELL_NUM_OFFSET) = cell_num;
    }
}

/* Increase leaf node cell num. */
void LeafNodeIncreaseCellNum(void *leaf_node, uint32_t default_value_len) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        (*(uint32_t *)(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len))++;
    } else {
        (*(uint32_t *)(leaf_node + CELL_NUM_OFFSET))++;
    }
}

/* Get leaf node next sibling. */
static uint32_t LeafNodeGetNextSibling(void *leaf_node, uint32_t default_value_len) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE);
    } else 
        return *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET); 
}

/* Set leaf node next sibling. */
static void LeafNodeSetNextSibling(void *leaf_node, uint32_t default_value_len, uint32_t sibling) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        *(uint32_t *)(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE) = sibling;
    } else 
        *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET) = sibling;
}

/* Get leaf node cell key. */
void *LeafNodeGetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return (leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE + 
                LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index + value_len); 
    } else
        return (leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index + value_len);
}

/* Set leaf node cell key. */
void LeafNodeSetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index, void *cell_key) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        memcpy(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
               ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE + 
               LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index + value_len, 
               cell_key, key_len);
    } else 
        memcpy(leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index + value_len, cell_key, key_len);
}

/* Get leaf node cell value. */
void *LeafNodeGetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        return (leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE + 
            LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index); 
    } else {
        return (leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index);
    }
}

/* Set leaf node cell value. */
void LeafNodeSetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index, void *cell_value) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = RootNodeGetColumnSize(leaf_node);
        memcpy(leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
               ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE + 
               LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index, 
               cell_value, value_len); 
    } else {
        memcpy(leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index, cell_value, value_len);
    }
}

/* Get created xid. */
Xid LeafNodeGetCellCreatedXid(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index) {
    void *cell_value = LeafNodeGetCellValue(leaf_node, key_len, value_len, default_value_len, index);
    return *(Xid *) (cell_value + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Get created xid. */
Xid LeafNodeGetCellExpiredXid(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index) {
    void *cell_value = LeafNodeGetCellValue(leaf_node, key_len, value_len, default_value_len, index);
    return *(Xid *) (cell_value + REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t) + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Initialize leaf node. */
void LeafNodeInitialize(void *leaf_node, uint32_t default_value_len, bool is_root) {
    SetNodeType(leaf_node, LEAF_NODE);
    NodeSetRoot(leaf_node, is_root);
    LeafNodeSetCellNum(leaf_node, default_value_len, 0);
    LeafNodeSetNextSibling(leaf_node, default_value_len, 0); 
}


/* Find the leaf node cell num postion. 
 * -----------------------------------------
 * In this function, we will use binary search to find the target cell.
 * */
 uint32_t LeafNodeFindCellNum(Oid oid, void *key, void *leaf_node) {
    Table *table;
    DataType ptype;
    uint32_t cell_num, min_index, max_index;
    
    table = open_table_inner(oid);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    cell_num = LeafNodeGetCellNum(leaf_node, table->heap_value_len);
    min_index = 0;
    max_index = cell_num;

    while (min_index != max_index) {
        uint32_t index;
        void *cell_key;

        index = (max_index + min_index) / 2;
        cell_key = LeafNodeGetCellKey(leaf_node, table->key_len, table->index_value_len, table->heap_value_len, index);
        /* Notice: Not only greater but aslo equal opreator is really import for store data, 
         * when keep the prince: always keep visible row lying at the forefront of same key cells. */
        if (GE(GetComparableValue(cell_key, ptype), GetComparableValue(key, ptype), ptype)) {
            max_index = index;
        } else {
            min_index = index + 1; 
        }
    }

    return min_index;
}


/* Get high key in the node. */
void *NodeGetHighKey(Table *table, void *node) {
    switch (GetNodeType(node)) {
        case INTERNAL_NODE:
            return InternalNodeGetRightKey(node, table->heap_value_len);
        case LEAF_NODE: {
            uint32_t cell_num = LeafNodeGetCellNum(node, table->heap_value_len);
            return cell_num == 0 
                ? NULL 
                : LeafNodeGetCellKey(node, table->key_len, table->index_value_len, table->heap_value_len, cell_num - 1);
        }
        default:
            UNEXPECTED_VALUE(GetNodeType(node));
            return NULL;
    }
}

/* Get next sibling page in the node. */
uint32_t NodeGetNextSibling(Table *table, void *node) {
    NodeType type = GetNodeType(node);
    switch (type) {
        case INTERNAL_NODE:
            return InternalNodeGetNextSibling(node, table->heap_value_len);
        case LEAF_NODE: 
            return LeafNodeGetNextSibling(node, table->heap_value_len);
        default:
            UNEXPECTED_VALUE(type);
            return 0;
    }
}

/* Set next sibling page in the node. */
void NodeSetNextSibling(Table *table, void *node, uint32_t sibling) {
    NodeType type = GetNodeType(node);
    switch (type) {
        case INTERNAL_NODE:
            InternalNodeSetNextSibling(node, table->heap_value_len, sibling);
            break;
        case LEAF_NODE: 
            LeafNodeSetNextSibling(node, table->heap_value_len, sibling);
            break;
        default:
            UNEXPECTED_VALUE(type);
    }
}

