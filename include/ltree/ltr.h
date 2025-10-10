#include <stdint.h>
#include "data.h"

/* Get node type. */
NodeType GetNodeType(void *node);

/* Set node type. */
void SetNodeType(void *node, NodeType type);

/* If a root node */
bool NodeIsRoot(void *node);

/* Get node parent page num. */
uint32_t NodeGetParentNum(void *node);

/* Set node parent page num. */
void NodeSetParentNum(void *node, uint32_t parent_num);

/* Get root node column size. */
uint32_t RootNodeGetColumnSize(void *root_node);

/* Get high key in the node. */
void *NodeGetHighKey(Table *table, void *node);

/* Get next sibling page in the node. */
uint32_t NodeGetNextSibling(Table *table, void *node);

/* Set next sibling page in the node. */
void NodeSetNextSibling(Table *table, void *node, uint32_t sibling);

/* If node has spliten. */
bool NodeHasSplit(void *search_key, void *high_key, DataType ptype);

/* Get internal node keys num. */
uint32_t InternalNodeGetKeysNum(void *internal_node, uint32_t default_value_len);

/* Set internal node keys num. */
void InternalNodeSetKeysNum(void *internal_node, uint32_t default_value_len, uint32_t keys_num);

/* Increase internal node keys num. */
void InternalNodeIncreaseKeysNum(void *internal_node, uint32_t default_value_len);

/* Get right child key in internal node. */
void *InternalNodeGetRightKey(void *internal_node, uint32_t default_value_len);

/* Set right child key in internal node. */
void InternalNodeSetRightKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, void *right_key);

/* Get right child cell value in internal node. */
uint32_t InternalNodeGetRightNum(void *internal_node, uint32_t default_value_len);

/* Set right child page num in internal node. */
void InternalNodeSetRightNum(void *internal_node, uint32_t default_value_len, uint32_t right_num);

/* Get internal node cell key. */
void *InternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index);

/* Set internal node cell key. */
void InternalNodeSetCellKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index, void *cell_key);

/* Get internal node cell num. */
uint32_t InternalNodeGetCellValue(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index);

/* Set internal node cell num. */
void InternalNodeSetCellValue(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index, uint32_t page_num);

/* Initialize leaf node. */
void InternalNodeInitialize(void *internal_node, uint32_t default_value_len, bool is_root);

/* Get leaf node cell num. */
uint32_t LeafNodeGetCellNum(void *leaf_node, uint32_t default_value_len);

/* Set leaf node cell num. */
void LeafNodeSetCellNum(void *leaf_node, uint32_t default_value_len, uint32_t cell_num);

/* Increase leaf node cell num. */
void LeafNodeIncreaseCellNum(void *leaf_node, uint32_t default_value_len);

/* Get leaf node cell key. */
void *LeafNodeGetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index);

/* Set leaf node cell key. */
void LeafNodeSetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index, void *cell_key);

/* Get leaf node cell value. */
void *LeafNodeGetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index);

/* Set leaf node cell value. */
void LeafNodeSetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t index, void *cell_value);

/* Initialize leaf node. */
void LeafNodeInitialize(void *leaf_node, uint32_t default_value_len, bool is_root);
