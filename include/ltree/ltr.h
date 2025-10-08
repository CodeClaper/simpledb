#include "data.h"
#include <stdint.h>

/* Get node type. */
NodeType GetNodeType(void *node);

/* Get high key in the node. */
void *NodeGetHighKey(Table *table, void *node);

/* Get next page in the node. */
uint32_t NodeFindNextPage(void *node);

/* If node has spliten. */
bool NodeHasSplit(void *search_key, void *high_key, DataType ptype);

/* Get internal node keys num. */
uint32_t InternalNodeGetKeysNum(void *internal_node, uint32_t default_value_len);

/* Get right child key in internal node. */
void *InternalNodeGetRightKey(void *internal_node, uint32_t default_value_len);

/* Get internal node cell key. */
void *InternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t default_value_len, uint32_t index);

/* Get leaf node next sibling. */
uint32_t LeafNodeGetNext(void *leaf_node, uint32_t default_value_len);
