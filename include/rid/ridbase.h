#include <stdbool.h>
#include "refer.h"

#define RID_KEY_LENGTH                      sizeof(Rid)
#define RID_LEAF_NODE_VALUE_LENGTH          REFER_SIZE
#define RID_LEAF_NODE_CELL_LENGTH           (RID_KEY_LENGTH + RID_LEAF_NODE_VALUE_LENGTH)
#define RID_INTERNAL_NODE_VALUE_LENGTH      INTERNAL_NODE_CELL_CHILD_SIZE 
#define RID_INTERNAL_NODE_CELL_LENGTH       (RID_KEY_LENGTH + RID_INTERNAL_NODE_VALUE_LENGTH)


/* Get rid internal node cell num. */
uint32_t RidInternalNodeGetKeysNum(void *internal_node);

/* Set rid internal node cell num. */
void RidInternalNodeSetKeysNum(void *internal_node, uint32_t keys_num);

/* Set rid internal node cell num. */
void RidInternalNodeIncreaseKeysNum(void *internal_node);

/* Get rid internal node next sibling. */
uint32_t RidInternalNodeGetNextSibling(void *internal_node);

/* Set rid internal node next sibling. */
void RidInternalNodeSetNextSibling(void *internal_node, uint32_t sibling);

/* Get rid internal node right key. */
Rid RidInternalNodeGetRightKey(void *internal_node);

/* Set rid internal node right key. */
void RidInternalNodeSetRightKey(void *internal_node, Rid key);

/* Get rid internal node right page num. */
void RidInternalNodeSetRightNum(void *internal_node, uint32_t right_num);

/* Get rid internal node right page num. */
uint32_t RidInternalNodeGetRightNum(void *internal_node);

/* Get rid internal node cell key. */
Rid RidInternalNodeGetCellKey(void *internal_node, uint32_t index);

/* Set rid internal node cell key. */
void RidInternalNodeSetCellKey(void *internal_node, uint32_t index, Rid key);

/* Get rid internal node cell value. */
uint32_t RidInternalNodeGetCellValue(void *internal_node, uint32_t index);

/* Get rid internal node cell value. */
void RidInternalNodeSetCellValue(void *internal_node, uint32_t index, uint32_t cell_value);

/* Initialize rid internal node. */
void RidInternalNodeInitialize(void *internal_node, bool is_root);

/* Find the internal node cell postion. */
uint32_t RidInternalNodeFindCellNum(void *internal_node, Rid key);

/* Get rid leaf node sibling. */
uint32_t RidLeafNodeGetNextSibling(void *leaf_node);

/* Set rid leaf node sibling. */
void RidLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling);

/* Get rid leaf node cell num. */
uint32_t RidLeafNodeGetCellNum(void *leaf_node);

/* Set rid leaf node cell num. */
void RidLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num);

/* Increase rid leaf node cell num. */
void RidLeafNodeIncreaseCellNum(void *leaf_node);

/* Get rid leaf node cell key. */
Rid RidLeafNodeGetCellKey(void *leaf_node, uint32_t index);

/* Set rid leaf node cell key. */
void RidLeafNodeSetCellKey(void *leaf_node, uint32_t index, Rid key);

/* Set rid leaf node cell value. */
void RidLeafNodeSetCellValue(void *leaf_node, uint32_t index, Refer *refer);

/* Get rid leaf node cell value. */
Refer *RidLeafNodeGetCellValue(void *leaf_node, uint32_t index);

/* Rid leaf node initialize. */
void RidLeafNodeInitialize(void *leaf_node, bool is_root);

/* Find the leaf node cell postion. */
uint32_t RidLeafNodeFindCellNum(void *leaf_node, Rid key);

/* Rid node get high key. */
Rid RidNodeGetHighKey(void *node);
