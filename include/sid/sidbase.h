#include <stdbool.h>
#include <stdint.h>
#include "data.h"
#include "refer.h"

#define SID_KEY_LENGTH                      sizeof(Sid)
#define SID_LEAF_NODE_VALUE_LENGTH          REFER_SIZE
#define SID_LEAF_NODE_CELL_LENGTH           (SID_KEY_LENGTH + SID_LEAF_NODE_VALUE_LENGTH)
#define SID_INTERNAL_NODE_VALUE_LENGTH      INTERNAL_NODE_CELL_CHILD_SIZE 
#define SID_INTERNAL_NODE_CELL_LENGTH       (SID_KEY_LENGTH + SID_INTERNAL_NODE_VALUE_LENGTH)


/* Get sid internal node cell num. */
uint32_t SidInternalNodeGetKeysNum(void *internal_node);

/* Set sid internal node cell num. */
void SidInternalNodeSetKeysNum(void *internal_node, uint32_t keys_num);

/* Set sid internal node cell num. */
void SidInternalNodeIncreaseKeysNum(void *internal_node);

/* Get sid internal node next sibling. */
uint32_t SidInternalNodeGetNextSibling(void *internal_node);

/* Set sid internal node next sibling. */
void SidInternalNodeSetNextSibling(void *internal_node, uint32_t sibling);

/* Get sid internal node right key. */
Sid SidInternalNodeGetRightKey(void *internal_node);

/* Set sid internal node right key. */
void SidInternalNodeSetRightKey(void *internal_node, Sid key);

/* Get sid internal node right page num. */
void SidInternalNodeSetRightNum(void *internal_node, uint32_t right_num);

/* Get sid internal node right page num. */
uint32_t SidInternalNodeGetRightNum(void *internal_node);

/* Get sid internal node cell key. */
Sid SidInternalNodeGetCellKey(void *internal_node, uint32_t index);

/* Set sid internal node cell key. */
void SidInternalNodeSetCellKey(void *internal_node, uint32_t index, Sid key);

/* Get sid internal node cell value. */
uint32_t SidInternalNodeGetCellValue(void *internal_node, uint32_t index);

/* Get sid internal node cell value. */
void SidInternalNodeSetCellValue(void *internal_node, uint32_t index, uint32_t cell_value);

/* Initialize sid internal node. */
void SidInternalNodeInitialize(void *internal_node, bool is_root);

/* Find the internal node cell postion. */
uint32_t SidInternalNodeFindCellNum(void *internal_node, Sid key);

/* Get sid leaf node sibling. */
uint32_t SidLeafNodeGetNextSibling(void *leaf_node);

/* Set sid leaf node sibling. */
void SidLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling);

/* Get sid leaf node cell num. */
uint32_t SidLeafNodeGetCellNum(void *leaf_node);

/* Set sid leaf node cell num. */
void SidLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num);

/* Increase sid leaf node cell num. */
void SidLeafNodeIncreaseCellNum(void *leaf_node);

/* Get sid leaf node cell key. */
Sid SidLeafNodeGetCellKey(void *leaf_node, uint32_t index);

/* Set sid leaf node cell key. */
void SidLeafNodeSetCellKey(void *leaf_node, uint32_t index, Sid key);

/* Set sid leaf node cell value. */
void SidLeafNodeSetCellValue(void *leaf_node, uint32_t index, Refer *refer);

/* Get sid leaf node cell value. */
Refer *SidLeafNodeGetCellValue(void *leaf_node, uint32_t index);

/* Sid leaf node initialize. */
void SidLeafNodeInitialize(void *leaf_node, bool is_root);

/* Find the leaf node cell postion. */
uint32_t SidLeafNodeFindCellNum(void *leaf_node, Sid key);

/* Sid node get high key. */
Sid SidNodeGetHighKey(void *node);
