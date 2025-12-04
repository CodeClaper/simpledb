#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "sidbase.h"
#include "const.h"
#include "ltbase.h"

/* Get rid internal node cell num. */
uint32_t SidInternalNodeGetKeysNum(void *internal_node) {
    return *(uint32_t *)(internal_node + KEYS_NUM_OFFSET);
}

/* Set rid internal node cell num. */
void SidInternalNodeSetKeysNum(void *internal_node, uint32_t keys_num) {
    *(uint32_t *)(internal_node + KEYS_NUM_OFFSET) = keys_num;
}

/* Set rid internal node cell num. */
void SidInternalNodeIncreaseKeysNum(void *internal_node) {
    (*(uint32_t *)(internal_node + KEYS_NUM_OFFSET))++;
}

/* Get rid internal node next sibling. */
uint32_t SidInternalNodeGetNextSibling(void *internal_node) {
    return *(uint32_t *)(internal_node + INTERNAL_NODE_NEXT_SIBLING_OFFSET);
}

/* Set rid internal node next sibling. */
void SidInternalNodeSetNextSibling(void *internal_node, uint32_t sibling) {
    *(uint32_t *)(internal_node + INTERNAL_NODE_NEXT_SIBLING_OFFSET) = sibling;
}

/* Get rid internal node right key. */
Sid SidInternalNodeGetRightKey(void *internal_node) {
    return *(Sid *)(internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE);
}

/* Set rid internal node right key. */
void SidInternalNodeSetRightKey(void *internal_node, Sid key) {
    *(Sid *)(internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE) = key;
}

/* Get rid internal node right page num. */
uint32_t SidInternalNodeGetRightNum(void *internal_node) { 
    return *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET);
}

/* Get rid internal node right page num. */
void SidInternalNodeSetRightNum(void *internal_node, uint32_t right_num) { 
     *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET) = right_num;
}

/* Get rid internal node cell key. */
Sid SidInternalNodeGetCellKey(void *internal_node, uint32_t index) {
    return *(Sid *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + \
            RIGHT_CHILD_SIZE + SID_KEY_LENGTH + SID_INTERNAL_NODE_CELL_LENGTH * index + SID_INTERNAL_NODE_VALUE_LENGTH);
}

/* Set rid internal node cell key. */
void SidInternalNodeSetCellKey(void *internal_node, uint32_t index, Sid key) {
    *(Sid *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + \
            RIGHT_CHILD_SIZE + SID_KEY_LENGTH + SID_INTERNAL_NODE_CELL_LENGTH * index + SID_INTERNAL_NODE_VALUE_LENGTH) = key;
}

/* Get rid internal node cell value. */
uint32_t SidInternalNodeGetCellValue(void *internal_node, uint32_t index) {
    return *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + \
                         RIGHT_CHILD_SIZE + SID_KEY_LENGTH + SID_INTERNAL_NODE_CELL_LENGTH * index);
}

/* Get rid internal node cell value. */
void SidInternalNodeSetCellValue(void *internal_node, uint32_t index, uint32_t cell_value) {
    *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + \
                  SID_KEY_LENGTH + SID_INTERNAL_NODE_CELL_LENGTH * index) = cell_value;
}

/* Initialize rid internal node. */
void SidInternalNodeInitialize(void *internal_node, bool is_root) {
    SetNodeType(internal_node, INTERNAL_NODE);
    NodeSetRoot(internal_node, is_root);
    SidInternalNodeSetKeysNum(internal_node, 0);
    SidInternalNodeSetNextSibling(internal_node, 0);
}

/* Find the internal node cell postion.
 * --------------------------------
 * We will use binary search to find the target cell postion.
 * */
uint32_t SidInternalNodeFindCellNum(void *internal_node, Sid key) {
    uint32_t keys_num, min_index, max_index;

    keys_num = SidInternalNodeGetKeysNum(internal_node);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        Sid cell_key;

        index = (max_index + min_index) / 2;
        cell_key = SidInternalNodeGetCellKey(internal_node, index);
        /* Notice: Greate EQ opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (cell_key >= key) 
            max_index = index;
        else 
            min_index = index + 1;
    }
    
    return min_index;
}

/* Get rid leaf node cell num. */
uint32_t SidLeafNodeGetCellNum(void *leaf_node) {
    return *(uint32_t *) (leaf_node + CELL_NUM_OFFSET);
}

/* Set rid leaf node cell num. */
void SidLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num) {
    *(uint32_t *)(leaf_node + CELL_NUM_OFFSET) = cell_num;
}

/* Increase rid leaf node cell num. */
void SidLeafNodeIncreaseCellNum(void *leaf_node) {
    (*(uint32_t *)(leaf_node + CELL_NUM_OFFSET))++;
}

/* Get rid leaf node sibling. */
uint32_t SidLeafNodeGetNextSibling(void *leaf_node) {
    return *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET);
}

/* Set rid leaf node sibling. */
void SidLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling) {
    *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET) = sibling;
}

/* Get rid leaf node cell key. */
Sid SidLeafNodeGetCellKey(void *leaf_node, uint32_t index) {
    return *(Sid *)(leaf_node + LEAF_NODE_HEAD_SIZE + SID_LEAF_NODE_CELL_LENGTH * index + SID_LEAF_NODE_VALUE_LENGTH);
}

/* Set rid leaf node cell key. */
void SidLeafNodeSetCellKey(void *leaf_node, uint32_t index, Sid key) {
    *(Sid *)(leaf_node + LEAF_NODE_HEAD_SIZE + SID_LEAF_NODE_CELL_LENGTH * index + SID_LEAF_NODE_VALUE_LENGTH) = key;
}

/* Get rid leaf node cell value. */
Refer *SidLeafNodeGetCellValue(void *leaf_node, uint32_t index) {
    return (Refer *)(leaf_node + LEAF_NODE_HEAD_SIZE + SID_LEAF_NODE_CELL_LENGTH * index);
}

/* Set rid leaf node cell value. */
void SidLeafNodeSetCellValue(void *leaf_node, uint32_t index, Refer *refer) {
    memcpy(leaf_node + LEAF_NODE_HEAD_SIZE + SID_LEAF_NODE_CELL_LENGTH * index, refer, SID_LEAF_NODE_VALUE_LENGTH);
}

/* Initialize rid leaf node. */
void SidLeafNodeInitialize(void *leaf_node, bool is_root) {
    SetNodeType(leaf_node, LEAF_NODE);
    NodeSetRoot(leaf_node, is_root);
    SidLeafNodeSetCellNum(leaf_node, 0);
    SidLeafNodeSetNextSibling(leaf_node, 0);
}

/* Find the leaf node cell postion.
 * --------------------------------
 * We will use binary search to find the target cell postion.
 * */
uint32_t SidLeafNodeFindCellNum(void *leaf_node, Sid key) {
    uint32_t cell_num, min_index, max_index;

    cell_num = SidLeafNodeGetCellNum(leaf_node);
    min_index = 0;
    max_index = cell_num;

    while (min_index != max_index) {
        uint32_t index;
        Sid cell_key;

        index = (max_index + min_index) / 2;
        cell_key = SidLeafNodeGetCellKey(leaf_node, index);
        /* Notice: Not only greater but aslo equal opreator is really import for store data, 
         * when keep the prince: always keep visible row lying at the forefront of same key cells. */
        if (cell_key >= key) {
            max_index = index;
        } else {
            min_index = index + 1; 
        }
    }

    return min_index;
}

/* Sid node get high key. */
Sid SidNodeGetHighKey(void *node) {
    switch (GetNodeType(node)) {
        case INTERNAL_NODE:
            return SidInternalNodeGetRightKey(node);
        case LEAF_NODE: {
            uint32_t cell_num = SidLeafNodeGetCellNum(node);
            return cell_num == 0 
                ? SID_ZERO 
                : SidLeafNodeGetCellKey(node, cell_num - 1);
        }
        default:
            UNEXPECTED_VALUE(GetNodeType(node));
            return SID_ZERO;
    }
}
