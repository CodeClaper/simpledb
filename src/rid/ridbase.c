#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "ridbase.h"
#include "ltbase.h"
#include "const.h"
#include "refer.h"


/* Get rid internal node cell num. */
uint32_t RidInternalNodeGetKeysNum(void *internal_node) {
    return *(uint32_t *)(internal_node + KEYS_NUM_OFFSET);
}

/* Set rid internal node cell num. */
void RidInternalNodeSetKeysNum(void *internal_node, uint32_t keys_num) {
    *(uint32_t *)(internal_node + KEYS_NUM_OFFSET) = keys_num;
}

/* Set rid internal node cell num. */
void RidInternalNodeIncreaseKeysNum(void *internal_node) {
    (*(uint32_t *)(internal_node + KEYS_NUM_OFFSET))++;
}

/* Get rid internal node next sibling. */
uint32_t RidInternalNodeGetNextSibling(void *internal_node) {
    return *(uint32_t *)(internal_node + INTERNAL_NODE_NEXT_SIBLING_OFFSET);
}

/* Set rid internal node next sibling. */
void RidInternalNodeSetNextSibling(void *internal_node, uint32_t sibling) {
    *(uint32_t *)(internal_node + INTERNAL_NODE_NEXT_SIBLING_OFFSET) = sibling;
}

/* Get rid internal node right key. */
Rid RidInternalNodeGetRightKey(void *internal_node) {
    return *(Rid *)(internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE);
}

/* Set rid internal node right key. */
void RidInternalNodeSetRightKey(void *internal_node, Rid key) {
    *(Rid *)(internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE) = key;
}

/* Get rid internal node right page num. */
uint32_t RidInternalNodeGetRightNum(void *internal_node) { 
    return *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET);
}

/* Get rid internal node right page num. */
void RidInternalNodeSetRightNum(void *internal_node, uint32_t right_num) { 
     *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET) = right_num;
}

/* Get rid internal node cell key. */
Rid RidInternalNodeGetCellKey(void *internal_node, uint32_t index) {
    return *(Rid *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + \
            RIGHT_CHILD_SIZE + RID_KEY_LENGTH + RID_INTERNAL_NODE_CELL_LENGTH * index + RID_INTERNAL_NODE_VALUE_LENGTH);
}

/* Set rid internal node cell key. */
void RidInternalNodeSetCellKey(void *internal_node, uint32_t index, Rid key) {
    *(Rid *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + \
            RIGHT_CHILD_SIZE + RID_KEY_LENGTH + RID_INTERNAL_NODE_CELL_LENGTH * index + RID_INTERNAL_NODE_VALUE_LENGTH) = key;
}

/* Get rid internal node cell value. */
uint32_t RidInternalNodeGetCellValue(void *internal_node, uint32_t index) {
    return *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + \
                         RIGHT_CHILD_SIZE + RID_KEY_LENGTH + RID_INTERNAL_NODE_CELL_LENGTH * index);
}

/* Get rid internal node cell value. */
void RidInternalNodeSetCellValue(void *internal_node, uint32_t index, uint32_t cell_value) {
    *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + \
                  RID_KEY_LENGTH + RID_INTERNAL_NODE_CELL_LENGTH * index) = cell_value;
}

/* Initialize rid internal node. */
void RidInternalNodeInitialize(void *internal_node, bool is_root) {
    SetNodeType(internal_node, INTERNAL_NODE);
    NodeSetRoot(internal_node, is_root);
    RidInternalNodeSetKeysNum(internal_node, 0);
    RidInternalNodeSetNextSibling(internal_node, 0);
}

/* Find the internal node cell postion.
 * --------------------------------
 * We will use binary search to find the target cell postion.
 * */
uint32_t RidInternalNodeFindCellNum(void *internal_node, Rid key) {
    uint32_t keys_num, min_index, max_index;

    keys_num = RidInternalNodeGetKeysNum(internal_node);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        Rid cell_key;

        index = (max_index + min_index) / 2;
        cell_key = RidInternalNodeGetCellKey(internal_node, index);
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
uint32_t RidLeafNodeGetCellNum(void *leaf_node) {
    return *(uint32_t *) (leaf_node + CELL_NUM_OFFSET);
}

/* Set rid leaf node cell num. */
void RidLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num) {
    *(uint32_t *)(leaf_node + CELL_NUM_OFFSET) = cell_num;
}

/* Increase rid leaf node cell num. */
void RidLeafNodeIncreaseCellNum(void *leaf_node) {
    (*(uint32_t *)(leaf_node + CELL_NUM_OFFSET))++;
}

/* Get rid leaf node sibling. */
uint32_t RidLeafNodeGetNextSibling(void *leaf_node) {
    return *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET);
}

/* Set rid leaf node sibling. */
void RidLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling) {
    *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET) = sibling;
}

/* Get rid leaf node cell key. */
Rid RidLeafNodeGetCellKey(void *leaf_node, uint32_t index) {
    return *(Rid *)(leaf_node + LEAF_NODE_HEAD_SIZE + RID_LEAF_NODE_CELL_LENGTH * index + RID_LEAF_NODE_VALUE_LENGTH);
}

/* Set rid leaf node cell key. */
void RidLeafNodeSetCellKey(void *leaf_node, uint32_t index, Rid key) {
    *(Rid *)(leaf_node + LEAF_NODE_HEAD_SIZE + RID_LEAF_NODE_CELL_LENGTH * index + RID_LEAF_NODE_VALUE_LENGTH) = key;
}

/* Get rid leaf node cell value. */
Refer *RidLeafNodeGetCellValue(void *leaf_node, uint32_t index) {
    return (Refer *)(leaf_node + LEAF_NODE_HEAD_SIZE + RID_LEAF_NODE_CELL_LENGTH * index);
}

/* Set rid leaf node cell value. */
void RidLeafNodeSetCellValue(void *leaf_node, uint32_t index, Refer *refer) {
    memcpy(leaf_node + LEAF_NODE_HEAD_SIZE + RID_LEAF_NODE_CELL_LENGTH * index, refer, RID_LEAF_NODE_VALUE_LENGTH);
}

/* Initialize rid leaf node. */
void RidLeafNodeInitialize(void *leaf_node, bool is_root) {
    SetNodeType(leaf_node, LEAF_NODE);
    NodeSetRoot(leaf_node, is_root);
    RidLeafNodeSetCellNum(leaf_node, 0);
    RidLeafNodeSetNextSibling(leaf_node, 0);
}

/* Find the leaf node cell postion.
 * --------------------------------
 * We will use binary search to find the target cell postion.
 * */
uint32_t RidLeafNodeFindCellNum(void *leaf_node, Rid key) {
    uint32_t cell_num, min_index, max_index;

    cell_num = RidLeafNodeGetCellNum(leaf_node);
    min_index = 0;
    max_index = cell_num;

    while (min_index != max_index) {
        uint32_t index;
        Rid cell_key;

        index = (max_index + min_index) / 2;
        cell_key = RidLeafNodeGetCellKey(leaf_node, index);
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

/* Rid node get high key. */
Rid RidNodeGetHighKey(void *node) {
    switch (GetNodeType(node)) {
        case INTERNAL_NODE:
            return RidInternalNodeGetRightKey(node);
        case LEAF_NODE: {
            uint32_t cell_num = RidLeafNodeGetCellNum(node);
            return cell_num == 0 
                ? RID_ZERO 
                : RidLeafNodeGetCellKey(node, cell_num - 1);
        }
        default:
            UNEXPECTED_VALUE(GetNodeType(node));
            return RID_ZERO;
    }
}
