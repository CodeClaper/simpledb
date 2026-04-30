#include <stdbool.h>
#include <stdint.h>
#include "data.h"
#include "const.h"
#include "ltbase.h"

#define BIN_ROOT_NODE_INDEX_TYPE_SIZE sizeof(uint32_t)
#define BIN_ROOT_NODE_IS_UNIQUE_SIZE sizeof(uint8_t)
#define BIN_ROOT_NODE_COLUMN_SIZE_SIZE sizeof(uint32_t)
#define BIN_ROOT_NODE_COLUMN_NAME_SIZE MAX_COLUMN_NAME_LEN

uint32_t BinRootNodeGetColumnSize(void *root_node);
uint32_t BinInternalNodeGetNextSibling(void *internal_node);
void BinInternalNodeSetNextSibling(void *internal_node, uint32_t sibling);
uint32_t BinInternalNodeGetKeysNum(void *internal_node);
void BinInternalNodeSetKeysNum(void *internal_node, uint32_t keys_num);
void BinInternalNodeIncreaseKeysNum(void *internal_node);
void *BinInternalNodeGetRightKey(void *internal_node);
void BinInternalNodeSetRightKey(void *internal_node, uint32_t key_len, void *right_key);
uint32_t BinInternalNodeGetRightNum(void *internal_node);
void BinInternalNodeSetRightNum(void *internal_node, uint32_t right_num);
void *BinInternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t index);
void BinInternalNodeSetCellKey(void *internal_node, uint32_t key_len, uint32_t index, void *cell_key);
uint32_t BinInternalNodeGetCellValue(void *internal_node, uint32_t key_len, uint32_t index);
void BinInternalNodeSetCellValue(void *internal_node, uint32_t key_len, uint32_t index, uint32_t cell_value);
void BinInternalNodeInitialize(void *internal_node, bool is_root);
uint32_t BinInternalNodeFindCellNum(MetaIndex *meta_index, void *internal_node, void *key);
uint32_t BinLeafNodeGetCellNum(void *leaf_node);
void BinLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num);
void BinLeafNodeIncreaseCellNum(void *leaf_node);
uint32_t BinLeafNodeGetNextSibling(void *leaf_node);
void BinLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling);
void *BinLeafNodeGetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index); 
void BinLeafNodeSetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index, void *cell_key);
void *BinLeafNodeGetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index);
void BinLeafNodeSetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index, void *value);
void BinLeafNodeInitialize(void *leaf_node, bool is_root);
uint32_t BinLeafNodeFindCellNum(MetaIndex *meta_index, void *leaf_node, void *key); 
void *BinNodeGetHighKey(void *node, uint32_t key_len, uint32_t value_len);
bool BinCreate(MetaIndex *meta_index);
MetaIndex *BinLoad(Oid oid, Table *table);
bool BinDrop(Oid oid);
