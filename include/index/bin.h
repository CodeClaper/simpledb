#include <stdbool.h>
#include <stdint.h>
#include "data.h"
#include "const.h"
#include "ltbase.h"

#define BIN_ROOT_NODE_INDEX_TYPE_SIZE sizeof(uint32_t)
#define BIN_ROOT_NODE_IS_UNIQUE_SIZE sizeof(uint8_t)
#define BIN_ROOT_NODE_COLUMN_SIZE_SIZE sizeof(uint32_t)
#define BIN_ROOT_NODE_COLUMN_NAME_SIZE MAX_COLUMN_NAME_LEN

/* Get bin root node column size. */
uint32_t BinRootNodeGetColumnSize(void *root_node);

/* Get bin internal node next sibling. */
uint32_t BinInternalNodeGetNextSibling(void *internal_node);

/* Get bin internal node next sibling. */
void BinInternalNodeSetNextSibling(void *internal_node, uint32_t sibling);

/* Get bin internal node keys num. */
uint32_t BinInternalNodeGetKeysNum(void *internal_node);

/* Set bin internal node keys num. */
void BinInternalNodeSetKeysNum(void *internal_node, uint32_t keys_num);

/* Increase bin internal node keys num. */
void BinInternalNodeIncreaseKeysNum(void *internal_node);

/* Get bin internal node next sibling. */
void *BinInternalNodeGetRightKey(void *internal_node);

/* Set bin internal node next sibling. */
void BinInternalNodeSetRightKey(void *internal_node, uint32_t key_len, void *right_key);

/* Get bin internal node next sibling. */
uint32_t BinInternalNodeGetRightNum(void *internal_node);

/* Set bin internal node next sibling. */
void BinInternalNodeSetRightNum(void *internal_node, uint32_t right_num);

/* Get bin internal node cell key. */
void *BinInternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t index);

/* Set bin internal node cell key. */
void BinInternalNodeSetCellKey(void *internal_node, uint32_t key_len, uint32_t index, void *cell_key);

/* Get bin internal node cell value. */
uint32_t BinInternalNodeGetCellValue(void *internal_node, uint32_t key_len, uint32_t index);

/* Set bin internal node cell value. */
void BinInternalNodeSetCellValue(void *internal_node, uint32_t key_len, uint32_t index, uint32_t cell_value);

/* Initialize bin internal node. */
void BinInternalNodeInitialize(void *internal_node, bool is_root);

/* Find the internal node cell num postion. */
uint32_t BinInternalNodeFindCellNum(MetaIndex *meta_index, void *internal_node, void *key);

/* Get bin leaf node cell num. */
uint32_t BinLeafNodeGetCellNum(void *leaf_node);

/* Set bin leaf node cell num. */
void BinLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num);

/* Increase bin leaf node cell num. */
void BinLeafNodeIncreaseCellNum(void *leaf_node);

/* Get bin leaf node sibling. */
uint32_t BinLeafNodeGetNextSibling(void *leaf_node);

/* Set bin leaf node siling. */
void BinLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling);

/* Get bin leaf node cell key. */
void *BinLeafNodeGetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index); 

/* Get bin leaf node cell key. */
void BinLeafNodeSetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index, void *cell_key);

/* Get bin leaf node cell value. */
void *BinLeafNodeGetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index);

/* Set bin leaf node cell value. */
void BinLeafNodeSetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index, void *value);

/* Initialize bin leaf node. */
void BinLeafNodeInitialize(void *leaf_node, bool is_root);

/* Find the leaf node cell num postion. */
uint32_t BinLeafNodeFindCellNum(MetaIndex *meta_index, void *leaf_node, void *key); 

/* Get bin node high key. */
void *BinNodeGetHighKey(void *node, uint32_t key_len, uint32_t value_len);

/* Compare bin node key. */
int BinCompareKey(MetaIndex *meta_index, void *key1, void *key2);

/* Btree index create. */
bool BinCreate(MetaIndex *meta_index);

/* Btree index load. */
MetaIndex *BinLoad(Oid oid, Table *table);

/* Btree index drop. */
bool BinDrop(Oid oid);
