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

/* Get bin internal node keys num. */
uint32_t BinInternalNodeGetKeysNum(void *internal_node);

/* Get bin internal node next sibling. */
void *BinInternalNodeGetRightKey(void *internal_node);

/* Get bin internal node next sibling. */
uint32_t BinInternalNodeGetRightNum(void *internal_node);

/* Get bin internal node cell key. */
void *BinInternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t index);

/* Get bin internal node cell value. */
uint32_t BinInternalNodeGetCellValue(void *internal_node, uint32_t key_len, uint32_t index);

/* Get bin leaf node cell num. */
uint32_t BinLeafNodeGetCellNum(void *leaf_node);

/* Get bin leaf node sibling. */
uint32_t BinLeafNodeGetSibling(void *leaf_node);

/* Set bin leaf node siling. */
void BinLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling);

/* Get bin node high key. */
void *BinNodeGetHighKey(void *node, uint32_t key_len, uint32_t value_len);

/* Compare bin node key. */
int BinCompareKey(MetaIndex *meta_index, void *key1, void *key2);

/* Btree index create. */
bool BtreeIndexCreate(MetaIndex *meta_index);

/* Btree index load. */
MetaIndex *BtreeIndexLoad(Oid oid, Table *table);

/* Btree index drop. */
bool BtreeIndexDrop(Oid oid);
