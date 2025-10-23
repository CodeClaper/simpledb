#include "table.h"

/* Check if leaf node is safe when inserting new item. */
bool BtreeInsertForLeafNodeSafe(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t default_value_len, uint32_t cell_num);

/* Insert item into the btree. */
Refer *BtreeInsert(Oid oid, void *key, void *value);
