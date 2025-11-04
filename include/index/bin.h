#include <stdbool.h>
#include <stdint.h>
#include "data.h"
#include "const.h"
#include "ltbase.h"

#define BIN_ROOT_NODE_INDEX_TYPE_SIZE sizeof(uint32_t)
#define BIN_ROOT_NODE_IS_UNIQUE_SIZE sizeof(uint8_t)
#define BIN_ROOT_NODE_COLUMN_SIZE_SIZE sizeof(uint32_t)
#define BIN_ROOT_NODE_COLUMN_NAME_SIZE MAX_COLUMN_NAME_LEN

/* Btree index create. */
bool BtreeIndexCreate(MetaIndex *meta_index);

/* Btree index load. */
MetaIndex *BtreeIndexLoad(Oid oid, Table *table);

/* Btree index drop. */
bool BtreeIndexDrop(Oid oid);
