#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "bin.h"
#include "table.h"
#include "ltbase.h"
#include "log.h"
#include "mmgr.h"
#include "meta.h"
#include "bufmgr.h"
#include "fdesc.h"
#include "refer.h"
#include "pager.h"
#include "compare.h"
#include "systable.h"

/* Set bin root node index type. */
static void BinRootNodeSetIndexType(void *root_node, IndexType type) {
    Assert(NodeIsRoot(root_node));
    *(uint32_t *) (root_node + COMMON_NODE_HEADER_SIZE) = type;
}

/* Get bin root node index type. */
static IndexType BinRootNodeGetIndexType(void *root_node) {
    Assert(NodeIsRoot(root_node));
    uint32_t value = *(uint32_t *) (root_node + COMMON_NODE_HEADER_SIZE);
    return (IndexType) value;
}

/* Set bin root node unique. */
static void BinRootNodeSetUnique(void *root_node, bool is_unique) {
    Assert(NodeIsRoot(root_node));
    *(uint8_t *) (root_node + COMMON_NODE_HEADER_SIZE + BIN_ROOT_NODE_INDEX_TYPE_SIZE) = is_unique;
}

/* Get bin root node unique. */
static bool BinRootNodeIsUnique(void *root_node) {
    Assert(NodeIsRoot(root_node));
    uint8_t value = *(uint8_t *) (root_node + COMMON_NODE_HEADER_SIZE + BIN_ROOT_NODE_INDEX_TYPE_SIZE);
    return (bool) value;
}

/* Get bin root node column size. */
uint32_t BinRootNodeGetColumnSize(void *root_node) {
    Assert(NodeIsRoot(root_node));
    return *(uint32_t *) (root_node + COMMON_NODE_HEADER_SIZE + BIN_ROOT_NODE_INDEX_TYPE_SIZE + BIN_ROOT_NODE_IS_UNIQUE_SIZE);
}

/* Set bin root node column size. */
static void BinRootNodeSetColumnSize(void *root_node, uint32_t column_size) {
    Assert(NodeIsRoot(root_node));
    *(uint32_t *) (root_node + COMMON_NODE_HEADER_SIZE + BIN_ROOT_NODE_INDEX_TYPE_SIZE + BIN_ROOT_NODE_IS_UNIQUE_SIZE) = column_size;
}

/* Get bin root node column name. */
static char *BinRootNodeGetColumnName(void *root_node, uint32_t index) {
    Assert(NodeIsRoot(root_node));
    return (char *) (root_node + COMMON_NODE_HEADER_SIZE + BIN_ROOT_NODE_INDEX_TYPE_SIZE + BIN_ROOT_NODE_IS_UNIQUE_SIZE + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + BIN_ROOT_NODE_COLUMN_NAME_SIZE * index);
}

/* Set bin root node column name. */
static void BinRootNodeSetColumnName(void *root_node, uint32_t index, char *column_name) {
    Assert(NodeIsRoot(root_node));
    strcpy((root_node + COMMON_NODE_HEADER_SIZE + BIN_ROOT_NODE_INDEX_TYPE_SIZE + BIN_ROOT_NODE_IS_UNIQUE_SIZE + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + BIN_ROOT_NODE_COLUMN_NAME_SIZE * index), column_name);
}

/* Get bin internal node keys num. */
uint32_t BinInternalNodeGetKeysNum(void *internal_node) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                            BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size);
    } else
        return *(uint32_t *)(internal_node + KEYS_NUM_OFFSET);
}

/* Set bin internal node keys num. */
void BinInternalNodeSetKeysNum(void *internal_node, uint32_t keys_num) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                    BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size) = keys_num;
    } else
        *(uint32_t *)(internal_node + KEYS_NUM_OFFSET) = keys_num;
}

/* Increase bin internal node keys num. */
void BinInternalNodeIncreaseKeysNum(void *internal_node) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        (*(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size))++;
    } else
        (*(uint32_t *)(internal_node + KEYS_NUM_OFFSET))++;
}

/* Get bin internal node next sibling. */
uint32_t BinInternalNodeGetNextSibling(void *internal_node) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                            BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE);
    } else
        return *(uint32_t *)(internal_node + INTERNAL_NODE_NEXT_SIBLING_OFFSET);
}

/* Get bin internal node next sibling. */
void BinInternalNodeSetNextSibling(void *internal_node, uint32_t sibling) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE) = sibling;
    } else
        *(uint32_t *)(internal_node + INTERNAL_NODE_NEXT_SIBLING_OFFSET) = sibling;
}

/* Get bin internal node next sibling. */
void *BinInternalNodeGetRightKey(void *internal_node) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        return (internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE);
    } else
        return (internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE);
}

/* Set bin internal node next sibling. */
void BinInternalNodeSetRightKey(void *internal_node, uint32_t key_len, void *right_key) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        memcpy(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
               BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE, right_key, key_len);
    } else
        memcpy(internal_node + RIGHT_CHILD_OFFSET + RIGHT_CHILD_SIZE, right_key, key_len);
}

/* Get bin internal node next sibling. */
uint32_t BinInternalNodeGetRightNum(void *internal_node) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                             BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE);
    } else
        return *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET);
}

/* Set bin internal node next sibling. */
void BinInternalNodeSetRightNum(void *internal_node, uint32_t right_num) {
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE) = right_num;
    } else
        *(uint32_t *)(internal_node + RIGHT_CHILD_OFFSET) = right_num;
}

/* Get bin internal node cell key. */
void *BinInternalNodeGetCellKey(void *internal_node, uint32_t key_len, uint32_t index) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        return (internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE);
    } else
        return (internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE);
}

/* Set bin internal node cell key. */
void BinInternalNodeSetCellKey(void *internal_node, uint32_t key_len, uint32_t index, void *cell_key) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        memcpy(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
               BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE, cell_key, key_len);
    } else
        memcpy(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE, cell_key, key_len);
}

/* Get bin internal node cell value. */
uint32_t BinInternalNodeGetCellValue(void *internal_node, uint32_t key_len, uint32_t index) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        return *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index);
    } else
        return *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index);
}

/* Set bin internal node cell value. */
void BinInternalNodeSetCellValue(void *internal_node, uint32_t key_len, uint32_t index, uint32_t cell_value) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (NodeIsRoot(internal_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(internal_node);
        *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                   BIN_ROOT_NODE_COLUMN_NAME_SIZE * column_size + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index) = cell_value;
    } else
        *(uint32_t *)(internal_node + COMMON_NODE_HEADER_SIZE + KEYS_NUM_SIZE + INTERNAL_NODE_NEXT_SIBLING_SIZE + RIGHT_CHILD_SIZE + key_len + cell_len * index) = cell_value;
}

/* Initialize bin internal node. */
void BinInternalNodeInitialize(void *internal_node, bool is_root) {
    SetNodeType(internal_node, INTERNAL_NODE);
    NodeSetRoot(internal_node, is_root);
    BinInternalNodeSetKeysNum(internal_node, 0);
    BinInternalNodeSetNextSibling(internal_node, 0);
}

/* Find the internal node cell num postion. 
 * -----------------------------------------
 * In this function, we will use binary search to find the target cell.
 * */
uint32_t BinInternalNodeFindCellNum(MetaIndex *meta_index, void *internal_node, void *key) {
    uint32_t keys_num, min_index, max_index;

    keys_num = BinInternalNodeGetKeysNum(internal_node);
    min_index = 0;
    max_index = keys_num;

    while (min_index != max_index) {
        uint32_t index;
        void *cell_key;

        index = (max_index + min_index) / 2;
        cell_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, index);
        /* Notice: Greate EQ opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (BinCompareKey(meta_index, cell_key, key) >= 0) 
            max_index = index;
        else 
            min_index = index + 1;
    }
    
    return min_index;
}

/* Get bin leaf node cell num. */
uint32_t BinLeafNodeGetCellNum(void *leaf_node) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                            BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size);
    } else
        return *(uint32_t *)(leaf_node + CELL_NUM_OFFSET);
}

/* Set bin leaf node cell num. */
void BinLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size) = cell_num;
    } else
        *(uint32_t *)(leaf_node + CELL_NUM_OFFSET) = cell_num;
}

/* Increase bin leaf node cell num. */
void BinLeafNodeIncreaseCellNum(void *leaf_node) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        (*(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size))++;
    } else
        (*(uint32_t *)(leaf_node + CELL_NUM_OFFSET))++;
}


/* Get bin leaf node sibling. */
uint32_t BinLeafNodeGetNextSibling(void *leaf_node) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                             BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + CELL_NUM_SIZE);
    } else 
        return *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET);
}

/* Set bin leaf node siling. */
void BinLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + CELL_NUM_SIZE) = sibling;
    } else 
        *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET) = sibling;
}


/* Get bin leaf node cell key. */
void *BinLeafNodeGetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        return (leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + CELL_NUM_SIZE + LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index + value_len);
    } else 
        return (leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index + value_len);
}


/* Get bin leaf node cell key. */
void BinLeafNodeSetCellKey(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index, void *cell_key) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        memcpy(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
               BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + CELL_NUM_SIZE + LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index + value_len, cell_key, key_len);
    } else 
        memcpy(leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index + value_len, cell_key, key_len);
}


/* Get bin leaf node cell value. */
void *BinLeafNodeGetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        return (leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + CELL_NUM_SIZE + LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index);
    } else 
        return (leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index);
}


/* Set bin leaf node cell value. */
void BinLeafNodeSetCellValue(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t index, void *value) {
    uint32_t cell_len = key_len + value_len;
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        memcpy(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
               BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + CELL_NUM_SIZE + LEAF_NODE_NEXT_SIBLING_SIZE + cell_len * index, value, value_len);
    } else 
        memcpy(leaf_node + LEAF_NODE_HEAD_SIZE + cell_len * index, value, value_len);
}

/* Initialize bin leaf node. */
void BinLeafNodeInitialize(void *leaf_node, bool is_root) {
    SetNodeType(leaf_node, LEAF_NODE);
    NodeSetRoot(leaf_node, is_root);
    BinLeafNodeSetCellNum(leaf_node, 0);
    BinLeafNodeSetNextSibling(leaf_node, 0);
}

/* Find the leaf node cell num postion. 
 * -----------------------------------
 * We will use binary search to find the target cell.
 * */
uint32_t BinLeafNodeFindCellNum(MetaIndex *meta_index, void *leaf_node, void *key) {
    uint32_t cell_num, min_index, max_index;

    cell_num = BinLeafNodeGetCellNum(leaf_node);
    min_index = 0;
    max_index = cell_num;

    while (min_index != max_index) {
        uint32_t index;
        void *cell_key;

        index = (max_index + min_index) / 2;
        cell_key = BinLeafNodeGetCellKey(leaf_node, meta_index->key_len, meta_index->value_len, index);
        /* Notice: Not only greater but aslo equal opreator is really import for store data, 
         * when keep the prince: always keep visible row lying at the forefront of same key cells. */
        if (BinCompareKey(meta_index, cell_key, key) >= 0) {
            max_index = index;
        } else {
            min_index = index + 1; 
        }
    }

    return min_index;
}

/* Get bin node high key. */
void *BinNodeGetHighKey(void *node, uint32_t key_len, uint32_t value_len) {
    switch (GetNodeType(node)) {
        case INTERNAL_NODE:
            return BinInternalNodeGetRightKey(node);
        case LEAF_NODE: {
            uint32_t cell_num = BinLeafNodeGetCellNum(node);
            return cell_num == 0
                ? NULL
                : BinLeafNodeGetCellKey(node, key_len, value_len, cell_num - 1);
        }
        default:
            UNEXPECTED_VALUE(GetNodeType(node));
            return NULL;
    }
}

/* Compare each key in order. */
static int BinCompareKeyInner(MetaIndex *meta_index, void *key1, void *key2) {
    uint32_t offset = 0;
    ListCell *lc;
    foreach (lc, meta_index->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        void *v1 = GetComparableValue(key1 + offset, meta_column->column_type);
        void *v2 = GetComparableValue(key2 + offset, meta_column->column_type);
        if (EQ(v1, v2, meta_column->column_type)) continue;
        else if (GT(v1, v2, meta_column->column_type)) return 1;
        else return -1;
        offset += meta_column->column_length;
    }
    return 0;
}

/* Compare bin node key. */
int BinCompareKey(MetaIndex *meta_index, void *key1, void *key2) {
    if (key1 == NULL && key2 == NULL) return 0;
    else if (key1 != NULL && key2 == NULL) return 1;
    else if (key1 == NULL && key2 != NULL) return -1;
    else return BinCompareKeyInner(meta_index, key1, key2);
}

/* Btree index create. */
bool BinCreate(MetaIndex *meta_index) {
    char *file_path;
    int descr;
    void *root_node;

    file_path = table_file_path(meta_index->oid);
    if (table_file_exist(file_path)) {
        db_log(ERROR, "Index '%s' already exists.", meta_index->index_name);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(ERROR, "Open database file '%s' fail.", file_path);
        return false;
    }

    root_node = dalloc(PAGE_SIZE);
    
    /* Initialize bin leaf node. */
    BinLeafNodeInitialize(root_node, true);
    BinRootNodeSetUnique(root_node, meta_index->is_unique);
    BinRootNodeSetIndexType(root_node, meta_index->type);
    BinRootNodeSetUnique(root_node, meta_index->is_unique);
    BinRootNodeSetColumnSize(root_node, meta_index->column_size);
    
    ListCell *lc;
    foreach (lc, meta_index->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        BinRootNodeSetColumnName(root_node, __i, meta_column->column_name);
    }

    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    ssize_t w_size = write(descr, root_node, PAGE_SIZE);
    if (w_size == -1) {
        db_log(ERROR, "Write index meta info error and error message: %s.", strerror(errno));
        return false;
    }

    /* Close desription. */
    close(descr);

    /* Free memory. */
    dfree(file_path);
    dfree(root_node);

    return true;
}

/* Btree index load. */
MetaIndex *BinLoad(Oid oid, Table *table) {
    Buffer buffer;
    void *root;
    Object object;
    MetaIndex *meta_index;
    
    meta_index = instance(MetaIndex);
    buffer = ReadBuffer(oid, ROOT_PAGE_NUM);
    LockBuffer(buffer, RW_READERS);
    root = GetBufferPage(buffer);

    object = OidFindObject(oid);

    meta_index->oid = oid;
    meta_index->tid = object.toid;
    meta_index->index_name = dstrdup(object.relname);
    meta_index->type = BinRootNodeGetIndexType(root);
    meta_index->is_unique = BinRootNodeIsUnique(root);
    meta_index->is_pri = false;
    meta_index->column_size = BinRootNodeGetColumnSize(root);
    meta_index->meta_columns = create_list(NODE_META_COLUMN);
    meta_index->page_num = GetPageSize(oid);
    meta_index->key_len = 0;
    meta_index->value_len = REFER_SIZE;

    for (int i = 0; i < meta_index->column_size; i++) {
        char *column_name;
        MetaColumn *meta_column;

        column_name = BinRootNodeGetColumnName(root, i);
        meta_column = NameFindMetaColumn(table->meta_table, column_name);

        append_list(meta_index->meta_columns, meta_column);
        meta_index->key_len += meta_column->column_length;
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return meta_index;
}

/* Btree index drop. */
bool BinDrop(Oid oid) {
    Assert(NON_ZERO_OID(oid));

    char *file_path;

    file_path = table_file_path(oid);
    if (!table_file_exist(file_path)) {
        db_log(ERROR, "Logic error, not found index file %ld", oid);
        return false;
    }

    /* Remove from disk and remove the object. */
    if (remove(file_path) == 0 && RemoveObject(oid)) {
        /* Unregister fdesc. */
        unregister_fdesc(oid);
        return true;
    }

    db_log(ERROR, "Index file %s deleted fail, error: %s", oid, strerror(errno));
    return false;
}

