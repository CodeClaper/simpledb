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
#include "fdesc.h"
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
static uint32_t BinRootNodeGetColumnSize(void *root_node) {
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

static void BinRootNodeSetColumnName(void *root_node, uint32_t index, char *column_name) {
    Assert(NodeIsRoot(root_node));
    strcpy((root_node + COMMON_NODE_HEADER_SIZE + BIN_ROOT_NODE_INDEX_TYPE_SIZE + BIN_ROOT_NODE_IS_UNIQUE_SIZE + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + BIN_ROOT_NODE_COLUMN_NAME_SIZE * index), column_name);
}

/* Get bin leaf node cell num. */
static uint32_t BinLeafNodeGetCellNum(void *leaf_node) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                            BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size);
    } else
        return *(uint32_t *)(leaf_node + CELL_NUM_OFFSET);
}

/* Set bin leaf node cell num. */
static void BinLeafNodeSetCellNum(void *leaf_node, uint32_t cell_num) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size) = cell_num;
    } else
        *(uint32_t *)(leaf_node + CELL_NUM_OFFSET) = cell_num;
}

/* Get bin leaf node sibling. */
static uint32_t BinLeafNodeGetSibling(void *leaf_node) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        return *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                             BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + LEAF_NODE_NEXT_SIBLING_OFFSET);
    } else 
        return *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET);
}

/* Set bin leaf node siling. */
static void BinLeafNodeSetNextSibling(void *leaf_node, uint32_t sibling) {
    if (NodeIsRoot(leaf_node)) {
        uint32_t column_size = BinRootNodeGetColumnSize(leaf_node);
        *(uint32_t *)(leaf_node + COMMON_NODE_HEADER_SIZE  + BIN_ROOT_NODE_INDEX_TYPE_SIZE  + BIN_ROOT_NODE_IS_UNIQUE_SIZE  + BIN_ROOT_NODE_COLUMN_SIZE_SIZE + 
                      BIN_ROOT_NODE_COLUMN_NAME_SIZE  * column_size + LEAF_NODE_NEXT_SIBLING_OFFSET) = sibling;
    } else 
        *(uint32_t *)(leaf_node + LEAF_NODE_NEXT_SIBLING_OFFSET) = sibling;
}

/* Initialize leaf node. */
static void BinLeafNodeInitialize(void *leaf_node, bool is_root) {
    SetNodeType(leaf_node, LEAF_NODE);
    NodeSetRoot(leaf_node, true);
    BinLeafNodeSetCellNum(leaf_node, 0);
    BinLeafNodeSetNextSibling(leaf_node, 0);
}

/* Btree index create. */
bool BtreeIndexCreate(MetaIndex *meta_index) {
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
    BinLeafNodeInitialize(root_node, false);
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
MetaIndex *BtreeIndexLoad(Oid oid) {
    return NULL;
}

/* Btree index drop. */
bool BtreeIndexDrop(Oid oid) {
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
