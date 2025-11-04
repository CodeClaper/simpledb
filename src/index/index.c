#include <stdbool.h>
#include "index.h"
#include "bufpool.h"
#include "data.h"
#include "bin.h"
#include "hin.h"
#include "table.h"
#include "systable.h"

/* Index methods. */
struct IndexMethods {
    bool (*create) (MetaIndex *meta_index);
    MetaIndex *(*load) (Oid oid, Table *table);
    bool (*drop) (Oid oid);
};

static struct IndexMethods methods[] = {
    /* For btree index. */
    [BTREE_INDEX].create = BtreeIndexCreate,
    [BTREE_INDEX].load = BtreeIndexLoad,
    [BTREE_INDEX].drop = BtreeIndexDrop,
    
    /* For hash index. */
    [HASH_INDEX].create = HashIndexCreate,
};

/* Get index type. */
static IndexType GetIndexType(Oid oid) {
    Buffer buffer;
    void *root_node;
    IndexType type;
    
    buffer = ReadBuffer(oid, ROOT_PAGE_NUM);
    LockBuffer(buffer, RW_READERS);

    root_node = GetBufferPage(buffer);
    type = *(IndexType *) (root_node + COMMON_NODE_HEADER_SIZE);

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    return type;
}

/* Index create. */
bool IndexCreate(MetaIndex *meta_index) {
    Assert(meta_index != NULL);
    return methods[meta_index->type].create(meta_index);
}

/* Index load. */
MetaIndex *IndexLoad(Oid oid, Table *table) {
    IndexType type = GetIndexType(oid);
    return methods[type].load(oid, table);
}

static bool IndexDropInner(Oid oid) {
    IndexType type = GetIndexType(oid);
    /* Drop index. */
    return methods[type].drop(oid);
}

/* Index drop. */
bool IndexDrop(char *index_name) {
    Oid oid = IndexNameFindOid(index_name);
    return IndexDropInner(oid);
}

/* Index drop by table name. */
bool IndexDropByTableName(char *table_name) {
    Table *table;
    Oid toid;
    List *indexs;

    table = open_table(table_name);
    toid = GET_TABLE_OID(table);
    indexs = ToidFindIndexs(toid);

    ListCell *lc;
    foreach (lc, indexs) {
        if (!IndexDropInner(*(Oid *) lfirst(lc)))
            return false;
    }

    return true;
}
