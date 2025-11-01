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
    MetaIndex *(*load) (Oid oid);
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
MetaIndex *IndexLoad(char *index_name) {
    Oid oid;
    IndexType type;

    oid = IndexNameFindOid(index_name);
    type = GetIndexType(oid);

    return methods[type].load(oid);
}

/* Index remove. */
bool IndexDrop(char *index_name) {
    Oid oid;
    IndexType type;

    oid = IndexNameFindOid(index_name);
    type = GetIndexType(oid);

    /* Drop index. */
    return methods[type].drop(oid);
}
