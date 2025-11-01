#include <stdbool.h>
#include "index.h"
#include "data.h"
#include "bin.h"
#include "hin.h"

/* Index methods. */
struct IndexMethods {
    void (*create)(MetaIndex *meta_index);
    MetaIndex *(*load) (Oid oid);
};

static struct IndexMethods methods[] = {
    /* For btree. */
    [BTREE_INDEX].create = BtreeIndexCreate,

    [HASH_INDEX].create = HashIndexCreate,
};

/* Index create. */
bool IndexCreate(MetaIndex *meta_index) {
    Assert(meta_index != NULL);
    methods[meta_index->type].create(meta_index);
    return true;
}

MetaIndex *IndexLoad(Oid oid) {
    return NULL;
}
