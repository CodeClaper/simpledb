#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "index.h"
#include "bufpool.h"
#include "data.h"
#include "bin.h"
#include "bininsert.h"
#include "binsearch.h"
#include "hin.h"
#include "table.h"
#include "systable.h"
#include "tuple.h"
#include "mmgr.h"
#include "fdesc.h"
#include "meta.h"
#include "compare.h"
#include "select.h"
#include "instance.h"
#include "heaptable.h"

/* Index methods. */
struct IndexMethods {
    bool (*create) (MetaIndex *meta_index);
    MetaIndex *(*load) (Oid oid, Table *table);
    bool (*drop) (Oid oid);
    bool (*insert) (MetaIndex *meta_index, void *key, Refer *value);
    void (*searchUnderCondition) (SelectResult *result, SelectPlan *plan);
};

static struct IndexMethods methods[] = {
    /* For btree index. */
    [BTREE_INDEX].create = BinCreate,
    [BTREE_INDEX].load = BinLoad,
    [BTREE_INDEX].drop = BinDrop,
    [BTREE_INDEX].insert = BtreeIndexInsert,
    [BTREE_INDEX].searchUnderCondition = BinSearchUnderCondition,
    
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

/* Generate index key by tuple. */ 
static void *TupleGenerateKey(MetaIndex *meta_index, void *tuple) {
    uint32_t offset = 0;
    void *key = dalloc(meta_index->key_len);

    ListCell *lc;
    foreach (lc, meta_index->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        void *value = TupleFindValue(tuple, meta_column);
        memcpy(key + offset, value, meta_column->column_length);
        offset += meta_column->column_length;
    }

    return key;
}


/* Get index next unused page num. */
uint32_t IndexGetNextUnusedPageNum(MetaIndex *meta_index) {
    uint32_t page_num = meta_index->page_num;
    while (!__sync_bool_compare_and_swap(&meta_index->page_num, page_num, page_num + 1)) {
        page_num = meta_index->page_num;
    }
    return page_num;
}

/* Compare each key in order. */
static int CompareKeyInner(MetaIndex *meta_index, void *key1, void *key2) {
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
int CompareKey(MetaIndex *meta_index, void *key1, void *key2) {
    if (key1 == NULL && key2 == NULL) return 0;
    else if (key1 != NULL && key2 == NULL) return 1;
    else if (key1 == NULL && key2 != NULL) return -1;
    else return CompareKeyInner(meta_index, key1, key2);
}

/* Loop heap table and reinsert into index. */
static bool LoopHeapTableAndReinsert(MetaIndex *meta_index) {
    Table *table;
    void *tuple;
    Refer *refer;

    table = open_table_inner(meta_index->tid);
    refer = new_refer(table->hoid, 0, 0);

    /* Keep loop and reinsert until there is no tuple. */
    while ((tuple = HeapTableLookupTuple(meta_index->tid, refer)) != NULL) {
        IndexInsert(meta_index, tuple, refer);
        /* Iterate refer. */
        HeapTableIteratorRefer(refer);
    }

    return true;
}

/* Index create. */
bool IndexCreate(MetaIndex *meta_index) {
    Assert(meta_index != NULL);
    return methods[meta_index->type].create(meta_index) && 
            LoopHeapTableAndReinsert(meta_index);
}

/* Index load. */
MetaIndex *IndexLoad(Oid oid, Table *table) {
    IndexType type = GetIndexType(oid);
    return methods[type].load(oid, table);
}
 
bool IndexDrop(Oid oid) {
    IndexType type = GetIndexType(oid);
    return methods[type].drop(oid);
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
        if (!IndexDrop(*(Oid *) lfirst(lc)))
            return false;
    }

    return true;
}

/* Index insert. */
bool IndexInsert(MetaIndex *meta_index, void *tuple, Refer *value) {
    void *key = TupleGenerateKey(meta_index, tuple);
    return methods[meta_index->type].insert(meta_index, key, value);
}

/* Index search. */
void IndexSearchUnderCondition(SelectResult *result, SelectPlan *plan) {
    Assert(plan->hit_index);
    return methods[plan->meta_index->type].searchUnderCondition(result, plan);
}

