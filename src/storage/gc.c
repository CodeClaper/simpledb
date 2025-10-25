/**
* ================================= The Garbage Collector (GC) ===================================================
* The GC module is intended to collecte the deleted rows and clean up the unused disk.
* Usually, GC works in the deamon thread, and loop interval scanning.
* GC only starts working on the table only if there are no transactions on the table.
* When GC working on the table, it will lock the table, any transaction that trying to write the table will block.
* =================================================================================================================
*/
#include <unistd.h>
#include "gc.h"
#include "data.h"
#include "row.h"
#include "log.h"
#include "free.h"
#include "trans.h"
#include "table.h"
#include "ltsearch.h"
#include "refer.h"
#include "tablecache.h"
#include "select.h"
#include "asserts.h"
#include "instance.h"
#include "systable.h"
#include "optimizer.h"
#include "tuple.h"

#define DEFAULT_GC_INTERVAL 10

/* Check if allow to GC for now. */
static bool allow_gc();

/* loop GC */
void loop_gc() {
    
    /* Loop scanning. */
    while(true) {

        sleep(DEFAULT_GC_INTERVAL); /* Sleep specified interval. */
        
        if (!allow_gc())
            continue;


        /* Each check loop opens a new transaction. */
        AutoBeginTransaction();

        /* loop each of tables to gc. */
        List *obj_list = FindAllObject();

        ListCell *lc;
        foreach (lc, obj_list) {
            Object *entity = (Object *)lfirst(lc);
            if (TABLE_OR_VIEW(entity->reltype))
                gc_table(entity->relname); 
        }

        /* Commit transction manually. */
        AutoCommitTransaction();
    }
}

/* Check if allow to GC for now. 
 * Conditions: 
 * (1) No transaction running.
 * */
static bool allow_gc() {
    /* Wait all transaction committed. */
    while(AnyTransactionRunning()) {
        usleep(10);
    }
    return true;
}

/* GC row*/
static void GCTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    Oid oid;
    Table *table;
    Xid created_xid, expired_xid;
    void *key;
    Refer *refer;

    oid = select_result->oid;
    table = open_table_inner(oid);
    created_xid = TupleFindCreatedXid(tuple, table->meta_table);
    expired_xid = TupleFindCreatedXid(tuple, table->meta_table);
    key = TupleFindKey(tuple, table->meta_table);

    /* Only for deleted row. */
    if (!IsVisible(created_xid, expired_xid))
        return;

    /* Get refer. */
    refer = BtreeSearchRefer(oid, key);

    /* Delete row. */
    // delete_row_data(key, refer);
}

/* Gc table */
void gc_table(char *table_name) {
#ifdef DEBUG
    db_log(DEBUG, "GC table '%s'.", table_name);
#endif
    /* Query with condition, and delete satisfied condition row. */
    SelectResult *select_result = new_select_result(UNKONWN_STMT, table_name, true);
    QueryUnderSearchCondition(select_result, SimpleSelectPlan(GCTuple, ARG_NULL, NULL, NULL));
    free_select_result(select_result);
}

