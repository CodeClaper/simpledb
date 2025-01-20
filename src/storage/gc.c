/**
* ================================= The Garbage Collector (GC) ===================================================
* The GC module is intended to collecte the deleted rows and clean up the unused disk.
* Usually, GC works in the deamon thread, and loop interval scanning.
* GC only starts working on the table only if there are no transactions on the table.
* When GC working on the table, it will lock the table, any transaction that trying to write the table will block.
* =================================================================================================================
*/
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "gc.h"
#include "data.h"
#include "log.h"
#include "free.h"
#include "trans.h"
#include "table.h"
#include "ltree.h"
#include "refer.h"
#include "cache.h"
#include "select.h"
#include "asserts.h"

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
        List *table_list = get_table_list();

        ListCell *lc;
        foreach (lc, table_list) {
            gc_table(lfirst(lc)); 
        }

        free_list_deep(table_list);

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

/* Gc row*/
void gc_row(Row *row, SelectResult *select_result, 
            Table *table, ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* Only for deleted row. */
    if (!RowIsDeleted(row))
        return;

    /* Cursor */
    Cursor * cursor = define_cursor(table, row->key, true);

    /* Delete row. */
    delete_leaf_node_cell(cursor, row->key);
}

/* Gc table */
void gc_table(char *table_name) {

#ifdef DEBUG
    db_log(DEBUG, "GC table '%s'.", table_name);
#endif

    /* Query with condition, and delete satisfied condition row. */
    SelectResult *select_result = new_select_result(table_name);

    query_with_condition(NULL, select_result, gc_row, ARG_NULL, NULL);
    
    free_select_result(select_result);

}

