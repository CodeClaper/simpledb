/********************************** Delete Module ********************************************
 * Auth:        JerryZhou
 * Created:     2023/10/28
 * Modify:      2024/11/26
 * Locataion:   src/backend/delete.c
 * Description: Delete modeule support delete statment. 
 ********************************************************************************************
 */
#include <stdbool.h>
#include "delete.h"
#include "data.h"
#include "table.h"
#include "free.h"
#include "ltsearch.h"
#include "ltmodify.h"
#include "ltindex.h"
#include "check.h"
#include "trans.h"
#include "xlog.h"
#include "log.h"
#include "instance.h"
#include "optimizer.h"
#include "tuple.h"
#include "heaptable.h"

/* Delete row */
void delete_row(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    Oid oid;
    Table *table;
    Xid created_xid, expired_xid, current_xid;
    Sid sid;
    
    oid = select_result->oid;
    table = open_table_inner(select_result->oid);
    created_xid = TupleFindCreatedXid(tuple, table->meta_table);
    expired_xid = TupleFindExpiredXid(tuple, table->meta_table);
    current_xid = GetCurrentXid();
    sid = TupleGetSysId(tuple, table->meta_table);

    /* Only deal with row that is visible for current transaction. */
    if (IsVisible(created_xid, expired_xid)) {
        void *key, *index;

        /* Decrease row size. */
        select_result->row_size++;

        key = TupleFindKey(tuple, table);
        index = BtreeSearchValue(oid, key);

        Assert(IndexGetSysId(index) == sid);

        /* Delete from index table. */
        BtreeModifyExpiredXid(oid, key, current_xid);

        /* Delete tuple in heap table. */
        HeapTableUpdateRowExpiredXid(table, (Refer *) index, current_xid);
        
        /* Record xlog for delete. */
        RecordXlog(oid, sid, HEAP_DELETE);
    }
}

/* Execute delete statment.*/
void exec_delete_statement(DeleteNode *delete_node, DBResult *result) { 
    /* Check table exists. */
    Table *table = open_table(delete_node->table_name);
    if (table == NULL) {
        logger(ERROR, "Try to open table '%s' fail.", delete_node->table_name);
        return;
    }

    /* Check out delete node. */
    if (!CheckForDelete(delete_node)) 
        return;

    /* Query with condition, and delete satisfied condition row. */
    SelectResult *select_result = new_select_result(DELETE_STMT, delete_node->table_name, true);

    /* Query with condition and delete satisfied row. */
    QueryUnderSearchCondition(
        select_result, 
        SimpleSelectPlan(delete_row, ARG_NULL, NULL, delete_node->condition_node)
    );

    /* Success Result . */
    result->success = true;
    result->rows = select_result->row_size;
    result->message = FormatStr("Successfully deleted %d row data.", select_result->row_size);

    logger(SUCCESS, "Successfully deleted %d row data.", select_result->row_size);
}
