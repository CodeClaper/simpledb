/********************************** Update Module ********************************************
 * Auth:        JerryZhou
 * Created:     2023/11/14
 * Modify:      2024/11/26
 * Locataion:   src/backend/update.c
 * Description: Update modeule support Update statment. 
 ********************************************************************************************
 */
#include <stdint.h>
#define _XOPEN_SOURCE
#define __USE_XOPEN
#include "update.h"
#include "mmgr.h"
#include "data.h"
#include "meta.h"
#include "select.h"
#include "insert.h"
#include "delete.h"
#include "copy.h"
#include "compare.h"
#include "table.h"
#include "ltsearch.h"
#include "ltmodify.h"
#include "ltinsert.h"
#include "ltindex.h"
#include "check.h"
#include "free.h"
#include "trans.h"
#include "refer.h"
#include "asserts.h"
#include "session.h"
#include "utils.h"
#include "xlog.h"
#include "jsonwriter.h"
#include "log.h"
#include "instance.h"
#include "optimizer.h"
#include "tuple.h"
#include "systable.h"
#include "heaptable.h"

/* Update tuple for assignment. */
static void UpdateTupleForAssignment(void *tuple, List *assignment_list, MetaTable *meta_table) {
    /* Handle each of assignment. */
    ListCell *lc;
    foreach (lc, assignment_list) {
        AssignmentNode *assign_node;
        MetaColumn *meta_column;
        void *new_value;

        assign_node = (AssignmentNode *) lfirst(lc);
        meta_column = NameFindMetaColumn(meta_table, assign_node->column->column_name);
        new_value = ValueItemNodeAssignValue(assign_node->value, meta_column);

        /* Reset new value to tuple. */
        TupleSetValue(tuple, meta_column, new_value);
        if (new_value) dfree(new_value);
    }
}


/* Delete row for update 
 * ---------------------
 * Will do thoese:
 * (1) Delete heap tuple.
 * (2) Delete index.
 * (3) Record xlog for HEAP_UPDATE_DELETE.
 * */
static void DeleteRowForUpdate(Oid oid, void *key) {
    Table *table;
    Xid current_xid;
    void *value;
    Sid sid;

    table = open_table_inner(oid);
    current_xid = GetCurrentXid();
    value = BtreeSearchValue(oid, key);
    sid = IndexGetSysId(value);

    /* Make heap expired. */
    HeapTableUpdateRowExpiredXid(table, (Refer *)value, current_xid);
    
    /* Make main index expired. */
    BtreeModifyExpiredXid(oid, key, current_xid);

    /* Record xlog. */
    RecordXlog(oid, sid, HEAP_UPDATE_DELETE);
}

/* Insert row for update.
 * ----------------------
 * Will do these:
 * (1) Reinsert the tuple with new ref id.
 * (2) Record xlog for HEAP_UPDATE_INSERT.
 * */
static void ReinsertRowForUpdate(Oid oid, void *key, void *tuple) {
    Table *table;
    Xid created_xid, expired_xid;
    Sid sid;

    table = open_table_inner(oid);
    created_xid = GetCurrentXid();
    expired_xid = 0;
    sid = FindNextOid();

    TupleSetCreatedXid(tuple, table->meta_table, created_xid);
    TupleSetExpiredXid(tuple, table->meta_table, expired_xid);
    TupleSetSysId(tuple, table->meta_table, sid);
    
    /* Reinsert. */
    InsertForTuple(oid, key, tuple);

    /* Record xlog for insert. */
    RecordXlog(oid, sid, HEAP_UPDATE_INSERT);
}


/* Update row 
 * ----------
 * Update operation is divided into delete and re-insert operation. 
 * It makes transaction rollback operation simpler. */
static void UpdateTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    Oid oid;
    Table *table;
    void *old_key, *new_key, *new_tuple;
    Xid created_xid, expired_xid;
        
    oid = select_result->oid;
    table = open_table_inner(oid);
    created_xid = TupleFindCreatedXid(tuple, table->meta_table);
    expired_xid = TupleFindExpiredXid(tuple, table->meta_table);

    /* Only update row that is visible for current transaction. */
    if (!IsVisible(created_xid, expired_xid)) return;
    
    /* Copy the tuple to avod impact the old tuple data. */
    new_tuple = copy_block(tuple, table->heap_value_len);
    select_result->row_size++;

    /* Get old refer, and lock update refer. */
    old_key = TupleFindKey(tuple, table);

    /* Delete row for update. */
    DeleteRowForUpdate(oid, old_key);

    /* Update tuple for assignment. */
    Assert(type == ARG_ASSIGNMENT_LIST);
    UpdateTupleForAssignment(new_tuple, (List *) arg, table->meta_table);

    /* Get new key. */
    new_key = TupleFindKey(new_tuple, table);

    /* Reinsert row for update. */
    ReinsertRowForUpdate(oid, new_key, new_tuple);
}

/* Get SearchConditionNode form WhereClause.. */
static inline SearchConditionNode *WhereClauseFindSearchCondition(WhereClauseNode *where_clause) {
    return where_clause != NULL ? where_clause->condition : NULL;
}

/* Execute update statment. */
void ExecuteUpdateStatement(UpdateNode *update_node, DBResult *result) {
    Table *table;
    SelectResult *select_result;
    SearchConditionNode *condition_node;

    table = open_table(update_node->table_name);
    /* Check table exists. */
    if (table == NULL)
        logger(ERROR, "Try to open table '%s' fail.", 
               update_node->table_name);

    /* Check out update node. */
    if (!CheckForUpdate(update_node)) return;

    /* Query with conditon, and update satisfied condition row. */
    select_result = new_select_result(UPDATE_STMT, update_node->table_name, true);
    condition_node = WhereClauseFindSearchCondition(update_node->where_clause);

    /* Query with update row operation. */
    QueryUnderSearchCondition(
        select_result, 
        SimpleSelectPlan(UpdateTuple, ARG_ASSIGNMENT_LIST, update_node->assignment_list, condition_node)
    );
    
    /* Combine the result. */
    result->rows = select_result->row_size;
    result->success = true;
    result->message = FormatStr("Successfully updated %d row data.", result->rows);

    logger(SUCCESS, "Successfully updated %d row data.", result->rows);
    
    select_result->row_size = 0;
    free_select_result(select_result);
}
