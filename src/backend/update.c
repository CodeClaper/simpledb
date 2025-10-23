/********************************** Update Module ********************************************
 * Auth:        JerryZhou
 * Created:     2023/11/14
 * Modify:      2024/11/26
 * Locataion:   src/backend/update.c
 * Description: Update modeule support Update statment. 
 ********************************************************************************************
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#define _XOPEN_SOURCE
#define __USE_XOPEN
#include <time.h>
#include "update.h"
#include "mmgr.h"
#include "data.h"
#include "meta.h"
#include "select.h"
#include "delete.h"
#include "copy.h"
#include "compare.h"
#include "table.h"
#include "ltsearch.h"
#include "ltmodify.h"
#include "ltinsert.h"
#include "check.h"
#include "free.h"
#include "trans.h"
#include "refer.h"
#include "asserts.h"
#include "session.h"
#include "utils.h"
#include "index.h"
#include "xlog.h"
#include "jsonwriter.h"
#include "log.h"
#include "instance.h"
#include "optimizer.h"
#include "tuple.h"
#include "heaptable.h"
#include "timer.h"

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
        dfree(new_value);
    }
}


/* Delete row for update */
static Refer *DeleteRowForUpdate(Oid oid, void *key) {
    Table *table;
    Refer *refer;
    void *index;
    Xid current_xid;

    table = open_table_inner(oid);
    current_xid = GetCurrentXid();
    index = BtreeSearchValue(oid, key);

    /* Delete from heap table. */
    HeapTableUpdateRowExpiredXid(table, (Refer *) index, current_xid);
    
    /* Delete from btree. */
    refer = BtreeModifyExpiredXid(oid, key, current_xid);

    /* Record xlog. */
    RecordXlog(refer, HEAP_UPDATE_DELETE);

    dfree(index);

    return refer;
}

/* Insert row for update. */
static Refer *ReinsertRowForUpdate(Oid oid, void *key, void *tuple) {
    Table *table;
    Refer *refer, *hrefer;
    int64_t sys_id;
    Xid created_xid, expired_xid;
    void *index;

    table = open_table_inner(oid);
    sys_id = get_timestamp(NANOSECOND);
    created_xid = GetCurrentXid();
    expired_xid = 0;

    TupleSetCreatedXid(tuple, table->meta_table, created_xid);
    TupleSetExpiredXid(tuple, table->meta_table, expired_xid);
    TupleSetSysId(tuple, table->meta_table, sys_id);
    
    /* Reinsert into heap table. */
    hrefer = HeapTableInsertTuple(oid, tuple);

    /* Generate new index. */
    index = dalloc(table->index_value_len);
    IndexSetCreatedXid(index, created_xid);
    IndexSetExpiredXid(index, expired_xid);
    IndexSetSysId(index, sys_id);
    IndexSetRefer(index, hrefer);

    /* Reinsert into btree. */
    refer = BtreeInsert(oid, key, index);

    /* Record xlog for insert. */
    RecordXlog(refer, HEAP_UPDATE_INSERT);

    dfree(index);

    return refer;
}


/* Update row 
 * ----------
 * Update operation is divided into delete and re-insert operation. 
 * It makes transaction rollback operation simpler. */
static void UpdateTuple(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    Oid oid;
    Table *table;
    Refer *oldRefer, *newRefer;
    void *old_key, *new_key, *new_tuple;
    Xid created_xid, expired_xid;
        
    oid = select_result->oid;
    table = open_table_inner(oid);
    created_xid = TupleFindCreatedXid(tuple, table->meta_table);
    expired_xid = TupleFindExpiredXid(tuple, table->meta_table);

    /* Only update row that is visible for current transaction. */
    if (!IsVisible(created_xid, expired_xid)) 
        return;
    
    /* Copy the tuple to avod influenct old data. */
    new_tuple = copy_block(tuple, table->heap_value_len);
    select_result->row_size++;

    /* Get old refer, and lock update refer. */
    old_key = TupleFindKey(tuple, table->meta_table);

    /* Delete row for update. */
    oldRefer = DeleteRowForUpdate(oid, old_key);

    /* Lock refer. */
    add_refer_update_lock(oldRefer);

    /* Update tuple for assignment. */
    Assert(type == ARG_ASSIGNMENT_LIST);
    UpdateTupleForAssignment(new_tuple, (List *) arg, table->meta_table);

    /* Get new key. */
    new_key = TupleFindKey(new_tuple, table->meta_table);

    /* Reinsert row for update. */
    newRefer = ReinsertRowForUpdate(oid, new_key, new_tuple);

    /* Free Update refer lock. */
    free_refer_update_lock(oldRefer);
    
    /* If Refer changed, update refer. */
    if (!refer_equals(oldRefer, newRefer))
        update_refer(oid, 
                     oldRefer->page_num, oldRefer->cell_num, 
                     newRefer->page_num, newRefer->cell_num);

    /* Free memory. */
    free_refer(oldRefer);
    free_refer(newRefer);
}

/* Get SearchConditionNode form WhereClause.. */
static inline SearchConditionNode *WhereClauseFindSearchCondition(WhereClauseNode *where_clause) {
    return where_clause != NULL ? where_clause->condition : NULL;
}

/* Execute update statment. */
void exec_update_statment(UpdateNode *update_node, DBResult *result) {
    Table *table;
    SelectResult *select_result;
    SearchConditionNode *condition_node;

    table = open_table(update_node->table_name);
    /* Check table exists. */
    if (table == NULL)
        db_log(ERROR, "Try to open table '%s' fail.", update_node->table_name);

    /* Check out update node. */
    if (!CheckForUpdate(update_node)) 
        return;

    /* Query with conditon, and update satisfied condition row. */
    select_result = new_select_result(UPDATE_STMT, update_node->table_name, true);
    condition_node = WhereClauseFindSearchCondition(update_node->where_clause);

    /* Query with update row operation. */
    QueryUnderSearchCondition(
        select_result, 
        SimpleSelectPlan(UpdateTuple, ARG_ASSIGNMENT_LIST, update_node->assignment_list, condition_node)
    );
    
    /* Combine the result. */
    result->success = true;
    result->rows = select_result->row_size;
    result->message = FormatStr("Successfully updated %d row data.", result->rows);

    db_log(SUCCESS, "Successfully updated %d row data.", result->rows);
    
    select_result->row_size = 0;
    free_select_result(select_result);
}
