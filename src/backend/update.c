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
#include "row.h"
#include "select.h"
#include "delete.h"
#include "insert.h"
#include "copy.h"
#include "compare.h"
#include "table.h"
#include "pager.h"
#include "ltree.h"
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

/* Update cell */
static void UpdateCell(Row *row, AssignmentNode *assign_node, MetaColumn *meta_column) {
    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (StrEq(key_value->key, assign_node->column->column_name)) {
            ValueItemNode *value_item = assign_node->value;
            key_value->value = ValueItemNodeAssignValue(value_item, meta_column);
        }
    } 
}

/* Delete row for update */
static void DeleteRowForUpdate(Refer *refer, Row *row) {
    if (RowIsVisible(row)) {
        UpdateTransactionState(row, TR_DELETE);
        update_row_data(row, refer);
        RecordXlog(refer, HEAP_UPDATE_DELETE);
    }
}

/* Insert row for update. */
static void InsertRowForUpdate(Row *row, Table *table) {
    void *key;
    Refer *nrefer;

    key = RowFindKey(row, table->meta_table);
    nrefer = define_refer(table, key);

    /* Update old row. */
    UpdateTransactionState(row, TR_INSERT);

    /* Insert */
    insert_row_data(row, nrefer);

    /* Record xlog for insert. */
    RecordXlog(nrefer, HEAP_UPDATE_INSERT);

    free_refer(nrefer);
}


/* Update row 
 * ----------
 * Update operation is divided into delete and re-insert operation. 
 * It makes transaction roll back simpler. */
static void UpdateTuple(void *tuple, SelectResult *select_result, 
                       ROW_HANDLER_ARG_TYPE type, void *arg) {
    Table *table;
    Refer *oldRefer, *newRefer;
    Row *rawRow, *currentRow, *new_row;
    void *old_key, *new_key;
        
    table = open_table_inner(select_result->oid);
    rawRow = GenerateRow(tuple, table->meta_table);
    /* Only update row that is visible for current transaction. */
    if (!RowIsVisible(rawRow)) 
        return;

    select_result->row_size++;

    /* Get old refer, and lock update refer. */
    old_key = RowFindKey(rawRow, table->meta_table);
    oldRefer = define_refer(table, old_key);
    add_refer_update_lock(oldRefer);
    currentRow = DefineRow(oldRefer);

    /* Delete row for update. */
    DeleteRowForUpdate(oldRefer, currentRow);

    new_row = copy_row(currentRow);

    /* For update row funciton, the arg is the List of Assignment. */
    Assert(type == ARG_ASSIGNMENT_LIST);
    List *assignment_list = (List *) arg;

    /* Handle each of assignment. */
    ListCell *lc;
    foreach (lc, assignment_list) {
        AssignmentNode *assign_node = lfirst(lc);
        MetaColumn *meta_column = NameFindMetaColumn(table->meta_table, assign_node->column->column_name);
        UpdateCell(new_row, assign_node, meta_column);
    }
   
    /* Insert row for update. */
    InsertRowForUpdate(new_row, table);

    /* Recalculate Refer, because afer insert, row refer may be changed. */
    new_key = RowFindKey(new_row, table->meta_table);
    newRefer = define_refer(table, new_key);

    /* Free Update refer lock. */
    free_refer_update_lock(oldRefer);
    
    /* If Refer changed, update refer. */
    if (!refer_equals(oldRefer, newRefer)) {
        ReferUpdateEntity *refer_update_entity = new_refer_update_entity(oldRefer, newRefer);
        update_related_tables_refer(refer_update_entity);
        free_refer_update_entity(refer_update_entity);
    } else {
        free_refer(oldRefer);
        free_refer(newRefer);
    }
}

/* Get SearchConditionNode form WhereClause.. */
static SearchConditionNode *WhereClauseFindSearchCondition(WhereClauseNode *where_clause) {
    if (where_clause)
        return where_clause->condition;
    else
        return NULL;
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
    if (!check_update_node(update_node)) 
        return;

    /* Query with conditon, and update satisfied condition row. */
    select_result = new_select_result(UPDATE_STMT, update_node->table_name, true);
    condition_node = WhereClauseFindSearchCondition(update_node->where_clause);

    /* Query with update row operation. */
    QueryUnderSearchCondition(
        condition_node, select_result, 
        SimpleSelectPlan(UpdateTuple, ARG_ASSIGNMENT_LIST, update_node->assignment_list)
    );
    
    /* Combine the result. */
    result->success = true;
    result->rows = select_result->row_size;
    result->message = FormatStr("Successfully updated %d row data.", result->rows);

    db_log(SUCCESS, "Successfully updated %d row data.", result->rows);
    
    select_result->row_size = 0;
    free_select_result(select_result);
}
