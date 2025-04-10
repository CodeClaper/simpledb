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

/* Update cell */
static void update_cell(Row *row, AssignmentNode *assign_node, MetaColumn *meta_column) {
    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (streq(key_value->key, assign_node->column->column_name)) {
            ValueItemNode *value_item = assign_node->value;
            key_value->value = assign_value_from_value_item_node(value_item, meta_column);
        }
    } 
}

/* Delete row for update */
static void delete_row_for_update(Row *row, Table *table) {
    if (RowIsVisible(row)) {
        Cursor *cursor = define_cursor(table, row->key, true);
        Refer *refer = convert_refer(cursor);

        UpdateTransactionState(row, TR_DELETE);
        // update_row_data(row, cursor);
        RecordXlog(refer, HEAP_UPDATE_DELETE);

        free_cursor(cursor);
        free_refer(refer);
    }
}

/* Insert row for update. */
static void insert_row_for_update(Row *row, Table *table) {
    Cursor *new_cur = define_cursor(table, row->key, true);
    UpdateTransactionState(row, TR_INSERT);

    /* Insert */
    insert_leaf_node_cell(new_cur, row);
    Refer *new_ref = convert_refer(new_cur);
    /* Record xlog for insert. */
    RecordXlog(new_ref, HEAP_UPDATE_INSERT);

    free_cursor(new_cur);
    free_refer(new_ref);
}


/* Update row 
 * Update operation can be regarded as delete + re-insert operation. 
 * It makes transaction roll back simpler. */
static void update_row(Row *rawRow, SelectResult *select_result, Table *table, 
                       ROW_HANDLER_ARG_TYPE type, void *arg) {
    /* Only update row that is visible for current transaction. */
    if (!RowIsVisible(rawRow)) 
        return;

    select_result->row_size++;

    /* Get old refer, and lock update refer. */
    Refer *old_refer = define_refer(rawRow);
    add_refer_update_lock(old_refer);
    Row *currentRow = define_row(old_refer);

    /* Delete row for update. */
    delete_row_for_update(currentRow, table);

    Row* new_row = copy_row(currentRow);

    /* For update row funciton, the arg is the List of Assignment. */
    Assert(type == ARG_ASSIGNMENT_LIST);
    List *assignment_list = (List *) arg;

    /* Handle each of assignment. */
    ListCell *lc;
    foreach (lc, assignment_list) {
        AssignmentNode *assign_node = lfirst(lc);
        MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, assign_node->column->column_name);
        update_cell(new_row, assign_node, meta_column);
        if (meta_column->is_primary) { 
            /* If primary key changed, reassign new value. */
            new_row->key = get_value_from_value_item_node(assign_node->value, meta_column);
        }
    }
   
    /* Insert row for update. */
    insert_row_for_update(new_row, table);

    /* Recalculate Refer, because afer insert, row refer may be changed. */
    Refer *new_refer = define_refer(new_row);

    /* Free Update refer lock. */
    free_refer_update_lock(old_refer);
    
    /* If Refer changed, update refer. */
    if (!refer_equals(old_refer, new_refer)) {
        ReferUpdateEntity *refer_update_entity = new_refer_update_entity(old_refer, new_refer);
        update_related_tables_refer(refer_update_entity);
        free_refer_update_entity(refer_update_entity);
    }
}

/* Get ConditionNode form WhereClause.. */
static ConditionNode *get_condition_from_where(WhereClauseNode *where_clause) {
    if (where_clause)
        return where_clause->condition;
    else
        return NULL;
}

/* Execute update statment. */
void exec_update_statment(UpdateNode *update_node, DBResult *result) {

    /* Check table exists. */
    Table *table = open_table(update_node->table_name);
    if (table == NULL) {
        db_log(ERROR, "Try to open table '%s' fail.", update_node->table_name);
        return;
    }

    /* Check out update node. */
    if (!check_update_node(update_node)) 
        return;

    /* Query with conditon, and update satisfied condition row. */
    SelectResult *select_result = new_select_result(UPDATE_STMT, update_node->table_name);
    ConditionNode *condition_node = get_condition_from_where(update_node->where_clause);

    /* Query with update row operation. */
    query_with_condition(condition_node, select_result, update_row, 
                         ARG_ASSIGNMENT_LIST, update_node->assignment_list);
    result->success = true;
    result->rows = select_result->row_size;
    result->message = format("Successfully updated %d row data.", result->rows);

    db_log(SUCCESS, "Successfully updated %d row data.", result->rows);
    
    select_result->row_size = 0;
    free_select_result(select_result);
}
