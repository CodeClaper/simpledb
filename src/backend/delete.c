/********************************** Delete Module ********************************************
 * Auth:        JerryZhou
 * Created:     2023/10/28
 * Modify:      2024/11/26
 * Locataion:   src/backend/delete.c
 * Description: Delete modeule support delete statment. 
 ********************************************************************************************
 */
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include "delete.h"
#include "mmgr.h"
#include "data.h"
#include "table.h"
#include "copy.h"
#include "free.h"
#include "select.h"
#include "refer.h"
#include "ltree.h"
#include "check.h"
#include "jsonwriter.h"
#include "trans.h"
#include "session.h"
#include "utils.h"
#include "xlog.h"
#include "log.h"
#include "pager.h"
#include "instance.h"

/* Delete row */
void delete_row(Row *row, SelectResult *select_result, 
                Table *table, ROW_HANDLER_ARG_TYPE type, void *arg) {

    Cursor *cursor;
    Refer *refer;
    Row *currentRow;

    /* Only deal with row that is visible for current transaction. */
    if (RowIsVisible(row)) {

        /* Define the cursor of the row. */
        cursor = define_cursor(table, row->key, true);

        /* Get refer and record xlog. */
        refer = convert_refer(cursor);

        /* Get the current newest row. */
        currentRow = define_row(refer);

        /* Update transaction state. */
        UpdateTransactionState(currentRow, TR_DELETE);

        /* Sync row data */
        update_row_data(currentRow, cursor);

        RecordXlog(refer, HEAP_DELETE);

        select_result->row_size++;

        /* Free memeory. */
        free_cursor(cursor);
        free_refer(refer);
    }
}

/* Execute delete statment.*/
void exec_delete_statement(DeleteNode *delete_node, DBResult *result) { 

    /* Check table exists. */
    Table *table = open_table(delete_node->table_name);
    if (table == NULL) {
        db_log(ERROR, "Try to open table '%s' fail.", delete_node->table_name);
        return;
    }

    /* Check out delete node. */
    if (!check_delete_node(delete_node)) 
        return;

    /* Query with condition, and delete satisfied condition row. */
    SelectResult *select_result = new_select_result(DELETE_STMT, delete_node->table_name);

    /* Query with condition and delete satisfied row. */
    query_with_condition(
        delete_node->condition_node, select_result, 
        delete_row, ARG_NULL, NULL
    );

    /* Success Result . */
    result->success = true;
    result->rows = select_result->row_size;
    result->message = format("Successfully deleted %d row data.", select_result->row_size);

    db_log(SUCCESS, "Successfully deleted %d row data.", select_result->row_size);
}

