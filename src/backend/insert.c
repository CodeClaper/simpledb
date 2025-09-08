/********************************* Insert Statement Module ************************************
 * Auth:        JerryZhou
 * Created:     2023/08/22
 * Modify:      2024/09/13
 * Locataion:   src/backend/insert.c
 * Description: Insert modeule support insert statment. 
 * (1) Plain insert values statment, includes all column or special part column.
 * (2) Insert with subselect statment.
 *********************************************************************************************/
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#define _XOPEN_SOURCE
#define __USE_XOPEN
#include <time.h>
#include "insert.h"
#include "mmgr.h"
#include "common.h"
#include "table.h"
#include "meta.h"
#include "ltree.h"
#include "pager.h"
#include "index.h"
#include "asserts.h"
#include "session.h"
#include "create.h"
#include "select.h"
#include "check.h"
#include "copy.h"
#include "free.h"
#include "refer.h"
#include "trans.h"
#include "timer.h"
#include "xlog.h"
#include "log.h"
#include "utils.h"
#include "row.h"
#include "rowlock.h"
#include "jsonwriter.h"
#include "instance.h"
#include "strheaptable.h"

/* Get value in insert node to assign column at index. */
static void *GetInsertValue(List *value_item_list, uint32_t index, MetaColumn *meta_column) {
    /* Not out of boundary. */
    Assert(index < len_list(value_item_list));
    /* Get value item node at index. */
    ValueItemNode* value_item_node = lfirst(list_nth_cell(value_item_list, index));
    return ValueItemNodeAssignValue(value_item_node, meta_column);
}

/* Fake ValuesOrQuerySpecNode for VALUES type. */
static ValuesOrQuerySpecNode *GenerateValuesOrQuerySpc(List *value_list) {
    ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
    values_or_query_spec->type = VQ_VALUES;
    values_or_query_spec->values = create_list(NODE_LIST);
    append_list(values_or_query_spec->values, list_copy_deep(value_list));
    return values_or_query_spec;
}

/* Make a fake InsertNode. */
InsertNode *GenerateInsertNode(char *table_name, List *value_list) {
    InsertNode *insert_node = instance(InsertNode);
    insert_node->table_name = dstrdup(table_name);
    insert_node->all_column = true;
    insert_node->values_or_query_spec = GenerateValuesOrQuerySpc(value_list);
    return insert_node;
}

/* Convert QuerySpecNode to SelectionNode. 
 * Notice: not need to free selection, table_exp in select_node. */
static SelectNode *QuerySpceToSelection(QuerySpecNode *query_spec) {
    SelectNode *select_node = instance(SelectNode);
    select_node->selection = query_spec->selection;
    select_node->table_exp = query_spec->table_exp;
    return select_node;
}

/* Generate new sys_id column.*/
static KeyValue *NewSysIdKeyValue(char *table_name) {
    /* Automatically insert sys_id using current sys time. */
    int64_t sys_id = get_timestamp(NANOSECOND);
    return new_key_value(SYS_RESERVED_ID_COLUMN_NAME, &sys_id, T_LONG, table_name);
}

/* Generate new created_xid column.*/
static KeyValue *NewCreatedXidKeyValue(char *table_name) {
    /* Get current transaction. */
    TransEntry *current_trans = FindTransaction();
    Assert(current_trans);
    return new_key_value(CREATED_XID_COLUMN_NAME, &current_trans->xid, T_LONG, table_name);
}

/* Generate new expired_xid column. */
static KeyValue *NewExpiredXidKeyValue(char *table_name) {
    /* For expired_xid */
    int64_t zero = 0;
    return new_key_value(EXPIRED_XID_COLUMN_NAME, &zero, T_LONG, table_name);
}

/* Makeup the system reserved column. */
void makeup_reserved_columns(Row *row, char *table_name) {
    /* Append sys_id column key value. */
    append_list(row->data, NewSysIdKeyValue(table_name));
    /* Append created_xid column key value. */
    append_list(row->data, NewCreatedXidKeyValue(table_name));
    /* Append expired_xid column key value. */
    append_list(row->data, NewExpiredXidKeyValue(table_name));
}


/* Generate insert row for all columns. 
 * Return Row. */
static Row *GenerateInsertRowForAllInner(MetaTable *meta_table, List *value_item_list) {
    /* Check NodeType. */
    Assert(value_item_list->type == NODE_VALUE_ITEM);

    /* Instance row. */
    Row *row = NewRow();
    
    /* Row data. */
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        /* Ship system reserved. */
        if (meta_column->sys_reserved) 
            continue;

        KeyValue *key_value = new_key_value(meta_column->column_name, 
                                            NULL,
                                            meta_column->column_type, 
                                            meta_table->table_name);

        /* Maybe array value, and the funciton <copy_value> 
         * not support ArrayValue, so specially assign here.*/
        key_value->value = GetInsertValue(value_item_list, __i, meta_column);

        append_list(row->data, key_value);
    }

    /* Make up the reserved columns. */
    makeup_reserved_columns(row, meta_table->table_name);
    
    return row;
}

/* Generate insert row for all columns. 
 * Return list of Row.
 * */
static List *GenerateInsertRowForAll(InsertNode *insert_node) {
    List *value_list = insert_node->values_or_query_spec->values;

    /* Table and MetaTable. */
    Table *table = open_table(insert_node->table_name);
    if (table == NULL) {
        db_log(ERROR, "Try to open table '%s' fail.", insert_node->table_name);
        return NULL;
    }

    MetaTable *meta_table = table->meta_table;

    List *row_list = create_list(NODE_ROW);

    ListCell *lc;
    foreach (lc, value_list) {
        List *value_item_list = lfirst(lc);
        Row *row = GenerateInsertRowForAllInner(meta_table, value_item_list);
        append_list(row_list, row);
    }
    
    return row_list;
}


/* Generate insert row for part columns.
 * -------------------------------------
 * Return a Row which need to be freed by caller.
 * */
static Row *GenerateInsertRowForPartInner(MetaTable *meta_table, List *column_list, List *value_item_list) {
    /* Instance row. */
    Row *row = NewRow();
    
    /* Row data. */
    ListCell *lc;
    foreach (lc, column_list) {
        ColumnNode *column = lfirst(lc);
        MetaColumn *meta_column = NameFindMetaColumn(meta_table, column->column_name);

        if (!meta_table)
            db_log(ERROR, "Not found column '%s' in table '%s'.",
                   column->column_name,
                   meta_table->table_name);

        KeyValue *key_value = new_key_value(meta_column->column_name, 
                                            NULL,
                                            meta_column->column_type, 
                                            meta_table->table_name);

        /* Maybe the value is array value, and the funciton <copy_value> 
         * not support ArrayValue, so specially assign here.*/
        key_value->value = GetInsertValue(value_item_list, __i, meta_column);

        /* Value of KeyValue may be null when it is Refer. */
        if (key_value->data_type == T_REFERENCE && key_value->value == NULL)
            return NULL;

        append_list(row->data, key_value);
    }

    /* Make up the reserved columns. */
    makeup_reserved_columns(row, meta_table->table_name);

    return row;
}

/* Generate insert row for part columns.
 * Return list of Row.
 * */
static List *GenerateInsertRowForPart(InsertNode *insert_node) {
    List *column_list = insert_node->column_list;
    List *value_list = insert_node->values_or_query_spec->values;

    /* Table and MetaTable. */
    Table *table = open_table(insert_node->table_name);
    if (table == NULL) {
        db_log(ERROR, "Try to open table '%s' fail.", insert_node->table_name);
        return NULL;
    }

    MetaTable *meta_table = table->meta_table;
    List *row_list = create_list(NODE_ROW);

    ListCell *lc;
    foreach (lc, value_list) {
        List *value_item_list = lfirst(lc);
        Row *row = GenerateInsertRowForPartInner(meta_table, column_list, value_item_list);
        append_list(row_list, row);
    }

    return row_list;
}

/* Generate insert row. 
 * Return list of Row. */
static List *GenerateInsertRow(InsertNode *insert_node) {
    /* Check only for VQ_VALUES. */
    Assert(insert_node->values_or_query_spec->type == VQ_VALUES);
    return insert_node->all_column 
            ? GenerateInsertRowForAll(insert_node)
            : GenerateInsertRowForPart(insert_node);
}

/* Convert to insert row. */
static Row *SelectRowToInsertRow(Row *select_row, Table *table) {
    Row *insert_row = NewRow();

    /* Copy data. */
    ListCell *lc;
    foreach (lc, select_row->data) {
        KeyValue *current = (KeyValue *)lfirst(lc);
        KeyValue *key_value = new_key_value(current->key, 
                                            current->value, 
                                            current->data_type, 
                                            GET_TABLE_NAME(table));
        append_list(insert_row->data, key_value);
    }

    /* Make up the reserved columns. */
    makeup_reserved_columns(insert_row, GET_TABLE_NAME(table));

    return insert_row;
}

/* Insert one row. 
 * ---------------
 * Return the row refer, 
 * Throw error by log if fail. */
Refer *insert_one_row(Table *table, Row *row) {
    MetaColumn *primary_key_meta_column = MetaTableFindPrimaryKey(table->meta_table);
    Assert(primary_key_meta_column);
    
    void *key = RowFindKey(row, table->meta_table);
    Assert(key != NULL);
    Refer *refer = define_refer(table, key);
    
    /* Acquire the row lock. Maybe block here. */
    AcquireRowLock(GET_TABLE_OID(table), key);

    /* Check if duplicate key. */
    if (UserPrimaryKeyExists(table->meta_table) && 
        check_duplicate_key(key, refer) && 
        !refer_is_deleted(refer)
    ) {
        char *keyStr = primary_key_meta_column->column_type == T_STRING
                ? QueryStringValue(key)
                : get_key_str(key, primary_key_meta_column->column_type);
        db_log(ERROR, "key '%s' in table '%s' already exists, not allow duplicate key.", 
               keyStr, GET_TABLE_NAME(table));
        return NULL;
    }

    /* Insert into leaf node. */
    insert_row_data(row, refer);

    /* Record xlog for insert operation. */
    RecordXlog(refer, HEAP_INSERT);

    return refer;    
}

/* Insert for values case. 
 * ----------------------
 * Return list of Refer. */
List *insert_for_values(InsertNode *insert_node) {
    Table *table;
    List *row_list, *refer_list;

    table = open_table(insert_node->table_name);
    Assert(table);
    
    /* Generate insert row. */
    row_list = GenerateInsertRow(insert_node);
    AssertFalse(list_empty(row_list));

    /* Create refer list. */
    refer_list = create_list(NODE_REFER);

    /* Insert to page. */
    ListCell *lc;
    foreach (lc, row_list) {
        Row *row = lfirst(lc);
        Refer *refer = insert_one_row(table, row);
        append_list(refer_list, refer);
    }

    /* Free refer list. */
    free_list_deep(row_list);

    return refer_list;
}

/* Insert for query spec case. */
static List *insert_for_query_spec(InsertNode *insert_node) {
    /* Check if table exists. */
    Table *table = open_table(insert_node->table_name);
    if (!table) {
        db_log(ERROR, "Try to open table '%s' fail.", insert_node->table_name);
        return NULL;
    }
    
    List *list = create_list(NODE_REFER);

    ValuesOrQuerySpecNode *values_or_query_spec = insert_node->values_or_query_spec;

    /* Make select statement to get safisfied rows. */
    SelectNode *select_node = QuerySpceToSelection(values_or_query_spec->query_spec);

    /* Make a DBResult to store query result. */
    DBResult *result = new_db_result();
    result->stmt_type = INSERT_STMT;

    exec_select_statement(select_node, result);

    if (result->success) {
        SelectResult *select_result = (SelectResult *)result->data;

        /* Insert into rows. */
        QueueCell *qc;
        qforeach (qc, select_result->rows) {
            Row *insert_row = SelectRowToInsertRow((Row *)qfirst(qc), table);
            Refer *refer = insert_one_row(table, insert_row);
            append_list(list, refer);
            free_row(insert_row);
        }
    }

    dfree(select_node);
    return list;
}

/* Execute insert statement. 
 * --------------------------
 * Return Refer List if it excutes successfully,
 * otherwise, return NULL. */
List *exec_insert_statement(InsertNode *insert_node) {
    /* Check if insert node valid. */
    if (!CheckForInsert(insert_node)) 
        return NULL;

    ValuesOrQuerySpecNode *values_or_query_spec = insert_node->values_or_query_spec;

    switch (values_or_query_spec->type) {
        case VQ_VALUES: {
            /* Insert with values. */
            return insert_for_values(insert_node);
        }
        case VQ_QUERY_SPEC: {
            /* For query spec, there is no refer. 
             * Note, maybe used in multi-values which will be supported. */
            return insert_for_query_spec(insert_node);
        }
        default: {
            db_log(ERROR, "Unknown ValuesOrQuerySpecNode type.");
            return NULL;
        }
    }
}

