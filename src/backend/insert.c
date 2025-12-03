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
#include "ltbase.h"
#include "ltsearch.h"
#include "ltindex.h"
#include "pager.h"
#include "asserts.h"
#include "session.h"
#include "create.h"
#include "select.h"
#include "check.h"
#include "copy.h"
#include "free.h"
#include "refer.h"
#include "trans.h"
#include "xlog.h"
#include "log.h"
#include "utils.h"
#include "row.h"
#include "tuple.h"
#include "rowlock.h"
#include "jsonwriter.h"
#include "instance.h"
#include "systable.h"
#include "strheaptable.h"
#include "heaptable.h"
#include "ltinsert.h"
#include "ridinsert.h"
#include "index.h"
#include "bufpool.h"

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
static KeyValue *NewKeyValueForSysId(Oid tid) {
    /* Automatically insert sys_id using current sys time. */
    int64_t sys_id = FindNextOid();
    return new_key_value(SYS_RESERVED_ID_COLUMN_NAME, &sys_id, T_LONG, tid);
}

/* Generate new ref_id column. */
static KeyValue *NewKeyValueForRefId(Oid tid) {
    /* Automatically insert sys_id using current sys time. */
    int64_t ref_id = FindNextOid();
    return new_key_value(SYS_REF_ID_COLUMN_NAME, &ref_id, T_LONG, tid);
}

/* Generate new created_xid column.*/
static KeyValue *NewKeyValueForCreatedXid(Oid tid) {
    /* Get current transaction. */
    TransEntry *current_trans = FindTransaction();
    Assert(current_trans);
    return new_key_value(CREATED_XID_COLUMN_NAME, &current_trans->xid, T_LONG, tid);
}

/* Generate new expired_xid column. */
static KeyValue *NewKeyValueForExpiredXid(Oid tid) {
    /* For expired_xid */
    int64_t zero = 0;
    return new_key_value(EXPIRED_XID_COLUMN_NAME, &zero, T_LONG, tid);
}

/* Makeup the system reserved column. */
void MakeupReservedColumns(Oid tid, Row *row) {
    /* Append sys_id column key value. */
    append_list(row->data, NewKeyValueForSysId(tid));
    /* Append ref_id column key value. */
    append_list(row->data, NewKeyValueForRefId(tid));
    /* Append created_xid column key value. */
    append_list(row->data, NewKeyValueForCreatedXid(tid));
    /* Append expired_xid column key value. */
    append_list(row->data, NewKeyValueForExpiredXid(tid));
}


/* Generate insert row for all columns. 
 * Return Row. */
static Row *GenerateInsertRowForAllInner(Oid tid, MetaTable *meta_table, List *value_item_list) {
    /* Check NodeType. */
    Assert(value_item_list->type == NODE_VALUE_ITEM);

    /* Instance row. */
    Row *row = NewRow();
    
    /* Row data. */
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        /* Ship system reserved. */
        if (meta_column->sys_reserved) continue;
        KeyValue *key_value = new_key_value(meta_column->column_name, NULL,
                                            meta_column->column_type, tid);
        /* Maybe array value, and the funciton <copy_value> 
         * not support ArrayValue, so specially assign here.*/
        key_value->value = GetInsertValue(value_item_list, __i, meta_column);
        append_list(row->data, key_value);
    }

    /* Make up the reserved columns. */
    MakeupReservedColumns(tid, row);
    
    return row;
}

/* Generate insert row for all columns. 
 * Return list of Row.
 * */
static List *GenerateInsertRowForAll(InsertNode *insert_node) {
    Table *table;
    MetaTable *meta_table;
    List *value_list, *row_list;

    /* Table and MetaTable. */
    table = open_table(insert_node->table_name);
    if (table == NULL) {
        db_log(ERROR, "Try to open table '%s' fail.", insert_node->table_name);
        return NULL;
    }

    value_list = insert_node->values_or_query_spec->values;
    row_list = create_list(NODE_ROW);
    meta_table = table->meta_table;

    ListCell *lc;
    foreach (lc, value_list) {
        List *value_item_list = lfirst(lc);
        Row *row = GenerateInsertRowForAllInner(GET_TABLE_OID(table), meta_table, value_item_list);
        append_list(row_list, row);
    }
    
    return row_list;
}


/* Generate insert row for part columns.
 * -------------------------------------
 * Return a Row which need to be freed by caller.
 * */
static Row *GenerateInsertRowForPartInner(Oid tid, MetaTable *meta_table, List *column_list, List *value_item_list) {
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
                                            tid);

        /* Maybe the value is array value, and the funciton <copy_value> 
         * not support ArrayValue, so specially assign here.*/
        key_value->value = GetInsertValue(value_item_list, __i, meta_column);

        /* Value of KeyValue may be null when it is Refer. */
        if (key_value->data_type == T_REFERENCE && key_value->value == NULL)
            return NULL;

        append_list(row->data, key_value);
    }

    /* Make up the reserved columns. */
    MakeupReservedColumns(tid, row);

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
        Row *row = GenerateInsertRowForPartInner(GET_TABLE_OID(table), meta_table, column_list, value_item_list);
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
                                            GET_TABLE_OID(table));
        append_list(insert_row->data, key_value);
    }

    /* Make up the reserved columns. */
    MakeupReservedColumns(GET_TABLE_OID(table), insert_row);

    return insert_row;
}

/* Wait for duplicate key to release.
 * ---------------------------------
 * If there is duplicate key, the current transaction will
 * wait for the other transaction who hold the duplicate key to release. 
 * */
static bool WaitForDuplicateKeyRelease(Refer *refer) {
    bool flag = false;
    Buffer buffer;
    Table *table;
    void *leaf_node;
    uint32_t key_len, value_len, default_value_len;
    Xid current_xid, created_xid, expired_xid;

    table = open_table_inner(refer->oid);
    key_len = table->key_len; 
    value_len = table->index_value_len; 
    default_value_len = table->heap_value_len;
    current_xid = GetCurrentXid();

retry:
    /* Get the leaf node buffer. */
    buffer = ReadBuffer(refer->oid, refer->page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPageCopy(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);

    created_xid = LeafNodeGetCellCreatedXid(leaf_node, key_len, value_len, default_value_len, refer->cell_num);
    expired_xid = LeafNodeGetCellExpiredXid(leaf_node, key_len, value_len, default_value_len, refer->cell_num);
    Assert(created_xid != 0);
    
    if (expired_xid == 0) {
        /* Block here until the created transaction fade out. */
        while (current_xid != created_xid && IsActive(created_xid)) {
            while (IsActive(created_xid)) {
                lock_sleep(DEFAULT_SPIN_INTERVAL);
            }
            /* Retry is necessary for scene when the same 
             * transaction creates a row and then deletes it. */
            goto retry;
        }
        flag = true;
    } else {
        /* Block here until the expired transaction fade out. */
        while (current_xid != expired_xid && IsActive(expired_xid)) {
            lock_sleep(DEFAULT_SPIN_INTERVAL);
        }
        flag = false;
    }

    dfree(leaf_node);
    return flag;
}

/* 
 * Insert for tuple.
 * -------------------
 * As follow, we will do those:
 * (1) Insert tuple to heap table.
 * (2) Insert index to main index table.
 * (3) Insert index to rid index table.
 * (4) Insert index to other index table.
 * */
Rid InsertForTuple(Oid oid, void *key, void *tuple) {
    Table *table;
    Refer *refer;
    void *value;
    Rid ref_id;

    table = open_table_inner(oid);

    /* Insert tuple to heap table.*/
    refer = HeapTableInsertTuple(oid, tuple);

    /* Insert index to main index table. */
    value = GenerateIndex(oid, tuple, refer);
    BtreeInsert(oid, key, value);

    /* Insert index to rid index table. */
    ref_id = TupleGetRefId(tuple, table->meta_table);
    RidInsert(table->roid, ref_id, refer);

    /* Insert index to other index table. */
    ListCell *lc;
    foreach (lc, table->meta_indexs) {
        MetaIndex *meta_index = (MetaIndex *) lfirst(lc);
        /* Skip primary index. */
        if (meta_index->is_pri) continue;
        IndexInsert(meta_index, tuple, refer);
    }

    return TupleGetRefId(tuple, table->meta_table);
}


/* Insert one row. 
 * ---------------
 * Return the row refer, 
 * Throw error by log if fail. */
Rid InsertForRow(Table *table, Row *row) {
    Oid oid;
    DataType ptype;
    void *key, *tuple;
    Refer *preRefer;
    Rid ref_id;

    oid = GET_TABLE_OID(table);
    ptype = MetaTableFindPrimaryDataType(table->meta_table);
    
    key = RowFindKey(row, table);
    Assert(key != NULL);
    preRefer = BtreeSearchRefer(oid, key);
    
    /* Check if duplicate key. */
    if (UserPrimaryKeyExists(table->meta_table) && 
        IndexDuplicateKeyCheck(key, preRefer) && 
        WaitForDuplicateKeyRelease(preRefer)
    ) {
        db_log(ERROR, "key '%s' in table '%s' already exists, not allow duplicate key.", 
               KeyGetSysStrValue(key, ptype), GET_TABLE_NAME(table));
        return RID_ZERO;
    }

    tuple = RowSeriableTuple(row, table);
    ref_id = InsertForTuple(oid, key, tuple);

    /* Record xlog for insert operation. */
    RecordXlog(oid, ref_id, HEAP_INSERT);

    return ref_id;    
}

/* Insert for values case. 
 * ----------------------
 * Return list of RID. */
List *InsertForValues(InsertNode *insert_node) {
    Table *table;
    List *row_list, *ref_ids;

    table = open_table(insert_node->table_name);
    Assert(table);
    
    /* Generate insert row. */
    row_list = GenerateInsertRow(insert_node);
    AssertFalse(list_empty(row_list));

    /* Create rid list. */
    ref_ids = create_list(NODE_LONG);

    /* Insert to page. */
    ListCell *lc;
    foreach (lc, row_list) {
        Row *row = lfirst(lc);
        Rid ref_id = InsertForRow(table, row);
        append_list_long(ref_ids, ref_id);
    }

    /* Free refer list. */
    free_list_deep(row_list);

    return ref_ids;
}

/* Insert for query spec case. */
static List *InsertForQuerySpec(InsertNode *insert_node) {
    /* Check if table exists. */
    Table *table = open_table(insert_node->table_name);
    if (!table) {
        db_log(ERROR, "Try to open table '%s' fail.", insert_node->table_name);
        return NULL;
    }
    
    List *list = create_list(NODE_LONG);

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
            Rid ref_id = InsertForRow(table, insert_row);
            append_list_long(list, ref_id);
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
List *ExecuteInsertStatement(InsertNode *insert_node) {
    /* Check if insert node valid. */
    if (!CheckForInsert(insert_node)) 
        return NULL;

    ValuesOrQuerySpecNode *values_or_query_spec = insert_node->values_or_query_spec;

    switch (values_or_query_spec->type) {
        case VQ_VALUES: {
            /* Insert with values. */
            return InsertForValues(insert_node);
        }
        case VQ_QUERY_SPEC: {
            /* For query spec, there is no refer. 
             * Note, maybe used in multi-values which will be supported. */
            return InsertForQuerySpec(insert_node);
        }
        default: {
            db_log(ERROR, "Unknown ValuesOrQuerySpecNode type.");
            return NULL;
        }
    }
}

