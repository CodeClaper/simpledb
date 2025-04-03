#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "table.h"
#include "copy.h"
#include "free.h"
#include "data.h"
#include "index.h"
#include "meta.h"
#include "log.h"
#include "asserts.h"
#include "common.h"
#include "mmgr.h"
#include "utils.h"

/* Copy value. */
void *copy_value(void *value, DataType data_type) {
    if (!value)
        return NULL;

    switch (data_type) {
        case T_BOOL: {
            bool *new_val = instance(bool);
            memcpy(new_val, value, sizeof(bool));
            return new_val;
        }
        case T_INT: {
            int32_t *new_val = instance(int32_t);
            memcpy(new_val, value, sizeof(int32_t));
            return new_val;
        }
        case T_LONG: {
            int64_t *new_val = instance(int64_t);
            memcpy(new_val, value, sizeof(int64_t));
            return new_val;
        }
        case T_FLOAT: {
            float *new_val = instance(float);
            memcpy(new_val, value, sizeof(float));
            return new_val;
        }
        case T_DOUBLE: {
            double *new_val = instance(double);
            memcpy(new_val, value, sizeof(double));
            return new_val;
        }
        case T_DATE:
        case T_TIMESTAMP: {
            time_t *new_val = instance(time_t);
            memcpy(new_val, value, sizeof(time_t));
            return new_val;
        }
        case T_CHAR:
        case T_STRING: 
        case T_VARCHAR:
            return dstrdup((char *)value);
        case T_REFERENCE: 
            return copy_refer(value);
        default: {
            UNEXPECTED_VALUE("Not supported data type occurs at <copy_value>.");
            return NULL;
        }
    }    
}


/* Copy value. */
void *copy_value2(void *value, MetaColumn *meta_column) {
    if (!value)
        return NULL;

    void *new_val = dalloc(meta_column->column_length);
    switch (meta_column->column_type) {
        case T_BOOL: {
            memcpy(new_val, value, sizeof(bool));
            return new_val;
        }
        case T_INT: {
            memcpy(new_val, value, sizeof(int32_t));
            return new_val;
        }
        case T_LONG: {
            memcpy(new_val, value, sizeof(int64_t));
            return new_val;
        }
        case T_FLOAT: {
            memcpy(new_val, value, sizeof(float));
            return new_val;
        }
        case T_DOUBLE: {
            memcpy(new_val, value, sizeof(double));
            return new_val;
        }
        case T_DATE:
        case T_TIMESTAMP: {
            memcpy(new_val, value, sizeof(time_t));
            return new_val;
        }
        case T_CHAR:
        case T_STRING: 
        case T_VARCHAR: {
            memcpy(new_val, value, strlen(value));
            return new_val;
        }
        case T_REFERENCE: 
            return copy_refer(value);
        default: {
            UNEXPECTED_VALUE("Not supported data type occurs at <copy_value>.");
            return NULL;
        }
    }    
}

/* Copy Key value pair. */
KeyValue *copy_key_value(KeyValue *key_value) {
    if (!key_value)
        return NULL;

    /* copy key value */
    KeyValue *key_value_copy = instance(KeyValue);

    key_value_copy->key = dstrdup(key_value->key);
    /* Meta column may be null, in fact, for key aggregate function, 
     * key is min, max, sum, avg ect. there is no meta column. */
    key_value_copy->data_type = key_value->data_type;
    key_value_copy->is_array = key_value->is_array;
    /* Single and array data have difference way to deal. */
    if (key_value_copy->is_array)
        key_value_copy->value = copy_array_value(key_value->value);
    else
        key_value_copy->value = copy_value(key_value->value, key_value->data_type);
    key_value_copy->table_name = dstrdup(key_value->table_name);

    return key_value_copy;
}

/* Copy row. */
Row *copy_row(Row *row) {
    /* check */
    if (row == NULL)
        return NULL;
    Table *table = open_table(row->table_name);
    if (table == NULL) {
        db_log(ERROR, "Table '%s' not exists. ", row->table_name);
        return NULL;
    }

    /* copy row */
    Row *row_copy = instance(Row);
    MetaColumn *primary_meta_column = get_primary_key_meta_column(table->meta_table);
    row_copy->key = copy_value(row->key, primary_meta_column->column_type);
    strcpy(row_copy->table_name, row->table_name);
    row_copy->data = create_list(NODE_KEY_VALUE);

    ListCell *lc;
    foreach (lc, row->data) {
        append_list(row_copy->data, copy_key_value(lfirst(lc)));
    }

    return row_copy;
}

/* Copy row igonore system reserved columns. */
Row *copy_row_without_reserved(Row *row) {
    /* check */
    if (row == NULL)
        return NULL;

    Table *table = open_table(row->table_name);
    if (table == NULL) {
        db_log(ERROR, "Table '%s' not exists. ", row->table_name);
        return NULL;
    }

    MetaColumn *primary_meta_column = get_primary_key_meta_column(table->meta_table);

    /* copy row. */
    Row *row_copy = instance(Row);
    row_copy->key = copy_value(row->key, primary_meta_column->column_type);
    strcpy(row_copy->table_name, row->table_name);
    row_copy->data = create_list(NODE_KEY_VALUE);

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);

        /* Skip system reserved columns. */
        MetaColumn *meta_column = get_all_meta_column_by_name(table->meta_table, key_value->key);
        if (meta_column && meta_column->sys_reserved) 
            continue;

        append_list(row_copy->data, copy_key_value(key_value));
    }

    return row_copy;
}

/* Copy refer. */
Refer *copy_refer(Refer *refer) {
    if (refer == NULL) return NULL;
    Refer *copy_refer = instance(Refer);
    memset(copy_refer, 0, sizeof(Refer));
    strcpy(copy_refer->table_name, refer->table_name);
    copy_refer->page_num = refer->page_num;
    copy_refer->cell_num = refer->cell_num;
    return copy_refer;
}

/* Copy meta column. */
MetaColumn *copy_meta_column(MetaColumn *meta_column) {
    if (meta_column == NULL)
        return NULL;

    MetaColumn *meta_column_copy = instance(MetaColumn);
    memcpy(meta_column_copy, meta_column, sizeof(MetaColumn));
    return meta_column_copy;
}

/* Copy MetaTable.*/
MetaTable *copy_meta_table(MetaTable *meta_table) {
    if (meta_table == NULL)
        return NULL;

    MetaTable *copy = instance(MetaTable);
    copy->table_name = dstrdup(meta_table->table_name);
    copy->column_size = meta_table->column_size;
    copy->all_column_size = meta_table->all_column_size;
    copy->meta_column = dalloc(sizeof(MetaColumn *) * copy->all_column_size);

    uint32_t i;
    for (i = 0; i < meta_table->all_column_size; i++) {
        copy->meta_column[i] = copy_meta_column(meta_table->meta_column[i]);
    }

    return copy;
}

/* Copy BufferDesc. */
BufferDesc *copy_buffer_desc(BufferDesc *buff_desc) {
    if (buff_desc == NULL)
        return NULL;

    BufferDesc *duplica = instance(BufferDesc);
    memcpy(duplica, buff_desc, sizeof(BufferDesc));
    return duplica;
}

/* Copy Table. */
Table *copy_table(Table *table) {
    if (table == NULL)
        return NULL;

    Table *duplica = instance(Table);
    duplica->root_page_num = table->root_page_num;
    duplica->meta_table = copy_meta_table(table->meta_table);
    duplica->creator = table->creator;
    duplica->page_size = table->page_size;

    return duplica;
}

/* Copy column node. */
ColumnNode *copy_column_node(ColumnNode *column_node) {
    if (column_node == NULL)
        return NULL;
    ColumnNode *column_node_copy = instance(ColumnNode);
    column_node_copy->has_sub_column = column_node->has_sub_column;
    if (column_node_copy->has_sub_column) {
        column_node_copy->sub_column = copy_column_node(column_node->sub_column);
        column_node_copy->scalar_exp_list = list_copy_deep(column_node->scalar_exp_list);
    }
    column_node_copy->column_name = dstrdup(column_node->column_name);
    return column_node_copy;
}

/* Copy AtomNode. */
AtomNode *copy_atom_node(AtomNode *atom_node) {
    if (!atom_node)
        return NULL;
    AtomNode *atom_node_dup = instance(AtomNode);
    atom_node_dup->type = atom_node->type;
    switch (atom_node_dup->type) {
        case A_INT:
            atom_node_dup->value.intval = atom_node->value.intval;
            break;
        case A_BOOL:
            atom_node_dup->value.boolval = atom_node->value.boolval;
            break;
        case A_FLOAT:
            atom_node_dup->value.floatval = atom_node->value.floatval;
            break;
        case A_STRING:
            atom_node_dup->value.strval = dstrdup(atom_node->value.strval);
            break;
        case A_REFERENCE:
            atom_node_dup->value.referval = copy_refer_value(atom_node->value.referval);
            break;
        default:
            UNEXPECTED_VALUE(atom_node_dup->type);
    }
    
    return atom_node_dup;
}

/* Copy value item node. */
ValueItemNode *copy_value_item_node(ValueItemNode *value_item_node) {
    if (value_item_node == NULL)
        return NULL;
    ValueItemNode *value_item_node_dup = instance(ValueItemNode);
    value_item_node_dup->type = value_item_node->type;
    switch (value_item_node_dup->type) {
        case V_ATOM: {
            value_item_node_dup->value.atom = copy_atom_node(value_item_node->value.atom);
            break;
        }
        case V_ARRAY: {
            value_item_node_dup->value.value_list = list_copy_deep(value_item_node->value.value_list);
            break;
        }
        case V_NULL:
            break;
        default:
            UNEXPECTED_VALUE(value_item_node_dup->type);
    }
    return value_item_node_dup;
}

/*Copy function value node. */
FunctionValueNode *copy_function_value_node(FunctionValueNode *function_value_node) {
    if (function_value_node == NULL)
        return NULL;
    FunctionValueNode *function_value_node_copy = instance(FunctionValueNode);
    function_value_node_copy->value_type = function_value_node->value_type;
    switch(function_value_node->value_type) {
        case V_INT:
            function_value_node_copy->i_value = function_value_node->i_value;
            break;
        case V_COLUMN:
            function_value_node_copy->column = copy_column_node(function_value_node->column);
            break;
        case V_ALL:
            break;
    }
    return function_value_node_copy;
}

/* Copy function node. */
FunctionNode *copy_function_node(FunctionNode *function_node) {
    if (function_node == NULL)
        return NULL;
    FunctionNode *function_node_copy = instance(FunctionNode);
    function_node_copy->type = function_node->type;
    function_node_copy->value = copy_function_value_node(function_node->value);
    return function_node_copy;
}

CalculateNode *copy_calculate_node(CalculateNode *calculate_node) {
    if (calculate_node == NULL)
        return NULL;
    CalculateNode *copy = instance(CalculateNode);
    copy->type = calculate_node->type;
    copy->left = copy_scalar_exp_node(calculate_node->left);
    copy->right = copy_scalar_exp_node(calculate_node->right);

    return copy;
}

PredicateNode *copy_predicate_node(PredicateNode *predicate_node) {
    if (predicate_node == NULL)
        return NULL;
    PredicateNode *copy = instance(PredicateNode);
    copy->type = predicate_node->type;
    switch (copy->type) {
        case PRE_COMPARISON:
            copy->comparison = copy_comparison_node(predicate_node->comparison);
            break;
        case PRE_LIKE:
            copy->like = copy_like_node(predicate_node->like);
            break;
        case PRE_IN:
            copy->in = copy_in_node(predicate_node->in);
            break;
        default:
            db_log(PANIC, "Unknown predicate type");
    }
    return copy;
}

/* Copy condition node. */
ConditionNode *copy_condition_node(ConditionNode *condition_node) {
    if (condition_node == NULL)
        return NULL;
    ConditionNode *condition_node_copy = instance(ConditionNode);
    condition_node_copy->conn_type = condition_node->conn_type;
    switch(condition_node->conn_type) {
        case C_OR:
        case C_AND:
            condition_node_copy->left = copy_condition_node(condition_node->left);
            condition_node_copy->right = copy_condition_node(condition_node->right);
            condition_node_copy->conn_type = condition_node->conn_type;
            break;
        case C_NONE:
            condition_node_copy->predicate = copy_predicate_node(condition_node->predicate);
            break;
        default:
            db_log(ERROR, "Unknown conn type");
            return NULL;
    }
    return condition_node_copy;
}

/* Copy a ComparisonNode. */
ComparisonNode *copy_comparison_node(ComparisonNode *comparison_node) {
    if (comparison_node == NULL)
        return NULL;
    ComparisonNode *comparison_node_copy = instance(ComparisonNode);
    comparison_node_copy->type = comparison_node->type;
    comparison_node_copy->column = copy_column_node(comparison_node->column);
    comparison_node_copy->value = copy_scalar_exp_node(comparison_node->value);
    return comparison_node_copy;
}

/* Copy a LikeNode. */
LikeNode *copy_like_node(LikeNode *like_node) {
    if (like_node == NULL)
        return NULL;
    LikeNode *copy = instance(LikeNode);
    copy->column = copy_column_node(like_node->column);
    copy->value = copy_value_item_node(like_node->value);
    return copy;
}

/* Copy an InNode. */
InNode *copy_in_node(InNode *in_node) {
    if (in_node == NULL)
        return NULL;
    InNode *copy = instance(InNode);
    copy->column = copy_column_node(in_node->column);
    copy->value_list = list_copy_deep(in_node->value_list);
    return copy;
}

/* Copy LimitNode. */
LimitClauseNode *copy_limit_clause_node(LimitClauseNode *limit_clause_node) {
    if (is_null(limit_clause_node))
        return NULL;
    LimitClauseNode *duplica = instance(LimitClauseNode);
    duplica->rows = limit_clause_node->rows;
    duplica->offset = limit_clause_node->offset;
    return duplica;
}

/* Copy ReferValue. */
ReferValue *copy_refer_value(ReferValue *refer_value) {
    if (refer_value == NULL)
        return NULL;
    ReferValue *refer_value_copy = instance(ReferValue);
    refer_value_copy->type = refer_value->type;
    switch (refer_value->type) {
        case DIRECTLY:
            refer_value_copy->nest_value_list = list_copy_deep(refer_value->nest_value_list);
            break;
        case INDIRECTLY:
            refer_value_copy->condition = copy_condition_node(refer_value->condition);
            break;
    }
    return refer_value_copy;
}

/* Copy ArrayValue. */
ArrayValue *copy_array_value(ArrayValue *array_value) {
    if (!array_value) 
        return NULL;
    ArrayValue *array_value_dup = instance(ArrayValue);
    array_value_dup->type = array_value->type;
    array_value_dup->list = create_list(NODE_VOID);

    ListCell *lc;
    foreach (lc, array_value->list) {
        append_list(array_value_dup->list, copy_value(lfirst(lc), array_value_dup->type));
    }
    return array_value_dup;
}

/* Copy a ScalarExpNode. */
ScalarExpNode *copy_scalar_exp_node(ScalarExpNode *scalar_exp_node) {
    if (scalar_exp_node == NULL)
        return NULL;

    ScalarExpNode *copy = instance(ScalarExpNode);
    copy->type = scalar_exp_node->type;
    switch (scalar_exp_node->type) {
        case SCALAR_COLUMN:
            copy->column = copy_column_node(scalar_exp_node->column);
            break;
        case SCALAR_FUNCTION:
            copy->function = copy_function_node(scalar_exp_node->function);
            break;
        case SCALAR_CALCULATE:
            copy->calculate = copy_calculate_node(scalar_exp_node->calculate);
            break;
        case SCALAR_VALUE:
            copy->value = copy_value_item_node(scalar_exp_node->value);
            break;
        default:
            db_log(ERROR, "Not support scalar type.");
    }
    copy->alias = copy_value(scalar_exp_node->alias, T_STRING);
    return copy;
}

/* Copy TableRefNode. */
TableRefNode *copy_table_ref_node(TableRefNode *table_ref) {
    if (!table_ref)
        return NULL;
    TableRefNode *table_ref_copy = instance(TableRefNode);
    if (table_ref->table)
        table_ref_copy->table = dstrdup(table_ref->table);
    if (table_ref->range_variable)
        table_ref_copy->range_variable = dstrdup(table_ref->range_variable);
    return table_ref_copy;
}


/* Copy FromClauseNode. */
FromClauseNode *copy_from_clause_node(FromClauseNode *from_clause_node) {
    if (!from_clause_node)
        return NULL;

    FromClauseNode *from_clause_copy = instance(FromClauseNode);
    from_clause_copy->from = list_copy_deep(from_clause_node->from);
    return from_clause_copy;
}

/* Copy WhereClauseNode. */
WhereClauseNode *copy_where_clause_node(WhereClauseNode *where_clause) {
    if (!where_clause)
        return NULL;
    WhereClauseNode *where_clause_copy = instance(WhereClauseNode);
    where_clause_copy->condition = copy_condition_node(where_clause->condition);
    return where_clause_copy;
}

/* Copy TableExpNode. */
TableExpNode *copy_table_exp_node(TableExpNode *table_exp_node) {
    if (!table_exp_node) 
        return NULL;
    TableExpNode *table_exp_copy = instance(TableExpNode);
    table_exp_copy->from_clause = copy_from_clause_node(table_exp_node->from_clause);
    table_exp_copy->where_clause = copy_where_clause_node(table_exp_node->where_clause);
    table_exp_copy->limit_clause = copy_limit_clause_node(table_exp_node->limit_clause);
    return table_exp_copy;
}

/* Copy a dymamic memory block */
void *copy_block(void *value, size_t size) {
    if (value == NULL)
        return NULL;
    void *block = dalloc(size);
    memcpy(block, value, size);
    return block;
}

