/********************************** Check Module ********************************************************
 * Auth:        JerryZhou
 * Created:     2024/01/09
 * Modify:      2024/11/26
 * Locataion:   src/common/check.c
 * Description: The check module is intended to check user sql valid and it only works for static checking.
 ***********************************************************************************************************
 */
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <time.h>
#include <limits.h>
#include <float.h>
#include "check.h"
#include "utils.h"
#include "asserts.h"
#include "compare.h"
#include "data.h"
#include "table.h"
#include "log.h"
#include "copy.h"
#include "free.h"
#include "ltree.h"
#include "trans.h"
#include "pager.h"
#include "meta.h"
#include "index.h"
#include "select.h"
#include "refer.h"
#include "list.h"
#include "cache.h"
#include "instance.h"
#include "sys.h"
#include "systable.h"

static bool check_value_item_set_node(MetaTable *meta_table, char *column_name, List *value_list);
static bool check_scalar_exp(ScalarExpNode *scalar_exp, AliasMap alias_map);

/* Get column name in ColumnDefNode. */
static inline char *get_column_def_name(ColumnDefNode *column_def) {
    return column_def->column->column;
}

/* Get data type in ColumnDefNode. */
static inline DataType get_column_def_data_type(ColumnDefNode *column_def) {
    return column_def->data_type->type;
}

/* Get ConditionNode from WhereClauseNode. */
static ConditionNode *get_condition_from_where_clause(WhereClauseNode *where_clause) {
    if (!where_clause)
        return NULL;
    return where_clause->condition;
}

/* Get value from atom. 
 * Notice, CHAR, DATE, TIMESTAMP use '%s' format to pass value. */
static void *get_value_from_atom(AtomNode *atom_node) {
    switch (atom_node->type) {
        case A_INT:
            return &atom_node->value.intval;
        case A_BOOL:
            return &atom_node->value.boolval;
        case A_FLOAT:
            return &atom_node->value.floatval;
        case A_STRING:
            return atom_node->value.strval;
        case A_REFERENCE:
            return atom_node->value.referval;
        default:
            UNEXPECTED_VALUE(atom_node->type);
            return NULL;
    } 
}

/* Find meta column in table ref list.
 * Return meta column or NULL if not found.
 * */
static MetaColumn *find_meta_column_in_table_ref_list(List *list, char *column_name) {
    ListCell *lc;
    foreach (lc, list) {
        TableRefNode *table_ref = lfirst(lc);
        Table *table = open_table(table_ref->table);
        if (table == NULL) {
            db_log(ERROR, "Table '%s' not exist.", table_ref->table);
            return NULL;
        }
        MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, column_name);
        if (meta_column != NULL)
            return meta_column;
    }

    return NULL;
}

static bool include_column_for_query_spece(MetaColumn *meta_column, QuerySpecNode *query_spec) {
    SelectionNode *selection = query_spec->selection;
    List *list = query_spec->table_exp->from_clause->from;
    if (selection->all_column) {
        MetaColumn *target_meta_column = find_meta_column_in_table_ref_list(list, meta_column->column_name);
        if (!target_meta_column) {
            db_log(ERROR, "Lack column '%s' in query spec. ", 
                   meta_column->column_name);
            return false;
        }
        if (meta_column->column_type != target_meta_column->column_type) {
            db_log(ERROR, "Column '%s' data type is %s, but support data type %s in query spec.", 
                   meta_column->column_name,
                   data_type_name(meta_column->column_type),
                   data_type_name(target_meta_column->column_type));
            return false;
        }
        return true;
    } else {
        MetaColumn *target_meta_column = NULL;

        ListCell *lc;
        foreach (lc, selection->scalar_exp_list) {
            ScalarExpNode *scalar_exp = lfirst(lc);
            char *alias_name = scalar_exp->alias;
            char *column_name = scalar_exp->column->column_name;
            if (alias_name) {
                if (streq(meta_column->column_name, alias_name)) 
                    target_meta_column = find_meta_column_in_table_ref_list(list, column_name);
            } else {
               if (streq(meta_column->column_name, column_name)) 
                    target_meta_column = find_meta_column_in_table_ref_list(list, column_name);
            }
        }
        if (!target_meta_column) {
            db_log(ERROR, "Lack column '%s' in query spec. ", meta_column->column_name);
            return false;
        }
        if (meta_column->column_type != target_meta_column->column_type) {
            db_log(ERROR, "Column '%s' data type is %s, but support data type %s in query spec.", 
                   meta_column->column_name,
                   data_type_name(meta_column->column_type),
                   data_type_name(target_meta_column->column_type));
            return false;
        }
        return true;
    }
}

/* Check ident node. */
static bool check_column_node(ColumnNode *column_node, MetaTable *meta_table) {
    uint32_t i;
    for (i = 0; i < meta_table->column_size; i++) {
        MetaColumn *meta_column = meta_table->meta_column[i];
        if (streq(meta_column->column_name, column_node->column_name)) {
            if (column_node->has_sub_column == false)
                return true;
            else if (meta_column->column_type == T_REFERENCE && column_node->has_sub_column) {
                Table *table = open_table(meta_column->table_name);
                if (column_node->sub_column)
                    return check_column_node(column_node->sub_column, table->meta_table);
                else if (column_node->scalar_exp_list) {

                    ListCell *lc;
                    foreach (lc, column_node->scalar_exp_list) {
                        ScalarExpNode *scalar_exp = lfirst(lc);
                        switch (scalar_exp->type) {
                            case SCALAR_COLUMN:{
                                check_column_node(scalar_exp->column, table->meta_table);
                                break;
                            }
                            case SCALAR_FUNCTION:
                                db_log(ERROR, "Not allowed use funtion in nested column. ");
                                break;
                            case SCALAR_CALCULATE:
                            case SCALAR_VALUE:
                                break;
                            default:
                                db_log(PANIC, "Unknown Scalar Express type.");
                        }
                    }
                    return true;
                }
            }
        }
    }
    
    /* Reach here, means column is unknown. */
    if (column_node->range_variable) 
        db_log(ERROR, "Unknown column '%s.%s', ", 
               column_node->range_variable, 
               column_node->column_name);
    else 
        db_log(ERROR, "Unknown column '%s', ", 
               column_node->column_name);

    return false;
}

/* Confirm MetaTable via ColumnNode. */
static MetaTable *confirm_meta_table_via_column(ColumnNode *column, AliasMap alias_map) {

    MetaTable *current_meta_table = NULL;
    int times = 0;

    uint32_t i, j;
    for (i = 0; i < alias_map.size; i++) {
        AliasEntry alias_entry = alias_map.map[i];
        Table *table = open_table(alias_entry.name);
        MetaTable *meta_table = table->meta_table;

        if (column->range_variable && 
            (streq(column->range_variable, alias_entry.name) || 
                streq(column->range_variable, alias_entry.alias))) 
                current_meta_table = meta_table;

        if (column->range_variable == NULL) {
            for (j = 0; j < meta_table->column_size; j++) {
                MetaColumn *meta_column = meta_table->meta_column[j];
                if (streq(meta_column->column_name, column->column_name)) {
                    current_meta_table = meta_table;
                    times++;
                }            
            }
        }
    }

    if (current_meta_table == NULL) {
        if (column->range_variable)
            db_log(ERROR, "Unknown column name '%s.%s'. ", 
                   column->range_variable, column->column_name);
        else
            db_log(ERROR, "Unknown column name '%s'. ", 
                   column->column_name);

        return current_meta_table;
    }

    if (times > 1) {
        db_log(ERROR, "Column name '%s' is ambiguous.", column->column_name);
        return current_meta_table;
    }

    return current_meta_table;

}

/* Check if type convert pass. */
static bool check_value_data_type(DataType column_type, AtomNode *atom_node, 
                                  char *column_name, char *table_name) {
    switch(column_type) {
        case T_BOOL: {
            if (atom_node->type == A_BOOL)
                return true;
            break;
        }
        case T_INT:
        case T_LONG: {
            if (atom_node->type == A_INT)
                return true;
            break;
        }
        case T_FLOAT:
        case T_DOUBLE: {
            if (atom_node->type == A_FLOAT || atom_node->type == A_INT)
                return true;
            break;
        }
        case T_CHAR:
        case T_STRING:
        case T_VARCHAR: {
            if (atom_node->type == A_STRING)
                return true;
            break;
        }
        case T_TIMESTAMP:
        case T_DATE: {
            if (atom_node->type == A_STRING)
                return true;
            break;
        }
        case T_REFERENCE: 
            /* For Reference, it`s complicate, user can pass a refer or subrow column, 
             * to be simple, just make flag true. */
            return true;
        default:
            UNEXPECTED_VALUE(column_type);
    }
    db_log(ERROR, "Incorrect %s value for column '%s' with type %s in table '%s'", 
           data_type_name(convert_data_type(atom_node->type)),
           column_name, 
           data_type_name(column_type), 
           table_name);
    return false;
}

/* Check value if valid. 
 * Because, CHAR, DATE, TIMESTAMP use '%s' format to pass value, thus check it. */
bool check_value_valid(MetaColumn *meta_column, AtomNode *atom_node) {

    /* Get value from atom. */
    void *value = get_value_from_atom(atom_node);

    switch(meta_column->column_type) {
        case T_BOOL:
        case T_LONG:
        case T_DOUBLE:
        case T_REFERENCE: 
            return true;
        case T_INT: {
            if (atom_node->value.intval > INT_MAX || atom_node->value.intval < INT_MIN)
                db_log(ERROR, "Value is int overflow for column '%s'.", meta_column->column_name);
            return true;
        }
        case T_FLOAT: {
            if (atom_node->type == A_FLOAT && 
                    (isinff(atom_node->value.floatval) || 
                        atom_node->value.floatval > FLT_MAX || 
                            atom_node->value.floatval < FLT_MIN))
                db_log(ERROR, "Value is float overflow for column '%s'.", meta_column->column_name);
            return true;
        }
        case T_CHAR: {
            if (value == NULL)
                return false;
            /* For CHAR type, only allow one character. */
            size_t len = strlen((char *) value);
            if (len != 1)
                db_log(ERROR, "Try to convert value '%s' to char value type fail.", 
                       (char *) value);
            return len == 1;
        }
        case T_VARCHAR:
        case T_STRING: {
            if (value == NULL)
                return false;
            size_t size = strlen(value);
            if (size > meta_column->column_length)
                db_log(ERROR, "Exceed the limit of data length: %d > %d, for column '%s'. ", 
                       size, 
                       meta_column->column_length - 2, 
                       meta_column->column_name);
            return size <= meta_column->column_length;
        }
        case T_TIMESTAMP: {   
            if (value == NULL)
                return false;

            /* When data type is TIMESTAMP, user`s input is a STIRNG type. */
            regex_t reegex;
            int comp_result, exe_result;

            /* Visit `https://www.regular-expressions.info/gnu.html` and notice there`s not "\\b". */
            comp_result = regcomp(&reegex, "^([0-9]{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])\\s(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(\\.[0-9]{1,3})?$", REG_EXTENDED);
            if (comp_result != 0)
                db_log(ERROR, "Regex compile fail.");
            exe_result = regexec(&reegex, (char *)value, 0, NULL, 0);
            regfree(&reegex);

            if (exe_result == REG_NOMATCH) 
                db_log(ERROR, "Try to convert value '%s' to timestamp value fail.", 
                       (char *) value);

            return exe_result == REG_NOERROR;
        }
        case T_DATE: {
            if (value == NULL)
                return false;
            /* When data type is DATE, user`s input is a STRING type. */
            regex_t reegex;
            int comp_result, exe_result;

            /* Visit `https://www.regular-expressions.info/gnu.html`, and notice there`s not "\\b". */
            comp_result = regcomp(&reegex, 
                                  "^([0-9]{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$", 
                                  REG_EXTENDED);
            if (comp_result != 0)
                db_log(ERROR, "Regex compile fail.");
            exe_result = regexec(&reegex, (char *)value, 0, NULL, 0);
            regfree(&reegex);

            if (exe_result == REG_NOMATCH) 
                db_log(ERROR, "Try to convert value '%s' to date value fail.", 
                       (char *) value);

            return exe_result == REG_NOERROR;
        }
        default: {
            UNEXPECTED_VALUE("Not implement yet.");
            return false;
        }
    }
}


/* Check ValueItemNode. */
static bool check_value_item_node(MetaTable *meta_table, char *column_name, ValueItemNode *value_item_node) {

    uint32_t i;
    for (i = 0; i < meta_table->column_size; i++) {
        MetaColumn *meta_column = meta_table->meta_column[i];
        if (streq(meta_column->column_name, column_name)) {
            switch (value_item_node->type) {
                case V_ATOM: {
                    AtomNode *atom_node = value_item_node->value.atom;
                    return check_value_data_type(meta_column->column_type, atom_node, column_name, meta_table->table_name) && 
                                check_value_valid(meta_column, atom_node);
                }
                case V_NULL: {
                    if (meta_column->not_null)
                        db_log(ERROR, "Column '%s' can`t be null.", 
                               column_name);
                    return true;
                }
                case V_ARRAY: {
                    List *value_list = value_item_node->value.value_list;
                    return check_value_item_set_node(meta_table, column_name, value_list);
                }
                default: {
                    UNEXPECTED_VALUE(value_item_node->type);
                    return false;
                }
            }
        }
    }

    db_log(ERROR, "Unknown column '%s'.", column_name);
    return false;
}

/* Check ValueItemSetNode. */
static bool check_value_item_set_node(MetaTable *meta_table, char *column_name, List *value_list) {
    ListCell *lc;
    foreach (lc, value_list) {
        ValueItemNode *value_item_node = lfirst(lc);
        if (!check_value_item_node(meta_table, column_name, value_item_node))
            return false;
    }
    return true;
}

/* Check function value type. */
static bool check_funtion_value_type(FunctionType type, ColumnNode *column, MetaColumn *meta_column) {
    if (!column->has_sub_column) {
        switch (type) {
            case F_SUM:
            case F_AVG:
            case F_MAX:
            case F_MIN: {
                if (meta_column->column_type == T_REFERENCE) {
                    db_log(ERROR, "Function %s not support for reference type column.", 
                           function_type_name(type));
                    return false;
                }
                break;
            }
            default:
                break;
        }
    } else if (column->sub_column) {
        Table *table = open_table(meta_column->table_name);
        MetaColumn *sub_meta_column = get_meta_column_by_name(table->meta_table, column->sub_column->column_name);
        return check_funtion_value_type(type, column->sub_column, sub_meta_column);
    } else if (column->has_sub_column) {
        db_log(ERROR, "Function %s not support for reference type column.", 
               function_type_name(type));
        return false;
    }

    return true;
}

/* Check function node */
static bool check_function_node(FunctionNode *function, AliasMap alias_map) {
    FunctionValueNode *value_node = function->value;
    switch(value_node->value_type) {
        case V_INT:
        case V_ALL:
            return true;
        case V_COLUMN: {
            ColumnNode *column = value_node->column;
            MetaTable *meta_table = confirm_meta_table_via_column(column, alias_map);
            MetaColumn *meta_column = get_meta_column_by_name(meta_table, column->column_name);
            return check_funtion_value_type(function->type, column, meta_column) && 
                        check_column_node(column, meta_table); 
        }
        default: {
            UNEXPECTED_VALUE(value_node->value_type);
            return false;
        }
    }
}

/* Check CalculateNode. */
static bool check_calculate_node(CalculateNode *calculate_node, AliasMap alias_map) {
    return check_scalar_exp(calculate_node->left, alias_map) && 
            check_scalar_exp(calculate_node->right, alias_map);
}

/* Check ScalarExpNode if column. */
static bool check_scalar_exp(ScalarExpNode *scalar_exp, AliasMap alias_map) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:{
            ColumnNode *column = scalar_exp->column;
            MetaTable *meta_table = confirm_meta_table_via_column(column, alias_map);
            return check_column_node(scalar_exp->column, meta_table);
        }
        case SCALAR_FUNCTION:
            return check_function_node(scalar_exp->function, alias_map);
        case SCALAR_CALCULATE:
            return check_calculate_node(scalar_exp->calculate, alias_map);
        case SCALAR_VALUE:
            return true;
        default: {
            UNEXPECTED_VALUE("Unknown Scalar Express type.");
            return false;
        }
    }
}

/* Check ScalarExpNode list. */
static bool check_scalar_exp_list(List *scalar_exp_list, AliasMap alias_map) {

    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        if (!check_scalar_exp(scalar_exp, alias_map))
            return false;
    }

    return true;
}

/* Check select items if exist int meta column */
static bool check_selection(SelectionNode *selection_node, AliasMap alias_map) {
    return selection_node->all_column 
           ? true 
           : check_scalar_exp_list(selection_node->scalar_exp_list, alias_map);
}

/* Check ComparisonNode value.*/
static bool check_comparison_value(ScalarExpNode *comparion_value, MetaTable *meta_table, MetaColumn *meta_column) {
    switch (comparion_value->type) {
        case SCALAR_VALUE:
            return check_value_item_node(meta_table, meta_column->column_name, comparion_value->value);
        case SCALAR_COLUMN:
        case SCALAR_FUNCTION:
        case SCALAR_CALCULATE:
            return true;
        default: {
            UNEXPECTED_VALUE(comparion_value->type);
            return false;
        }
    }
}

/* Check ComparisonNode.*/
static bool check_comparison_node(ComparisonNode *comparison, AliasMap alias_map) {
    ColumnNode *column = comparison->column;

    /* Confirm MetaTable. */
    MetaTable *current_meta_table = confirm_meta_table_via_column(column, alias_map);
    MetaColumn *meta_column = get_meta_column_by_name(current_meta_table, column->column_name);

    return check_column_node(comparison->column, current_meta_table) // check select column
           && check_comparison_value(comparison->value, current_meta_table, meta_column);
}

/* Check InNode. */
static bool check_in_node(InNode *in_node, AliasMap alias_map) {
    ColumnNode *column = in_node->column;
    /* Confirm MetaTable. */
    MetaTable *current_meta_table = confirm_meta_table_via_column(column, alias_map);

    return check_column_node(in_node->column, current_meta_table) && // check select column
                check_value_item_set_node(current_meta_table, column->column_name, in_node->value_list);
}

/* Check like data type. */
static bool check_like_data_type(MetaColumn *meta_column) {
    if (meta_column->column_type != T_STRING) {
        db_log(ERROR, "For like predicate, only support string data type.");
        return false;
    }
    return true;
}

/* Check LikeNode. */
static bool check_like_node(LikeNode *like_node, AliasMap alias_map) {
    ColumnNode *column = like_node->column;

    /* Confirm MetaTable. */
    MetaTable *current_meta_table = confirm_meta_table_via_column(column, alias_map);
    MetaColumn *meta_column = get_meta_column_by_name(current_meta_table, column->column_name);

    return check_column_node(column, current_meta_table) && // check select column
                check_like_data_type(meta_column) && 
                    check_value_item_node(current_meta_table, column->column_name, like_node->value);
}

/* Check PredicateNode. */
static bool check_predicate_node(PredicateNode *predicate_node, AliasMap alias_map) {
    switch (predicate_node->type) {
        case PRE_COMPARISON:
            return check_comparison_node(predicate_node->comparison, alias_map);
        case PRE_IN:
            return check_in_node(predicate_node->in, alias_map);
        case PRE_LIKE:
            return check_like_node(predicate_node->like, alias_map);
        default:
            UNEXPECTED_VALUE(predicate_node->type);
            return false;
    }
}

/* Check condition node. */
static bool check_condition_node(ConditionNode *condition_node, AliasMap alias_entry) {

    if (!condition_node)
        return true;

    switch(condition_node->conn_type) {
        case C_AND:
        case C_OR:
            return check_condition_node(condition_node->left, alias_entry) && 
                        check_condition_node(condition_node->right, alias_entry);
        case C_NONE: 
            return check_predicate_node(condition_node->predicate, alias_entry);
        default:
            UNEXPECTED_VALUE(condition_node->conn_type);
            return false;
    }
}


/* Check TableRefNode. */
static bool check_table_ref(TableRefNode *table_ref) {
    Table *table = open_table(table_ref->table);
    if (table == NULL) {
        db_log(ERROR, "Table '%s' not exist.", table_ref->table);
        return false;
    }
    return true;
}

/* Check TableRef List. */
static bool check_table_ref_list(List *list) {
    uint32_t len = len_list(list);
    uint32_t i, j;
    for (i = 0; i < len; i++) {
        TableRefNode *table_ref = lfirst(list_nth_cell(list, i));
        if (!check_table_ref(table_ref))
            return false;

        for (j = i + 1; j < len; j++) {
            TableRefNode *table_ref2 = lfirst(list_nth_cell(list, j));
            /* Check duplicate table. */
            if (streq(table_ref->table, table_ref2->table)) {
                db_log(ERROR, "Duplicate table '%s'. ", table_ref->table);
                return false;
            }
            /* Check duplicate table alias name. */
            if (table_ref->range_variable && table_ref2->range_variable 
                && streq(table_ref->range_variable, table_ref2->range_variable)) {
                db_log(ERROR, "Duplicate table alias name: '%s'. ", table_ref->range_variable);
                return false;
            }
        }
    }

    return true;
}

/* Check FromClauseNode. */
static bool check_from_clause(FromClauseNode *from_clause) {
    return from_clause == NULL || check_table_ref_list(from_clause->from);
}

/* Check WhereClauseNode. */
static bool check_where_clause(WhereClauseNode *where_clause, AliasMap alias_map) {
    if (where_clause == NULL)
        return true;

    return check_condition_node(where_clause->condition, alias_map);
}

/* Check LimitClauseNode. */
static bool check_limit_clause(LimitClauseNode *limit_clause) {
    if (non_null(limit_clause)) {
        if (limit_clause->rows < 0) {
            db_log(ERROR, "LIMIT must not be negative.");
            return false;
        }
        if (limit_clause->offset < 0) {
            db_log(ERROR, "OFFSET must not be negative.");
            return false;
        }
    }
    return true;
}

/* Check TableExpNode. */
static bool check_table_exp(TableExpNode *table_exp, AliasMap alias_map) {
    return check_from_clause(table_exp->from_clause) && 
                check_where_clause(table_exp->where_clause, alias_map) && 
                    check_limit_clause(table_exp->limit_clause);
}


/* Check assignment set node */
static bool check_assignment_set_node(UpdateNode *update_node) { 

    Table *table = open_table(update_node->table_name);
    List *assignment_list = update_node->assignment_list;
    /* Although this cehck update node, but new select result is SELECT_STMT. */
    SelectResult *select_result = new_select_result(SELECT_STMT, update_node->table_name);
    ConditionNode *condition_node = get_condition_from_where_clause(update_node->where_clause);
    query_with_condition(condition_node, select_result, count_row, ARG_NULL, NULL);

    bool change_priamry = false;
    void *new_key = NULL;
    MetaColumn *primary_meta_column = NULL;

    ListCell *lc;
    foreach (lc, assignment_list) {
        AssignmentNode *assignment_node = lfirst(lc);
        ColumnNode *column_node = assignment_node->column;
        ValueItemNode *value_node = assignment_node->value;
        Assert(column_node != NULL);

        MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, column_node->column_name);
        if (is_null(meta_column)) {
            db_log(ERROR, "Not found column %s in table %s.", 
                   column_node->column_name, 
                   GET_TABLE_NAME(table));
            return false;
        }

        if (meta_column->is_primary) {
            change_priamry = true;
            new_key = get_value_from_value_item_node(value_node, meta_column);
            primary_meta_column = meta_column;
        }

        /* Check column, check type, check if value valid. */
        if (!(check_column_node(column_node, table->meta_table) && 
                check_value_item_node(table->meta_table, meta_column->column_name, value_node))) {
                free_select_result(select_result);
                if (change_priamry)
                    free_value(new_key, primary_meta_column->column_type);
                return false;
        }
    }

    if (change_priamry) {
        if (select_result->row_size > 1) {
            select_result->row_size = 0;
            db_log(ERROR, "Duplicate key not allowd. ");
            return false;
        }
        
        Assert(new_key);
        Assert(primary_meta_column);
        if (select_result->row_size == 1) {
            Cursor *new_cursor = define_cursor(table, new_key, true);
            Refer *new_refer = convert_refer(new_cursor);
            Row *default_row = define_row(new_refer);
            free_cursor(new_cursor);
            free_refer(new_refer);
            if (!RowIsDeleted(default_row) && 
                equal(default_row->key, new_key, primary_meta_column->column_type)) {
                    select_result->row_size = 0;
                    db_log(ERROR, "Key '%s' already exists, not allowd duplicate key. ",
                           get_key_str(new_key, primary_meta_column->column_type));
                    free_value(new_key, primary_meta_column->column_type);
                    return false;
            }
        }
    }

    select_result->row_size = 0;
    free_select_result(select_result);
    if (change_priamry)
        free_value(new_key, primary_meta_column->column_type);
    return true;
}

/* Check if system reserved table. */
static bool check_sys_reserved_table(char *table_name) {
    if (if_table_reserved(table_name)) {
        db_log(ERROR, "Table '%s' is system reserved, not allowd duplication.", table_name); 
        return false;
    }
    return true;
}

/* Check if table alreay exist. */
static bool check_duplicate_table(char *table_name) {
    if (check_table_exist(table_name)) {
        db_log(ERROR, "Table '%s' already exists.", table_name); 
        return false;
    } 
    else 
        return true;
}

/* Check if column already exists. */
static bool check_if_column_already_exists(List *list, ColumnDefNode *column_def) {

    ListCell *lc;
    foreach (lc, list) {
        ColumnDefNode *current_column_def = lfirst(lc);
        if (streq(current_column_def->column->column, column_def->column->column))
            return true;
    }
    return false;
}


/* Check if ColumnDefOptNodeList contains primary key. */
static bool check_if_contain_primary_key(List *column_def_opt_list) {
    if (column_def_opt_list) {
        ListCell *lc;
        foreach (lc, column_def_opt_list) {
            ColumnDefOptNode *column_def_opt = lfirst(lc);
            if (column_def_opt->opt_type == OPT_PRIMARY_KEY)
                return true;
        }
    }
    return false;
}


static bool check_default_atom_value_type(AtomNode *atom_node, DataType data_type) {
    switch(data_type) {
        case T_BOOL: {
            if (atom_node->type == A_BOOL)
                return true;
            break;
        }
        case T_INT:
        case T_LONG: {
            if (atom_node->type == A_INT)
                return true;
            break;
        }
        case T_FLOAT:
        case T_DOUBLE: {
            if (atom_node->type == A_FLOAT || atom_node->type == A_INT)
                return true;
            break;
        }
        case T_CHAR:
        case T_STRING:
        case T_VARCHAR: {
            if (atom_node->type == A_STRING)
                return true;
            break;
        }
        case T_TIMESTAMP:
        case T_DATE: {
            if (atom_node->type == A_STRING)
                return true;
            break;
        }
        case T_REFERENCE: 
            /* For Reference, it`s complicate, user can pass a refer or subrow column, 
             * to be simple, just make flag true. */
            return true;
        default:
            UNEXPECTED_VALUE(data_type);
    }

    return false;
}

/* Check default value type. */
static bool check_default_value_type(ValueItemNode *value_item_node, DataType data_type) {
    switch (value_item_node->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item_node->value.atom;
            return check_default_atom_value_type(atom_node, data_type);
        }
        case V_NULL: 
            return true;
        case V_ARRAY: {
            db_log(ERROR, "Not support array as default value.");
            return false;
        }
        default: 
            UNEXPECTED_VALUE(value_item_node->type);
    }
}


/* Check if ColumnDefOptNodeList contains conflict default value. */
static bool check_column_def_opt_list(ColumnDefNode *column_def) {
    if (column_def->column_def_opt_list) {
        bool has_defined_not_null = false;
        bool has_defined_default_null = false;

        ListCell *lc;
        foreach (lc, column_def->column_def_opt_list) {
            ColumnDefOptNode *column_def_opt = lfirst(lc);
            switch (column_def_opt->opt_type) {
                case OPT_NOT_NULL:
                case OPT_PRIMARY_KEY:
                    has_defined_not_null = true;
                    break;
                case OPT_DEFAULT_NULL: 
                    has_defined_default_null = true;
                    break;
                case OPT_DEFAULT_VALUE: {
                    ValueItemNode *value_item_node = column_def_opt->value;
                    DataType data_type = get_column_def_data_type(column_def);
                    if (!check_default_value_type(value_item_node, data_type)) {
                        db_log(ERROR, "Invalid default value for '%s', can`t convert to '%s'.", 
                               get_column_def_name(column_def),
                               data_type_name(data_type));
                        return false;
                    }
                    break;
                } 
                case OPT_COMMENT: {
                    if (strlen(column_def_opt->comment) > MAX_COMMENT_STRING_LENGTH) {
                        db_log(ERROR, "Too long comment for '%s'.", 
                               get_column_def_name(column_def));
                        return false;
                    }
                    break;
                }
                default:
                    break;
            }
        }

        if (has_defined_not_null && has_defined_default_null) {
            db_log(ERROR, "Invalid default value for '%s'", 
                   get_column_def_name(column_def));
            return false;
        }
    }

    return true;
}

/* Check if exists duplicate column name. */
static bool check_table_element_commalist(List *base_table_element_commalist) {

    List *list = create_list(NODE_COLUMN_DEF);

    bool primary_key_flag = false;

    ListCell *lc;
    foreach (lc, base_table_element_commalist) {
        BaseTableElementNode *base_table_element = lfirst(lc);
        switch (base_table_element->type) {
            case TELE_COLUMN_DEF: {
                ColumnDefNode *current_column_def = base_table_element->column_def;
                if (check_if_column_already_exists(list, current_column_def)) {
                    free_list(list);
                    db_log(ERROR, "Column '%s' already exists, not allowd duplicate defination.", 
                           get_column_def_name(current_column_def));
                    return false;
                }
                if (check_if_contain_primary_key(current_column_def->column_def_opt_list)) {
                    if (primary_key_flag) 
                    {
                        free_list(list);
                        db_log(ERROR, "Dulicate primary key.");
                        return false;
                    } 
                    else
                        primary_key_flag = true;
                }
                if (!check_column_def_opt_list(current_column_def)) {
                    return false;
                }
                append_list(list, current_column_def);
                break;
            }
            case TELE_TABLE_CONTRAINT_DEF: {
                TableContraintDefNode *table_contraint_def = base_table_element->table_contraint_def;
                if (table_contraint_def->type == TCONTRAINT_PRIMARY_KEY) {
                    if (primary_key_flag) {
                        free_list(list);
                        db_log(ERROR, "Dulicate primary key.");
                        return false;
                    } else
                        primary_key_flag = true;
                }
                break;
            }
            default:
                db_log(ERROR, "Unknown base table element type.");
                break;
        }
    }
    free_list(list);

    return true;
}

/* Check InsertNode for value items. */
static bool check_insert_node_for_value_items(InsertNode *insert_node, List *value_item_list) {

    /* Check table exist.*/
    Table *table = open_table(insert_node->table_name);
    if (table == NULL)
        return false;

    MetaTable *meta_table = table->meta_table;

    /* According to all column flag, determine the number of column set. */
    if (insert_node->all_column) {
        
        /* Check column number equals the insert values number. */
        if (meta_table->column_size != len_list(value_item_list)) {
            db_log(ERROR, "Column count doesn`t match value count: %d != %d.", 
                   meta_table->column_size, 
                   len_list(value_item_list));
            return false;
        }

        uint32_t i;
        for (i = 0; i < meta_table->column_size; i++) {
            MetaColumn *meta_column = meta_table->meta_column[i];
            ValueItemNode *value_item_node = lfirst(list_nth_cell(value_item_list, i));
            if (!check_value_item_node(meta_table, meta_column->column_name, value_item_node))
                return false;
        }

    } else {

        /* Check column number equals the insert values number. */
        if (len_list(insert_node->column_list) != len_list(value_item_list)) {
            db_log(ERROR, "Column count doesn`t match value count.");
            return false;
        }

        ListCell *lc1, *lc2;
        forboth (lc1, insert_node->column_list, lc2, value_item_list) {
            ColumnNode *column_node = lfirst(lc1);
            ValueItemNode *value_item_node = lfirst(lc2);
            MetaColumn *meta_column = get_meta_column_by_name(meta_table, column_node->column_name);
            if (is_null(meta_column)) {
                db_log(ERROR, "Unknown column '%s'", column_node->column_name);
                return false;
            }
            if (!check_value_item_node(meta_table, meta_column->column_name, value_item_node))
                return false;
        }

    }

    return true;
}

/* Check InsertNode for VALUES. */
static bool check_insert_node_for_values(InsertNode *insert_node, List *value_list) {

    ListCell *lc;
    foreach (lc, value_list) {
        List *value_item_list = lfirst(lc);
        if (!check_insert_node_for_value_items(insert_node, value_item_list))
            return false;
    }

    return true;
}


/* Check InsertNode for QUERY_SPEC. */
static bool check_insert_node_for_query_spec(InsertNode *insert_node, QuerySpecNode *query_spec) {

    /* Check table exist.*/
    Table *table = open_table(insert_node->table_name);
    if (table == NULL)
        return false;

    if (insert_node->all_column) {
        MetaTable *meta_table = table->meta_table;
        uint32_t i;
        for (i = 0; i < meta_table->column_size; i++) {
            MetaColumn *meta_column = meta_table->meta_column[i];
            if (!include_column_for_query_spece(meta_column, query_spec))
                return false;
        }
    } else {
        ListCell *lc;
        foreach (lc, insert_node->column_list) {
            ColumnNode *column_node = lfirst(lc);
            MetaColumn *meta_column = get_meta_column_by_name(table->meta_table, column_node->column_name);
            if (!include_column_for_query_spece(meta_column, query_spec))
                return false;
        }
    }
    return true;
}

/* Check ValuesOrQuerySpecNode in InsertNode. */
static bool check_values_or_query_spec(InsertNode *insert_node, ValuesOrQuerySpecNode *values_or_query_spec) {

    switch (values_or_query_spec->type) {
        case VQ_VALUES:
            return check_insert_node_for_values(insert_node, values_or_query_spec->values);
        case VQ_QUERY_SPEC:
            return check_insert_node_for_query_spec(insert_node, values_or_query_spec->query_spec);
        default:
            db_log(ERROR, "Unknown ValuesOrQuerySpecNode type");
            return false;
    }

}

/* Get column name for AddColumnDef. */
static inline char *get_column_name_from_add_column_def(AddColumnDef *add_column) {
    return add_column->column_def->column->column;
}


/* Check alter table add-column action*/
static bool check_alter_table_add_column_action(char *table_name, AddColumnDef *add_column) {
    char *column_name = get_column_name_from_add_column_def(add_column);
    /* Check add column if exists. */
    if (if_exists_column_in_table(column_name, table_name)) {
        db_log(ERROR, "Table '%s' already exists column '%s'.", 
               table_name, 
               column_name);
        return false;
    }

    /* Check if the position column def exists. */
    if (!is_null(add_column->position_def) 
            && !if_exists_column_in_table(add_column->position_def->column, table_name)) {
        db_log(ERROR, "Unknown column '%s' in table '%s'.", 
               add_column->position_def->column, 
               table_name);
        return false;
    }

    /* Check if add primary-key column, not support yet.*/
    return true;    
}

/* Check for dorp-column action. */
static bool check_alter_table_drop_column(char *table_name, DropColumnDef *drop_column_def) {
    
    Table *table = open_table(table_name);
    MetaTable *meta_table = table->meta_table;

    MetaColumn *meta_column = get_meta_column_by_name(meta_table, drop_column_def->column_name);

    /* Check drop column if exists. */
    if (is_null(meta_column)) {
        db_log(ERROR, "Table '%s' not exists column '%s'.", 
               table_name, 
               drop_column_def->column_name);
        return false;
    }

    /* Not alloed drop primary-key column. */
    if (meta_column->is_primary) {
        db_log(ERROR, "Column '%s' is priamry-key, not allowed to drop.", 
               drop_column_def->column_name);
        return false;
    }

    /* If only exists last one column, not allowed to drop. */
    if (meta_table->column_size == 1) {
        db_log(ERROR, "Column '%s' is the last column, not allowed to drop.", 
               drop_column_def->column_name);
        return false;
    }

    return true;
}


/* Check alter table action. */
static bool check_alter_table_action(char *table_name, AlterTableAction *action) {
    switch (action->type) {
        case ALTER_TO_ADD_COLUMN:
            return check_alter_table_add_column_action(table_name, action->action.add_column);
        case ALTER_TO_DROP_COLUMN:
            return check_alter_table_drop_column(table_name, action->action.drop_column);
    }
    return true;
}

/* Check table. */
static bool check_table(char *table_name) {
    if (check_table_exist(table_name))
        return true;
    else {
        db_log(ERROR, "Table '%s' not exists.", table_name);
        return false;
    }
}

/* Check if table uses refer. */
static bool if_table_used_refer(Table *table, char *refer_table_name) {
    MetaTable *meta_table = table->meta_table;

    uint32_t i;
    for (i = 0; i < meta_table->column_size; i++) {
        MetaColumn *current_meta_column = meta_table->meta_column[i];
        if (current_meta_column->column_type == T_REFERENCE && strcmp(current_meta_column->table_name, refer_table_name) == 0) {
            db_log(ERROR , "Table '%s' is refered by column '%s' in table '%s', so can`t drop it.", 
                   refer_table_name, current_meta_column->column_name, table->meta_table->table_name);
            return true;
        }
    }

    return false;
}

/* Check SelectNode. */
bool check_select_node(SelectNode *select_node) {
    AliasMap alias_map;
    alias_map.size = 0;

    FromClauseNode *from_clause = select_node->table_exp->from_clause;
    if (from_clause == NULL)
        return true;

    if (len_list(from_clause->from) > MAX_MULTI_TABLE_NUM) {
        db_log(ERROR, "Exceed max table numbers.");
        return false;
    }

    ListCell *lc;
    foreach (lc, from_clause->from) {
        TableRefNode *table_ref = lfirst(lc);
        alias_map.map[alias_map.size].name = table_ref->table;
        alias_map.map[alias_map.size].alias = table_ref->range_variable;
        alias_map.size++;
    }

    return check_table_exp(select_node->table_exp, alias_map)
        && check_selection(select_node->selection, alias_map);
}


/* Check insert node. */
bool check_insert_node(InsertNode *insert_node) {
    return check_table(insert_node->table_name)
           && check_values_or_query_spec(insert_node, insert_node->values_or_query_spec); 
}

/* Check for update node. */
bool check_update_node(UpdateNode *update_node) {
    AliasMap alias_map;
    alias_map.size = 1;
    alias_map.map[0].name = update_node->table_name;
    alias_map.map[0].alias = update_node->table_name;

    return check_table(update_node->table_name) && 
                check_assignment_set_node(update_node) && 
                    check_where_clause(update_node->where_clause, alias_map);
}

/* Check for delete node. */
bool check_delete_node(DeleteNode *delete_node) {
    /* Unimplement. */
    AliasMap alias_map;
    alias_map.size = 1;
    alias_map.map[0].name = delete_node->table_name;
    alias_map.map[0].alias = delete_node->table_name;

    return check_table(delete_node->table_name) && 
                check_condition_node(delete_node->condition_node, alias_map);
}

/* Check for create table node. */
bool check_create_table_node(CreateTableNode *create_table_node) {
    return check_sys_reserved_table(create_table_node->table_name) &&
                check_duplicate_table(create_table_node->table_name) && 
                    check_table_element_commalist(create_table_node->base_table_element_commalist);
}


/* Check allowed to drop table. */
bool check_drop_table(char *table_name) {
    /* Check table exists. */
    if (!check_table_exist(table_name)) {
        db_log(ERROR, "Table '%s' not exists.", table_name);
        return false;
    }
    
    /* Check table refered by others. */
    List *obj_list = FindAllObject();

    bool ret = true;
    ListCell *lc;
    foreach (lc, obj_list) {
        Object *entity = (Object *) lfirst(lc);
        Table *table = open_table_inner(entity->oid);
        if (if_table_used_refer(table, table_name))  {
            ret = false;
            break;
        }
    }

    return ret;
}

/* Check for AlterTableNode. */
bool check_alter_table(AlterTableNode *alter_table) {
    return check_table(alter_table->table_name)
            && check_alter_table_action(alter_table->table_name, alter_table->action);
}
