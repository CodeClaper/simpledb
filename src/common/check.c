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
#include "row.h"
#include "func.h"
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
#include "instance.h"
#include "sys.h"
#include "tablecache.h"
#include "optimizer.h"

static bool CheckForValueList(MetaTable *meta_table, char *column_name, List *value_list);
static bool CheckForScalarExp(ScalarExpNode *scalar_exp, AliasMap alias_map);
static bool CheckForSearchCondition(SearchConditionNode *condition_node, AliasMap alias_map);
static bool CheckSclarExpInSearchCondition(ScalarExpNode *scalar_exp);

/* Get column name in ColumnDefNode. */
static inline char *ColumnDefFindName(ColumnDefNode *column_def) {
    return column_def->column->column;
}

/* Get data type in ColumnDefNode. */
static inline DataType ColumnDefFindType(ColumnDefNode *column_def) {
    return column_def->data_type->type;
}

/* Get SearchConditionNode from WhereClauseNode. */
static SearchConditionNode *WhereClauseFindSearchCondition(WhereClauseNode *where_clause) {
    if (!where_clause)
        return NULL;
    return where_clause->condition;
}

/* Get value from atom. 
 * Notice, CHAR, DATE, TIMESTAMP use '%s' format to pass value. */
static void *AtomGetValue(AtomNode *atom_node) {
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
static MetaColumn *TableRefListFindMetaColumn(List *list, char *column_name) {
    ListCell *lc;
    foreach (lc, list) {
        TableRefNode *table_ref = lfirst(lc);
        Table *table = open_table(table_ref->table);
        if (table == NULL) {
            db_log(ERROR, "Table '%s' not exist.", table_ref->table);
            return NULL;
        }
        MetaColumn *meta_column = NameFindMetaColumn(table->meta_table, column_name);
        if (meta_column != NULL)
            return meta_column;
    }

    return NULL;
}

static bool CheckQuerySpecMatchColumn(MetaColumn *meta_column, QuerySpecNode *query_spec) {
    SelectionNode *selection = query_spec->selection;
    List *list = query_spec->table_exp->from_clause->from;
    if (selection->all_column) {
        MetaColumn *target_meta_column = TableRefListFindMetaColumn(list, meta_column->column_name);
        if (!target_meta_column) {
            db_log(ERROR, "Lack column '%s' in query spec. ", 
                   meta_column->column_name);
            return false;
        }
        if (meta_column->column_type != target_meta_column->column_type) {
            db_log(ERROR, "Column '%s' data type is %s, but support data type %s in query spec.", 
                   meta_column->column_name,
                   GET_DATA_TYPE_NAME(meta_column->column_type),
                   GET_DATA_TYPE_NAME(target_meta_column->column_type));
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
                if (StrEq(meta_column->column_name, alias_name)) 
                    target_meta_column = TableRefListFindMetaColumn(list, column_name);
            } else {
               if (StrEq(meta_column->column_name, column_name)) 
                    target_meta_column = TableRefListFindMetaColumn(list, column_name);
            }
        }
        if (!target_meta_column) {
            db_log(ERROR, "Lack column '%s' in query spec. ", meta_column->column_name);
            return false;
        }
        if (meta_column->column_type != target_meta_column->column_type) {
            db_log(ERROR, "Column '%s' data type is %s, but support data type %s in query spec.", 
                   meta_column->column_name,
                   GET_DATA_TYPE_NAME(meta_column->column_type),
                   GET_DATA_TYPE_NAME(target_meta_column->column_type));
            return false;
        }
        return true;
    }
}

/* Confirm MetaTable via ColumnNode. */
static MetaTable *ColumnFindMetaTable(ColumnNode *column, AliasMap alias_map) {
    MetaTable *current_meta_table = NULL;
    int times = 0;

    uint32_t i;
    for (i = 0; i < alias_map.size; i++) {
        AliasEntry alias_entry = alias_map.map[i];
        Table *table = open_table(alias_entry.name);
        MetaTable *meta_table = table->meta_table;

        if (column->range_variable && 
            (StrEq(column->range_variable, alias_entry.name) || 
                StrEq(column->range_variable, alias_entry.alias))) 
                current_meta_table = meta_table;

        if (column->range_variable == NULL) {
            ListCell *lc;
            foreach (lc, meta_table->meta_columns) {
                MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
                if (StrEq(meta_column->column_name, column->column_name)) {
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
static bool CheckValueMatchType(DataType column_type, AtomNode *atom_node, char *column_name, char *table_name) {
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
           GET_DATA_TYPE_NAME(AtomTypeConvertDataType(atom_node->type)),
           column_name, 
           GET_DATA_TYPE_NAME(column_type), 
           table_name);
    return false;
}

/* Check value if valid. 
 * Because, CHAR, DATE, TIMESTAMP use '%s' format to pass value, thus check it. */
bool CheckValueIsValid(MetaColumn *meta_column, AtomNode *atom_node) {
    /* Get value from atom. */
    void *value = AtomGetValue(atom_node);

    switch(meta_column->column_type) {
        case T_BOOL:
        case T_LONG:
        case T_DOUBLE:
        case T_REFERENCE: 
        case T_STRING:
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
        case T_VARCHAR:{
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
static bool CheckForValueItem(MetaTable *meta_table, char *column_name, ValueItemNode *value_item_node) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->sys_reserved)
            continue;
        if (StrEq(meta_column->column_name, column_name)) {
            switch (value_item_node->type) {
                case V_ATOM: {
                    AtomNode *atom_node = value_item_node->value.atom;
                    return CheckValueMatchType(meta_column->column_type, atom_node, column_name, meta_table->table_name) && 
                            CheckValueIsValid(meta_column, atom_node);
                }
                case V_NULL: {
                    if (meta_column->not_null)
                        db_log(ERROR, "Column '%s' can`t be null.", 
                               column_name);
                    return true;
                }
                case V_ARRAY: {
                    List *value_list = value_item_node->value.value_list;
                    return CheckForValueList(meta_table, column_name, value_list);
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
static bool CheckForValueList(MetaTable *meta_table, char *column_name, List *value_list) {
    ListCell *lc;
    foreach (lc, value_list) {
        ValueItemNode *value_item_node = lfirst(lc);
        if (!CheckForValueItem(meta_table, column_name, value_item_node))
            return false;
    }
    return true;
}

/* Check function value type. */
static bool CheckFunctionForValueType(FunctionType type, ColumnNode *column, MetaColumn *meta_column) {
    if (!column->has_sub_column) {
        switch (type) {
            case F_SUM:
            case F_AVG:
            case F_MAX:
            case F_MIN: {
                if (meta_column->column_type == T_REFERENCE) {
                    db_log(ERROR, "Function %s not support for reference type column.", 
                           GET_FUNCTION_TYPE_NAME(type));
                    return false;
                }
                break;
            }
            default:
                break;
        }
    } else if (column->sub_column) {
        Table *table = open_table(meta_column->table_name);
        MetaColumn *sub_meta_column = NameFindMetaColumn(table->meta_table, column->sub_column->column_name);
        return CheckFunctionForValueType(type, column->sub_column, sub_meta_column);
    } else if (column->has_sub_column) {
        db_log(ERROR, "Function %s not support for reference type column.", 
               GET_FUNCTION_TYPE_NAME(type));
        return false;
    }

    return true;
}

/* Check ident node. */
static bool CheckForColumn(ColumnNode *column_node, MetaTable *meta_table) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (StrEq(meta_column->column_name, column_node->column_name)) {
            if (column_node->has_sub_column == false)
                return true;
            else if (meta_column->column_type == T_REFERENCE && column_node->has_sub_column) {
                Table *table = open_table(meta_column->table_name);
                if (column_node->sub_column)
                    return CheckForColumn(column_node->sub_column, table->meta_table);
                else if (column_node->scalar_exp_list) {
                    ListCell *lc;
                    foreach (lc, column_node->scalar_exp_list) {
                        ScalarExpNode *scalar_exp = lfirst(lc);
                        switch (scalar_exp->type) {
                            case SCALAR_COLUMN:{
                                CheckForColumn(scalar_exp->column, table->meta_table);
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
        db_log(ERROR, "Unknown column '%s.%s'.", 
               column_node->range_variable, 
               column_node->column_name);
    else 
        db_log(ERROR, "Unknown column '%s'.", 
               column_node->column_name);

    return false;
}

/* Check function node */
static bool CheckForFunction(FunctionNode *function, AliasMap alias_map) {
    FunctionValueNode *value_node = function->value;
    switch(value_node->value_type) {
        case V_INT:
        case V_ALL:
            return true;
        case V_COLUMN: {
            ColumnNode *column = value_node->column;
            MetaTable *meta_table = ColumnFindMetaTable(column, alias_map);
            MetaColumn *meta_column = NameFindMetaColumn(meta_table, column->column_name);
            return CheckFunctionForValueType(function->type, column, meta_column) && 
                    CheckForColumn(column, meta_table); 
        }
        default: {
            UNEXPECTED_VALUE(value_node->value_type);
            return false;
        }
    }
}

/* Check CalculateNode. */
static bool CheckForCalculate(CalculateNode *calculate_node, AliasMap alias_map) {
    return CheckForScalarExp(calculate_node->left, alias_map) && 
            CheckForScalarExp(calculate_node->right, alias_map);
}

/* Check ScalarExpNode if column. */
static bool CheckForScalarExp(ScalarExpNode *scalar_exp, AliasMap alias_map) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:{
            ColumnNode *column = scalar_exp->column;
            MetaTable *meta_table = ColumnFindMetaTable(column, alias_map);
            return CheckForColumn(scalar_exp->column, meta_table);
        }
        case SCALAR_FUNCTION:
            return CheckForFunction(scalar_exp->function, alias_map);
        case SCALAR_CALCULATE:
            return CheckForCalculate(scalar_exp->calculate, alias_map);
        case SCALAR_VALUE:
            return true;
        default: {
            UNEXPECTED_VALUE("Unknown Scalar Express type.");
            return false;
        }
    }
}

/* Check ScalarExpNode list. */
static bool CheckForScalarExpList(List *scalar_exp_list, AliasMap alias_map) {
    ListCell *lc;
    foreach (lc, scalar_exp_list) {
        ScalarExpNode *scalar_exp = lfirst(lc);
        if (!CheckForScalarExp(scalar_exp, alias_map))
            return false;
    }

    return true;
}

/* Check select items if exist int meta column */
static bool CheckForSelection(SelectionNode *selection_node, AliasMap alias_map) {
    return selection_node->all_column 
           ? true 
           : CheckForScalarExpList(selection_node->scalar_exp_list, alias_map);
}

static bool CheckCalculateInSearchCondition(CalculateNode *calculate) {
    return CheckSclarExpInSearchCondition(calculate->left) && 
                CheckSclarExpInSearchCondition(calculate->right);
}

/* Check function in search condition. */
static bool CheckFuntionInSearchCondition(FunctionNode *function) {
    if (IsAggFuncion(function->type)) {
        db_log(ERROR, "Aggregate function not allowd in where.");
        return false;
    }
    return true;
}

/* Check atom in search condition.  
 * Note: this function mainly aims to check 
 * if exists indirect refer value in search condition. */
static bool CheckAtomInSeachCondition(AtomNode *atom) {
    if (atom->type == A_REFERENCE) {
        ReferValue *referVal = atom->value.referval;
        switch (referVal->type) {
            case INDIRECTLY:
                return true;
            case DIRECTLY: {
                db_log(ERROR, "Not allowed use directly refer value in search condition.");
                return false;
            }
            default:
                UNEXPECTED_VALUE(referVal->type);
        }
    }
    return true;
}

/* Check value item in search condition. */
static bool CheckValueItemInSearchCondition(ValueItemNode *value_item) {
    switch (value_item->type) {
        case V_ATOM:
            return CheckAtomInSeachCondition(value_item->value.atom);
        case V_ARRAY: {
            ListCell *lc;
            foreach (lc, value_item->value.value_list) {
                if (!CheckAtomInSeachCondition((AtomNode*) lfirst(lc)))
                    return false;
            }
            return true;
        }
        case V_NULL:
            return true;
        default:
            UNEXPECTED_VALUE(value_item->type);
    }
}

/* Check sclar exp in search condition.*/
static bool CheckSclarExpInSearchCondition(ScalarExpNode *scalar_exp) {
    switch (scalar_exp->type) {
        case SCALAR_COLUMN:
            return true;
        case SCALAR_VALUE:
            return CheckValueItemInSearchCondition(scalar_exp->value);
        case SCALAR_CALCULATE:
            return CheckCalculateInSearchCondition(scalar_exp->calculate);
        case SCALAR_FUNCTION: 
            return CheckFuntionInSearchCondition(scalar_exp->function); 
        default: {
            UNEXPECTED_VALUE(scalar_exp->type);
            return false;
        }
    }
}

/* Check ComparisonNode.*/
static bool CheckForComparisonPredicate(ComparisonNode *comparison, AliasMap alias_map) {
    return CheckSclarExpInSearchCondition(comparison->left) && 
                CheckSclarExpInSearchCondition(comparison->right);
}

/* Check in for value list.*/
static bool CheckInPredicateForValueList( List *value_list) {
    ListCell *lc;
    foreach (lc, value_list) {
        if (!CheckValueItemInSearchCondition((ValueItemNode *) lfirst(lc)))
            return false;
    }
    return true;
}

/* Check InNode. */
static bool CheckForInPredicate(InNode *in_node, AliasMap alias_map) {
    ColumnNode *column = in_node->column;
    /* Confirm MetaTable. */
    MetaTable *current_meta_table = ColumnFindMetaTable(column, alias_map);

    return CheckForColumn(in_node->column, current_meta_table) && // check select column
                CheckInPredicateForValueList(in_node->value_list) &&
                    CheckForValueList(current_meta_table, column->column_name, in_node->value_list);
}

/* Check like data type. */
static bool CheckLikePredicateType(MetaColumn *meta_column) {
    if (meta_column->column_type != T_STRING && meta_column->column_type != T_VARCHAR) {
        db_log(ERROR, "For like predicate, only support string data type.");
        return false;
    }
    return true;
}

/* Check LikeNode. */
static bool CheckForLikePredicate(LikeNode *like_node, AliasMap alias_map) {
    ColumnNode *column = like_node->column;

    /* Confirm MetaTable. */
    MetaTable *current_meta_table = ColumnFindMetaTable(column, alias_map);
    MetaColumn *meta_column = NameFindMetaColumn(current_meta_table, column->column_name);

    return CheckForColumn(column, current_meta_table) && // check select column
                CheckLikePredicateType(meta_column) && 
                    CheckForValueItem(current_meta_table, column->column_name, like_node->value);
}

/* Check PredicateNode. */
static bool CheckForPredicate(PredicateNode *predicate_node, AliasMap alias_map) {
    switch (predicate_node->type) {
        case PRE_COMPARISON:
            return CheckForComparisonPredicate(predicate_node->comparison, alias_map);
        case PRE_IN:
            return CheckForInPredicate(predicate_node->in, alias_map);
        case PRE_LIKE:
            return CheckForLikePredicate(predicate_node->like, alias_map);
        default:
            UNEXPECTED_VALUE(predicate_node->type);
            return false;
    }
}

/* Check boolean primary. */
static bool CheckForBooleanPrimary(BooleanPrimaryNode *boolean_primary, AliasMap alias_map) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return CheckForPredicate(boolean_primary->predicate, alias_map);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return CheckForSearchCondition(boolean_primary->search_condition, alias_map);
        default:
            UNEXPECTED_VALUE(boolean_primary->type);
            return false;
    }
}

/* Check boolean test. */
static bool CheckForBooleanTest(BooleanTestNode *boolean_test, AliasMap alias_map) {
    return CheckForBooleanPrimary(boolean_test->boolean_primary, alias_map);
}

/* Check boolean factor. */
static bool CheckForBooleanFactor(BooleanFactorNode *boolean_factor, AliasMap alias_map) {
    return CheckForBooleanTest(boolean_factor->boolean_test, alias_map);
}

/* Check boolean term. */
static bool CheckForBooleanTerm(BooleanTermNode *boolean_term, AliasMap alias_map) {
    return boolean_term->and_boolean_term == NULL
        ? CheckForBooleanFactor(boolean_term->boolean_factor, alias_map)
        : CheckForBooleanTerm(boolean_term->and_boolean_term, alias_map) &&
            CheckForBooleanFactor(boolean_term->boolean_factor, alias_map);
}

/* Check condition node. */
static bool CheckForSearchCondition(SearchConditionNode *condition_node, AliasMap alias_map) {
    if (!condition_node)
        return true;

    return condition_node->or_search_condition == NULL
        ? CheckForBooleanTerm(condition_node->boolean_term, alias_map)
        : CheckForBooleanTerm(condition_node->boolean_term, alias_map) &&
            CheckForSearchCondition(condition_node->or_search_condition, alias_map);
}


/* Check TableRefNode. */
static bool CheckForTableRef(TableRefNode *table_ref) {
    Table *table = open_table(table_ref->table);
    if (table == NULL) {
        db_log(ERROR, "Table '%s' not exist.", table_ref->table);
        return false;
    }
    return true;
}

/* Check TableRef List. */
static bool CheckForTableRefList(List *list) {
    uint32_t len = len_list(list);
    uint32_t i, j;
    for (i = 0; i < len; i++) {
        TableRefNode *table_ref = lfirst(list_nth_cell(list, i));
        if (!CheckForTableRef(table_ref))
            return false;

        for (j = i + 1; j < len; j++) {
            TableRefNode *table_ref2 = lfirst(list_nth_cell(list, j));
            /* Check duplicate table. */
            if (StrEq(table_ref->table, table_ref2->table)) {
                db_log(ERROR, "Duplicate table '%s'. ", table_ref->table);
                return false;
            }
            /* Check duplicate table alias name. */
            if (table_ref->range_variable && table_ref2->range_variable 
                && StrEq(table_ref->range_variable, table_ref2->range_variable)) {
                db_log(ERROR, "Duplicate table alias name: '%s'. ", table_ref->range_variable);
                return false;
            }
        }
    }

    return true;
}

/* Check FromClauseNode. */
static bool CheckForFromClause(FromClauseNode *from_clause) {
    return from_clause == NULL || CheckForTableRefList(from_clause->from);
}

/* Check WhereClauseNode. */
static bool CheckForWhereClause(WhereClauseNode *where_clause, AliasMap alias_map) {
    if (where_clause == NULL)
        return true;

    return CheckForSearchCondition(where_clause->condition, alias_map);
}

/* Check LimitClauseNode. */
static bool CheckForLimitClause(LimitClauseNode *limit_clause) {
    if (NonNull(limit_clause)) {
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
static bool CheckForTableExp(TableExpNode *table_exp, AliasMap alias_map) {
    return CheckForFromClause(table_exp->from_clause) && 
                CheckForWhereClause(table_exp->where_clause, alias_map) && 
                    CheckForLimitClause(table_exp->limit_clause);
}

/* Check update unique column. */
static bool CheckUpdateForUniqueColumn(Table *table, MetaColumn *meta_column, void *value, UpdateNode *update_node) {
    Assert(meta_column->is_unique);

    SelectResult *select_result;
    SearchConditionNode *condition;

    /* Although this cehck update node, but new select result is SELECT_STMT. */
    select_result = new_select_result(SELECT_STMT, update_node->table_name, true);
    condition = WhereClauseFindSearchCondition(update_node->where_clause);
    QueryUnderSearchCondition(select_result, SimpleSelectPlan(SelectRow, ARG_NULL, NULL, condition));

    /* If selected rows more than one, 
     * which means at least two rows has same value.*/
    if (select_result->row_size > 1) {
        db_log(ERROR, "Column '%s' is unique, not allowd duplicate.", 
               meta_column->column_name);
        return false;
    } else if (select_result->row_size == 1) {
        MetaColumn *primary_column = MetaTableFindPrimaryKey(table->meta_table);
        Row *selected_row = qfirst(select_result->rows->head);
        SelectResult *result = SelectWithColumnValue(GET_TABLE_OID(table), meta_column, value);
        QueueCell *qc;
        qforeach(qc, result->rows) {
            Row *row = (Row *) qfirst(qc);
            void *key = RowFindKey(row, table->meta_table);
            void *target_key = RowFindKey(selected_row, table->meta_table);
            if (NE(GetComparableValue(key, primary_column->column_type), 
                   GetComparableValue(target_key, primary_column->column_type), 
                   primary_column->column_type)
            ) {
                db_log(ERROR, "Key '%s' already exists, not allowd duplicate key. ",
                       KeyGetStrValue(value, meta_column->column_type));
                return false;
            } 
        }
    }
    return true;
}

/* Check assignment set node */
static bool CheckUpdateForAssignmentList(UpdateNode *update_node) { 
    Table *table = open_table(update_node->table_name);
    List *assignment_list = update_node->assignment_list;

    ListCell *lc;
    foreach (lc, assignment_list) {
        AssignmentNode *assignment_node = lfirst(lc);
        ColumnNode *column_node = assignment_node->column;
        ValueItemNode *value_node = assignment_node->value;
        void *assign_value = NULL;
        Assert(column_node != NULL);

        MetaColumn *meta_column = NameFindMetaColumn(table->meta_table, column_node->column_name);
        if (IsNull(meta_column)) {
            db_log(ERROR, "Not found column %s in table %s.", 
                   column_node->column_name, 
                   GET_TABLE_NAME(table));
            return false;
        }
        assign_value = ValueItemNodeFindValue(value_node);

        /* Check column, check type, check if value valid. */
        if (!(CheckForColumn(column_node, table->meta_table) && 
                CheckForValueItem(table->meta_table, meta_column->column_name, value_node))) 
            return false;

        /* Check duplicate column value if current column is unque.*/
        if (meta_column->is_unique) {
            if (!CheckUpdateForUniqueColumn(table, meta_column, assign_value, update_node))
                return false;
        }
    }

    return true;
}

/* Check if system reserved table. */
static bool AvoidSysReservedTableName(char *table_name) {
    if (if_table_reserved(table_name)) {
        db_log(ERROR, "Table '%s' is system reserved, not allowd duplication.", table_name); 
        return false;
    }
    return true;
}

/* Check if table alreay exist. */
static bool TableAlreadyExists(char *table_name) {
    if (check_table_exist(table_name)) {
        db_log(ERROR, "Table '%s' already exists.", table_name); 
        return false;
    } 
    else 
        return true;
}

/* Check if column already exists. */
static bool ColumnDefAlearyExists(List *list, ColumnDefNode *column_def) {
    ListCell *lc;
    foreach (lc, list) {
        ColumnDefNode *current_column_def = lfirst(lc);
        if (StrEq(current_column_def->column->column, column_def->column->column))
            return true;
    }
    return false;
}


/* Check if ColumnDefOptNodeList contains primary key. */
static bool ColumnDefListContainesPrimaryKey(List *column_def_opt_list) {
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

/* Check if atom_node is match .*/
static bool CheckForDefaultAtomValueType(AtomNode *atom_node, DataType data_type) {
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
        case T_REFERENCE:  {
            /* For Reference, it`s complicate. We not support subrow value. 
             * The logic is not appended sub row by the system instead of users. 
             * But we support for refer value, but it must exist. */
            ReferValue *refer_value = atom_node->value.referval;
            switch (refer_value->type) {
                case DIRECTLY: 
                    db_log(ERROR, "Default value does not support directly subrow value. You can try indirect refer value, but make sure the referenct exists.");
                    return false;
                case INDIRECTLY: 
                    return true;
            }
        }
        default:
            UNEXPECTED_VALUE(data_type);
    }

    return false;
}

/* Check default value type. */
static bool CheckForDefaultValueType(ValueItemNode *value_item_node, DataType data_type) {
    switch (value_item_node->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item_node->value.atom;
            return CheckForDefaultAtomValueType(atom_node, data_type);
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
static bool CheckForColumnDef(ColumnDefNode *column_def) {
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
                    DataType data_type = ColumnDefFindType(column_def);
                    if (!CheckForDefaultValueType(value_item_node, data_type)) {
                        db_log(ERROR, "Invalid default value for '%s', can`t convert to '%s'.", 
                               ColumnDefFindName(column_def),
                               GET_DATA_TYPE_NAME(data_type));
                        return false;
                    }
                    break;
                } 
                case OPT_COMMENT: {
                    if (strlen(column_def_opt->comment) > MAX_COMMENT_STRING_LENGTH) {
                        db_log(ERROR, "Too long comment for '%s'.", 
                               ColumnDefFindName(column_def));
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
                   ColumnDefFindName(column_def));
            return false;
        }
    }

    return true;
}

/* Check if exists duplicate column name. */
static bool CheckForBaseTableElementCommalist(List *base_table_element_commalist) {
    List *list = create_list(NODE_COLUMN_DEF);
    bool primary_key_flag = false;

    ListCell *lc;
    foreach (lc, base_table_element_commalist) {
        BaseTableElementNode *base_table_element = lfirst(lc);
        switch (base_table_element->type) {
            case TELE_COLUMN_DEF: {
                ColumnDefNode *current_column_def = base_table_element->column_def;
                if (ColumnDefAlearyExists(list, current_column_def)) {
                    free_list(list);
                    db_log(ERROR, "Column '%s' already exists, not allowd duplicate defination.", 
                           ColumnDefFindName(current_column_def));
                    return false;
                }
                if (ColumnDefListContainesPrimaryKey(current_column_def->column_def_opt_list)) {
                    if (primary_key_flag) 
                    {
                        free_list(list);
                        db_log(ERROR, "Dulicate primary key.");
                        return false;
                    } 
                    else
                        primary_key_flag = true;
                }
                if (!CheckForColumnDef(current_column_def)) {
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
static bool CheckInsertForValueItems(InsertNode *insert_node, List *value_item_list) {
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

        ListCell *lc;
        foreach (lc, meta_table->meta_columns) {
            MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
            if (meta_column->sys_reserved)
                continue;
            ValueItemNode *value_item_node = lfirst(list_nth_cell(value_item_list, __i));
            if (!CheckForValueItem(meta_table, meta_column->column_name, value_item_node))
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
            MetaColumn *meta_column = NameFindMetaColumn(meta_table, column_node->column_name);
            if (IsNull(meta_column)) {
                db_log(ERROR, "Unknown column '%s'", column_node->column_name);
                return false;
            }
            if (!CheckForValueItem(meta_table, meta_column->column_name, value_item_node))
                return false;
        }

    }

    return true;
}

/* Check InsertNode for VALUES. */
static bool CheckInsertForValues(InsertNode *insert_node, List *value_list) {
    ListCell *lc;
    foreach (lc, value_list) {
        List *value_item_list = lfirst(lc);
        if (!CheckInsertForValueItems(insert_node, value_item_list))
            return false;
    }
    return true;
}


/* Check InsertNode for QUERY_SPEC. */
static bool CheckInsertForQuerySpec(InsertNode *insert_node, QuerySpecNode *query_spec) {
    /* Check table exist.*/
    Table *table = open_table(insert_node->table_name);
    if (table == NULL)
        return false;

    if (insert_node->all_column) {
        MetaTable *meta_table = table->meta_table;
        ListCell *lc;
        foreach (lc, meta_table->meta_columns) {
            MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
            if (!CheckQuerySpecMatchColumn(meta_column, query_spec))
                return false;
        }
    } else {
        ListCell *lc;
        foreach (lc, insert_node->column_list) {
            ColumnNode *column_node = lfirst(lc);
            MetaColumn *meta_column = NameFindMetaColumn(table->meta_table, column_node->column_name);
            if (!CheckQuerySpecMatchColumn(meta_column, query_spec))
                return false;
        }
    }
    return true;
}

/* Check ValuesOrQuerySpecNode in InsertNode. */
static bool CheckInsertForValuesOrQuerySpec(InsertNode *insert_node, ValuesOrQuerySpecNode *values_or_query_spec) {
    switch (values_or_query_spec->type) {
        case VQ_VALUES:
            return CheckInsertForValues(insert_node, values_or_query_spec->values);
        case VQ_QUERY_SPEC:
            return CheckInsertForQuerySpec(insert_node, values_or_query_spec->query_spec);
        default:
            db_log(ERROR, "Unknown ValuesOrQuerySpecNode type");
            return false;
    }

}

/* Get column name for AddColumnDef. */
static inline char *AddColumnDefFindColumnName(AddColumnDef *add_column) {
    return add_column->column_def->column->column;
}

/* Check alter table add-column action*/
static bool CheckForAddColumn(char *table_name, AddColumnDef *add_column) {
    char *column_name = AddColumnDefFindColumnName(add_column);
    /* Check add column if exists. */
    if (ColumnExistsInTable(column_name, table_name)) {
        db_log(ERROR, "Table '%s' already exists column '%s'.", 
               table_name, 
               column_name);
        return false;
    }

    /* Check if the position column def exists. */
    if (!IsNull(add_column->position_def) 
            && !ColumnExistsInTable(add_column->position_def->column, table_name)) {
        db_log(ERROR, "Unknown column '%s' in table '%s'.", 
               add_column->position_def->column, 
               table_name);
        return false;
    }
    
    /* Check the column def. */
    if (!CheckForColumnDef(add_column->column_def)) {
        return false;
    }

    /* Check if add primary-key column, not support yet.*/
    return true;    
}

/* Check for dorp-column action. */
static bool CheckForDropColumn(char *table_name, DropColumnDef *drop_column_def) {
    Table *table;
    MetaTable *meta_table;
    MetaColumn *meta_column;

    table = open_table(table_name);
    meta_table = table->meta_table;
    meta_column = NameFindMetaColumn(meta_table, drop_column_def->column_name);

    /* Check drop column if exists. */
    if (IsNull(meta_column)) {
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
static bool CheckForAlterTableAction(char *table_name, AlterTableAction *action) {
    switch (action->type) {
        case ALTER_TO_ADD_COLUMN:
            return CheckForAddColumn(table_name, action->action.add_column);
        case ALTER_TO_DROP_COLUMN:
            return CheckForDropColumn(table_name, action->action.drop_column);
    }
    return true;
}

/* Check table. */
static bool CheckForTable(char *table_name) {
    if (check_table_exist(table_name))
        return true;
    else {
        db_log(ERROR, "Table '%s' not exists.", table_name);
        return false;
    }
}

/* Check if table uses refer. */
static bool TableIsRefered(Table *table, char *refer_table_name) {
    MetaTable *meta_table = table->meta_table;

    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->sys_reserved)
            continue;
        if (meta_column->column_type == T_REFERENCE && strcmp(meta_column->table_name, refer_table_name) == 0) {
            db_log(ERROR , "Table '%s' is refered by column '%s' in table '%s', so can`t drop it.", 
                   refer_table_name, meta_column->column_name, table->meta_table->table_name);
            return true;
        }
    }

    return false;
}

/* Check SelectNode. */
bool CheckForSelect(SelectNode *select_node) {
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

    return CheckForTableExp(select_node->table_exp, alias_map) && 
            CheckForSelection(select_node->selection, alias_map);
}


/* Check insert node. */
bool CheckForInsert(InsertNode *insert_node) {
    return CheckForTable(insert_node->table_name)
           && CheckInsertForValuesOrQuerySpec(insert_node, insert_node->values_or_query_spec); 
}

/* Check for update node. */
bool CheckForUpdate(UpdateNode *update_node) {
    AliasMap alias_map;
    alias_map.size = 1;
    alias_map.map[0].name = update_node->table_name;
    alias_map.map[0].alias = update_node->table_name;

    return CheckForTable(update_node->table_name) && 
                CheckUpdateForAssignmentList(update_node) && 
                    CheckForWhereClause(update_node->where_clause, alias_map);
}

/* Check for delete node. */
bool CheckForDelete(DeleteNode *delete_node) {
    /* Unimplement. */
    AliasMap alias_map;
    alias_map.size = 1;
    alias_map.map[0].name = delete_node->table_name;
    alias_map.map[0].alias = delete_node->table_name;

    return CheckForTable(delete_node->table_name) && 
            CheckForSearchCondition(delete_node->condition_node, alias_map);
}

/* Check for create table node. */
bool CheckForCreateTable(CreateTableNode *create_table_node) {
    return AvoidSysReservedTableName(create_table_node->table_name) && 
            TableAlreadyExists(create_table_node->table_name) && 
                CheckForBaseTableElementCommalist(create_table_node->base_table_element_commalist);
}


/* Check allowed to drop table. */
bool CheckForDropTable(char *table_name) {
    bool ret = true;

    /* Check table exists. */
    if (!check_table_exist(table_name)) {
        db_log(ERROR, "Table '%s' not exists.", table_name);
        return false;
    }
    
    /* Check table refered by others. */
    List *table_list = GetAllTableCache();

    ListCell *lc;
    foreach (lc, table_list) {
        Table *table = (Table *) lfirst(lc);
        if (TableIsRefered(table, table_name))  {
            ret = false;
            break;
        }
    }

    return ret;
}

/* Check for AlterTableNode. */
bool CheckForAlterTable(AlterTableNode *alter_table) {
    return CheckForTable(alter_table->table_name)
            && CheckForAlterTableAction(alter_table->table_name, alter_table->action);
}
