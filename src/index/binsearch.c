#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "binsearch.h"
#include "bin.h"
#include "bufmgr.h"
#include "mmgr.h"
#include "log.h"
#include "select.h"
#include "meta.h"
#include "table.h"
#include "tuple.h"
#include "trans.h"
#include "copy.h"
#include "row.h"
#include "index.h"
#include "heaptable.h"
#include "instance.h" 
#include "compare.h"

static void BinSearchUnderConditionInner(MetaIndex *meta_index, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan);
static bool BinSearchInternalNodeForSearchCondition(SelectPlan *select_plan, void *min_key, void *max_key, SearchConditionNode *condition);

/* Check if LimitClauseNode is full. 
 * LimitClauseNode full means the poffset >= the offset.
 * */
static inline bool LimitClauseIsFull(SelectPlan *select_plan) {
    return NonNull(select_plan->limitClause) && 
        (select_plan->offset >= select_plan->limitClause->offset + select_plan->limitClause->rows);
}

/* Merge meta columns. */
static List *MergeMetaColumns(SelectResult *head) {
    Assert(head != NULL);

    List *meta_columns = create_list(NODE_META_COLUMN);
    SelectResult *current = head;
    Size offset = 0, tuple_size = 0;;

    while (current != NULL) {
        Table *table;
        ListCell *lc;

        table = open_table(current->table_name);
        foreach (lc, table->meta_table->meta_columns) {
            MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
            MetaColumn *duplica = copy_meta_column(meta_column);
            duplica->offset = offset;
            append_list(meta_columns, duplica);
            offset += meta_column->column_length;
            tuple_size += meta_column->column_length;
        }
        current = current->nested;
    }
    head->tuple_size = tuple_size;

    return meta_columns;
}


/* Merge two tuples. */
static void *MergeTuple(SelectResult *head) {
    if (head->nested == NULL)
        return head->current_tuple;
    else {
        Size offset;
        SelectResult *current;
        void *ntuple;
        
        Assert(head->tuple_size != 0);
        ntuple = dalloc(head->tuple_size);
        offset = 0;
        current = head;

        while (current != NULL) {
            Table *table = open_table(current->table_name);
            memcpy(ntuple + offset, current->current_tuple, table->heap_value_len);
            offset += table->heap_value_len;
            current = current->nested;
        }

        return ntuple;
    }
}

/* Query tuple value item. */
static KeyValue *QueryTupleValueItem(ValueItemNode *value_item) {
    void *value = ValueItemNodeFindValue(value_item);
    return value == NULL 
        ? new_key_value(NULL, value, T_UNKNOWN, OID_ZERO, OID_ZERO)
        : new_key_value(NULL, value, AtomTypeConvertDataType(value_item->value.atom->type), OID_ZERO, OID_ZERO);
}

/* Check if value item is a refer value. */
static bool ValueItemIsReferValue(ValueItemNode *value_item) {
    switch (value_item->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item->value.atom;
            return atom_node->type == A_REFERENCE;
        }
        case V_ARRAY: {
            ListCell *lc;
            foreach(lc, value_item->value.value_list) {
                ValueItemNode *value_item = (ValueItemNode *)lfirst(lc);
                /* Any of it is ReferValue is true. */
                if (ValueItemIsReferValue(value_item))
                    return true;
            }
        }
        case V_NULL:
            return false;
    }
    return false;
} 

/* Index key to generate key value. */
static KeyValue *IndexKeyGenerateKeyValue(MetaIndex *meta_index, void *key, MetaColumn *meta_column) {
    int offset = 0;

    ListCell *lc;
    foreach(lc, meta_index->meta_columns) {
        MetaColumn *current = (MetaColumn *) lfirst(lc);
        if (current == meta_column) break;
        offset += current->column_length;
    }

    return new_key_value(
        meta_column->column_name, 
        GetComparableValue(key == NULL ? NULL : key + offset, meta_column->column_type),
        GetComparableType(meta_column->column_type), 
        meta_column->tid, meta_column->type_oid
    );
}


/* Search table via alias name in SelectResult. 
 * Note: range variable may be table name or table alias name. */
static Table *SearchTableViaAlias(SelectPlan *select_plan, char *alias_name) {
    if (select_plan->selectTableList != NIL) {
        ListCell *lc;
        foreach (lc, select_plan->selectTableList) {
            SelectTable *select_table = (SelectTable *)lfirst(lc);
            if (StrEq(select_table->alias_name, alias_name) || StrEq(GET_TABLE_NAME(select_table->table), alias_name))
                return select_table->table;
        }
    }
    return NULL;
}


static bool ColumnNodeHitMetaColumn(ColumnNode *column, SelectPlan *select_plan, MetaColumn *meta_column) {
    if (StrIsEmpty(column->range_variable))
        return StrEq(column->column_name, meta_column->column_name);
    Table *table = SearchTableViaAlias(select_plan, column->range_variable);
    if (table == NULL)
        db_log(ERROR, "Unknown column name %s.%s", column->range_variable, column->column_name);
    return meta_column->tid == GET_TABLE_OID(table) && StrEq(column->column_name, meta_column->column_name);
}

static bool ScalarExpIsReferValue(ScalarExpNode *scalar_exp) {
    if (scalar_exp->type == SCALAR_VALUE) {
        ValueItemNode *value_item = scalar_exp->value;
        return ValueItemIsReferValue(value_item);
    }
    return false;
}

/* If satisfy columnn and refer value in comparison. */
static bool SatisfyColumnAndReferValueCompparison(ScalarExpNode *left, ScalarExpNode *right) {
    if (ScalarExpIsReferValue(left)) {
        if (right->type == SCALAR_COLUMN)
            return true;
        else
            db_log(ERROR, "Refer value must compare with column.");
    } else if (ScalarExpIsReferValue(right)) {
        if (left->type == SCALAR_COLUMN)
            return true;
        else
            db_log(ERROR, "Refer value must compare with column.");
    }
    return false;
}

/* Check if the internal comparison meets comparison. */
static bool BinSearchInternalNodeForComparisonPredicate(SelectPlan *select_plan, void *min_key, void *max_key, ComparisonNode *comparison, bool negation, MetaColumn *meta_column) {
    /* Refer value will cause index invalid */
    if (SatisfyColumnAndReferValueCompparison(comparison->left, comparison->right))
        return true;
    
    ScalarExpNode *left = comparison->left;
    ScalarExpNode *right = comparison->right;
    switch (comparison->type) {
        case O_EQ: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(right->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value) 
                        : true; 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(left->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value) 
                        : true;
            }
            break;
        }
        case O_NE: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(right->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? true
                        : KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value);
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(left->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? true
                        : KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value); 
            }
            break;
        }
        case O_GT: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(right->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_GT, max_key_value, value) 
                        : KeyValueEval(O_LT, min_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(left->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_LT, min_key_value, value) 
                        : KeyValueEval(O_GT, max_key_value, value);
            }
            break;
        }
        case O_GE: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(right->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_GE, max_key_value, value) 
                        : KeyValueEval(O_LE, min_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(left->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_LE, min_key_value, value) 
                        : KeyValueEval(O_GE, max_key_value, value);
            }
            break;
        }
        case O_LT: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(right->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index,min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_LT, min_key_value, value) 
                        : KeyValueEval(O_GT, max_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(left->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_GT, max_key_value, value) 
                        : KeyValueEval(O_LT, min_key_value, value); 
            }
            break;
        }
        case O_LE: {
            if (left->type == SCALAR_COLUMN && right->type == SCALAR_VALUE) {
                ColumnNode *column = left->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(right->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_LE, min_key_value, value) 
                        : KeyValueEval(O_GE, max_key_value, value); 
            } else if (left->type == SCALAR_VALUE && right->type == SCALAR_COLUMN) {
                ColumnNode *column = right->column;
                if (!ColumnNodeHitMetaColumn(column, select_plan, meta_column)) 
                    return true;
                KeyValue *value = QueryTupleValueItem(left->value);
                KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
                KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
                return !negation 
                        ? KeyValueEval(O_GE, max_key_value, value) 
                        : KeyValueEval(O_LE, min_key_value, value); 
            }
            break;
        }
        default:
            UNEXPECTED_VALUE(comparison->type);
    }

    return true;
}

/* Check if the internal node meets like predicate. */
static bool BinSearchInternalNodeForLikePredicate(SelectPlan *select_plan, void *min_key, KeyValue *max_key, LikeNode *like, bool negation, MetaColumn *meta_column) { 
    /* For not like operation, it`s realy hard not to fall into the scope of internal node, 
     * which means everyone in the scope of internal node is like to the target value. 
     * Absolutely, it`s not possible so just return true. */
    if (negation) return true;

    ColumnNode *column;
    KeyValue *value, *min_key_value, *max_key_value;
    char *strVal, *newStrVal;
    Size len;

    column = like->column;
    value = QueryTupleValueItem(like->value);
    max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
    min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
    strVal = value->value;
    len = strlen(strVal);
    AssertFalse(strVal[0] == '%');

    /* Trim right '%s' character if necessary.  */
    if (strVal[len - 1] == '%') {
        newStrVal = dstrdup(strVal);
        newStrVal[len - 1] = '\0';
        value->value = newStrVal;
    }

    if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key))
        return !negation 
                ? KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value) 
                : KeyValueEval(O_GE, min_key_value, value) || KeyValueEval(O_LT, max_key_value, value); 
    return true;
}


/* Check if the internal node meets in predicate. */
static bool BinSearchInternalNodeForInPredicate(SelectPlan *select_plan, void *min_key, void *max_key, InNode *in, bool negation, MetaColumn *meta_column) {
    /* For not in operation, it`s realy hard not to fall into the scope of internal node. 
     * Because it`s the whole world escape the in operation target values, so just return true. */
    if (negation) return true;

    ColumnNode *column = in->column;
    KeyValue *max_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, max_key, meta_column);
    KeyValue *min_key_value = IndexKeyGenerateKeyValue(select_plan->meta_index, min_key, meta_column);
    if (StrEq(column->column_name, min_key_value->key) && StrEq(column->column_name, max_key_value->key)) {
        ListCell *lc;
        foreach (lc, in->value_list) {
            KeyValue *value = QueryTupleValueItem((ValueItemNode *) lfirst(lc));
            if (KeyValueEval(O_LT, min_key_value, value) && KeyValueEval(O_GE, max_key_value, value))
                return true;
        }
        return false;
    }
    return true;
}


/* Check if the internal node meets predicate. */
static bool BinSearchInternalNodeForPredicate(SelectPlan *select_plan, void *min_key, void *max_key, PredicateNode *predicate, bool negation, MetaColumn *meta_column) {
    switch (predicate->type) {
        case PRE_COMPARISON:
            return BinSearchInternalNodeForComparisonPredicate(
                select_plan, min_key, max_key, 
                predicate->comparison, negation, meta_column
            );
        case PRE_LIKE:
            return BinSearchInternalNodeForLikePredicate(
                select_plan, min_key, max_key, 
                predicate->like, negation, meta_column
            );
        case PRE_IN:
            return BinSearchInternalNodeForInPredicate(
                select_plan, min_key, max_key, 
                predicate->in, negation, meta_column
            );
        default:
            UNEXPECTED_VALUE(predicate->type);
            return false;
    }
}

/* Check if the internal node meets the boolean primary. */
static bool BinSearchInternalNodeForBooleanPrimary(SelectPlan *select_plan, void *min_key, void *max_key, BooleanPrimaryNode *boolean_primary, bool negation, MetaColumn *meta_column) {
    switch (boolean_primary->type) {
        case PREDICATE_BOOLEAN_PRIMAYR:
            return BinSearchInternalNodeForPredicate(select_plan, min_key, max_key, boolean_primary->predicate, negation, meta_column);
        case SEARCH_CONDITION_BOOLEAN_PRIMAYR:
            return !negation 
                    ? BinSearchInternalNodeForSearchCondition(select_plan, min_key, max_key, boolean_primary->search_condition) 
                    : true;
        default:
            UNEXPECTED_VALUE(boolean_primary->type);
            return false;
    } 
}

/* Check if the internal node meets the boolean test. */
static bool BinSearchInternalNodeForBooleanTest(SelectPlan *select_plan, void *min_key, void *max_key, BooleanTestNode *boolean_test, bool negation, MetaColumn *meta_column) {
    switch (boolean_test->type) {
        case NONE_TRUE_VALUE: 
            return BinSearchInternalNodeForBooleanPrimary(select_plan, min_key, max_key, boolean_test->boolean_primary, negation, meta_column);
        case IS_TRUTH_VALUE: 
            return BinSearchInternalNodeForBooleanPrimary(
                select_plan, min_key, max_key, boolean_test->boolean_primary, 
                /* The following is XOR. */
                negation == boolean_test->truth_value, 
                meta_column
            );
        case IS_NOT_TRUTH_VALUE: 
            return BinSearchInternalNodeForBooleanPrimary(
                select_plan, min_key, max_key, boolean_test->boolean_primary, 
                /* The following is XOR. */
                negation != boolean_test->truth_value,
                meta_column
            );
        default:
            UNEXPECTED_VALUE(boolean_test->type);
            return true;
    }
}

/* Check if the internal node meets the boolean fator. */
static bool BinSearchInternalNodeForBooleanFactor(SelectPlan *select_plan, void *min_key, void *max_key, BooleanFactorNode *boolean_factor) {
    bool flag = true;
    
    ListCell *lc;
    foreach(lc, select_plan->meta_index->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        flag = flag && BinSearchInternalNodeForBooleanTest(select_plan, min_key, max_key, boolean_factor->boolean_test, boolean_factor->is_not, meta_column);
    }

    return flag;
}

/* Check if the internal node meets the boolean term. */
static bool BinSearchInternalNodeForBooleanTerm(SelectPlan *select_plan, void *min_key, void *max_key, BooleanTermNode *boolean_term) {
    return boolean_term->and_boolean_term == NULL 
        ? BinSearchInternalNodeForBooleanFactor(select_plan, min_key,  max_key, boolean_term->boolean_factor)
        : BinSearchInternalNodeForBooleanFactor(select_plan, min_key,  max_key, boolean_term->boolean_factor) && 
            BinSearchInternalNodeForBooleanTerm(select_plan, min_key, max_key, boolean_term->and_boolean_term);
}


static bool BinSearchInternalNodeForSearchCondition(SelectPlan *select_plan, void *min_key, void *max_key, SearchConditionNode *condition) {
    /* If index is invalid, just return true. */
    if (!select_plan->hit_index) return true;
    return condition->or_search_condition == NULL 
        ? BinSearchInternalNodeForBooleanTerm(select_plan, min_key, max_key, condition->boolean_term)
        : BinSearchInternalNodeForBooleanTerm(select_plan, min_key, max_key, condition->boolean_term) || 
            BinSearchInternalNodeForSearchCondition(select_plan, min_key, max_key, condition->or_search_condition);
}

/* Bin search under conditon for internal node. */
static void BinSearchUnderConditionForInternalNode(MetaIndex *meta_index, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
    Buffer buffer;
    void *internal_node, *high_key;
    uint32_t keys_num, i;

    /* If limit full, not continue.*/
    if (LimitClauseIsFull(select_plan)) return;   

    buffer = ReadBuffer(meta_index->oid, page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPageCopy(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    keys_num = BinInternalNodeGetKeysNum(internal_node);
    high_key = BinNodeGetHighKey(internal_node, meta_index->key_len, meta_index->value_len);
    
    for (i = 0; i < keys_num; i++) {
        /* Check if index column, use index to avoid full text scanning. */
        /* Current internal node cell key as max key, previous cell key as min key, so the the range of values is (min_key, max_key]. */

        uint32_t child_page_num;
        void *max_key, *min_key;

        max_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, i);
        min_key = (i == 0) ? NULL : BinInternalNodeGetCellKey(internal_node, meta_index->key_len, i - 1);

        /* Filter the internal node by the B+ Tree. */
        if (!BinSearchInternalNodeForSearchCondition(select_plan, min_key, max_key, select_plan->condition))
            continue;

        child_page_num = BinInternalNodeGetCellValue(internal_node, meta_index->key_len, i);
        Assert(child_page_num != 0);
        BinSearchUnderConditionInner(meta_index, child_page_num, max_key, select_result, select_plan);
    }

    /* Don`t forget the right child. */
    uint32_t right_child_page_num;
    void *right_high_key, *max_cell_key;

    right_child_page_num = BinInternalNodeGetRightNum(internal_node);
    right_high_key = BinInternalNodeGetRightKey(internal_node);
    max_cell_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, keys_num - 1);

    /* Filter the internal node by the B+ Tree. */
    if (BinSearchInternalNodeForSearchCondition(select_plan, max_cell_key, right_high_key, select_plan->condition))
        BinSearchUnderConditionInner(meta_index, right_child_page_num, right_high_key, select_result, select_plan);

    /* The only condition to move to sibling:
     * The target node has spliten. */
    if (CompareKey(meta_index, boundary_key, high_key) > 0) {
        uint32_t next_sibling = BinInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        BinSearchUnderConditionForInternalNode(meta_index, next_sibling, boundary_key, select_result, select_plan);
    }

    dfree(internal_node);
}

/* Bin search under conditon for leaf node. */
static void BinSearchUnderConditionForLeafNode(MetaIndex *meta_index, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
    Table *table;
    Buffer buffer;
    void *leaf_node;
    uint32_t cell_num, i;
    SelectResult *head, *nested;

    buffer = ReadBuffer(meta_index->oid, page_num);
    LockBuffer(buffer, RW_READERS);
    leaf_node = GetBufferPage(buffer);
    
    table = open_table_inner(meta_index->tid);
    head = select_result->head;
    nested = select_result->nested;
    cell_num = BinLeafNodeGetCellNum(leaf_node);

    for (i = 0; i < cell_num; i++) {
        Refer *refer;
        void *tuple;
        Xid created_xid, expired_xid;

        refer = BinLeafNodeGetCellValue(leaf_node, meta_index->key_len, meta_index->value_len, i);
        tuple = HeapTableLookupTuple(meta_index->tid, refer);
        created_xid = TupleFindCreatedXid(tuple, table->meta_table);
        expired_xid = TupleFindExpiredXid(tuple, table->meta_table);

        /* If not visible, skip it. */
        if (!IsVisible(created_xid, expired_xid)) continue;
        
        select_result->current_tuple = tuple;
        
        /* If has nested, deep seek nested. */
        if (nested != NULL) {
            BinSearchUnderCondition(nested, select_plan);
            continue;
        }

        List *columns;
        void *ntuple;

        if (head->columns != NULL) {
            columns = head->columns;
        } else {
            columns = MergeMetaColumns(head);
            head->columns = columns;
        }

        ntuple = MergeTuple(head);
        Assert(columns != NIL);
        Assert(ntuple != NULL);
        
        /* Filt the leaf node. */
        if (LeafNodeForSearchCondition(select_plan, columns, ntuple, select_plan->condition))
            select_plan->rowHanler(ntuple, head, select_plan->type, select_plan->arg);
        
        /* When nested not null, means ntuple dalloc new memory. */
        if (head->nested != NULL)
            dfree(ntuple);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

/* Bin search under conditon inner. */
static void BinSearchUnderConditionInner(MetaIndex *meta_index, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
    Buffer buffer;
    void *node;
    NodeType type;

    buffer = ReadBuffer(meta_index->oid, page_num);

    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer);
    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
    
    type = GetNodeType(node);
    switch (type) {
        case LEAF_NODE:
            return BinSearchUnderConditionForLeafNode(meta_index, page_num, boundary_key, select_result, select_plan);
            break;
        case INTERNAL_NODE:
            return BinSearchUnderConditionForInternalNode(meta_index, page_num, boundary_key, select_result, select_plan);
            break;
        default:
            UNEXPECTED_VALUE(type);
            break;
    }
}

/* Bin search under conditon. */
void BinSearchUnderCondition(SelectResult *select_result, SelectPlan *select_plan) {
    Assert(select_plan->hit_index);
    BinSearchUnderConditionInner(select_plan->meta_index, ROOT_PAGE_NUM, NULL, select_result, select_plan);
}
