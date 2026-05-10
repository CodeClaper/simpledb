#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "binsearch.h"
#include "bin.h"
#include "bufmgr.h"
#include "data.h"
#include "mmgr.h"
#include "log.h"
#include "optimizer.h"
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

void BinSearchUnderExprInner(MetaIndex *meta_index, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan);

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
            BinSearchUnderExpr(nested, select_plan);
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
            select_plan->rowHanler(ntuple, head, select_plan);
        
        /* When nested not null, means ntuple dalloc new memory. */
        if (head->nested != NULL)
            dfree(ntuple);
    }

    UnlockBuffer(buffer);
    ReleaseBuffer(buffer);
}

static void TrimRightEscapeForLikePredicate(KeyValue *value) {
    char *strval;
    size_t len;

    strval = value->value;
    len = strlen(strval);
    if (strval[len - 1] == '%') {
        char *newval = dstrdup(strval);
        newval[len - 1] = '\0';
        value->value = newval;
    }
}

static inline KeyValue *KeyValueGenerateByMetaColumn(MetaColumn *meta_column, void *value) {
    return new_simple_key_value(meta_column->column_name, GetComparableValue(value, meta_column->column_type), meta_column->column_type);
}

/* Bin search internal node when case comparison predicate for EQ. */
static bool BinSearchMetaColumnFallIntoComparisonPredicateForEQ(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    ScalarExpNode *left_scalar, *right_scalar;
    KeyValue *value;

    left_scalar = expr->leftVal;
    right_scalar = expr->rightVal;
    if (left_scalar->type == SCALAR_COLUMN && right_scalar->type == SCALAR_VALUE) {
        value = QueryTupleValueItem(right_scalar->value);
        return KeyValueEval(O_LT, min_value, value) && KeyValueEval(O_GE, max_value, value);
    } else if (left_scalar->type == SCALAR_VALUE && right_scalar->type == SCALAR_COLUMN) {
        value = QueryTupleValueItem(left_scalar->value);
        return KeyValueEval(O_LT, min_value, value) && KeyValueEval(O_GE, max_value, value);
    } else UNREACHABLE(NULL, "Logic error.");
}

/* Bin search internal node when case comparison predicate for NE. */
static bool BinSearchMetaColumnFallIntoComparisonPredicateForNE(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    return !BinSearchMetaColumnFallIntoComparisonPredicateForEQ(select_table_list, meta_column, min_value, max_value, expr);
}

/* Bin search internal node when case comparison predicate for GT. */
static bool BinSearchMetaColumnFallIntoComparisonPredicateForGT(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    ScalarExpNode *left_scalar, *right_scalar;
    KeyValue *value;

    left_scalar = expr->leftVal;
    right_scalar = expr->rightVal;
    if (left_scalar->type == SCALAR_COLUMN && right_scalar->type == SCALAR_VALUE) {
        value = QueryTupleValueItem(right_scalar->value);
        return KeyValueEval(O_GT, max_value, value);
    } else if (left_scalar->type == SCALAR_VALUE && right_scalar->type == SCALAR_COLUMN) {
        value = QueryTupleValueItem(left_scalar->value);
        return KeyValueEval(O_LT, min_value, value);
    } else UNREACHABLE(NULL, "Logic error.");
}

/* Bin search internal node when case comparison predicate for GE. */
static bool BinSearchMetaColumnFallIntoComparisonPredicateForGE(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    ScalarExpNode *left_scalar, *right_scalar;
    KeyValue *value;

    left_scalar = expr->leftVal;
    right_scalar = expr->rightVal;
    if (left_scalar->type == SCALAR_COLUMN && right_scalar->type == SCALAR_VALUE) {
        value = QueryTupleValueItem(right_scalar->value);
        return KeyValueEval(O_GE, max_value, value);
    } else if (left_scalar->type == SCALAR_VALUE && right_scalar->type == SCALAR_COLUMN) {
        value = QueryTupleValueItem(left_scalar->value);
        return KeyValueEval(O_LE, min_value, value);
    } else UNREACHABLE(NULL, "Logic error.");
}

/* Bin search internal node when case comparison predicate for LT. */
static bool BinSearchMetaColumnFallIntoComparisonPredicateForLT(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, void *max_value, ExprNode *expr) {
    ScalarExpNode *left_scalar, *right_scalar;
    KeyValue *value;

    left_scalar = expr->leftVal;
    right_scalar = expr->rightVal;
    if (left_scalar->type == SCALAR_COLUMN && right_scalar->type == SCALAR_VALUE) {
        value = QueryTupleValueItem(right_scalar->value);
        return KeyValueEval(O_LT, min_value, value);
    } else if (left_scalar->type == SCALAR_VALUE && right_scalar->type == SCALAR_COLUMN) {
        value = QueryTupleValueItem(left_scalar->value);
        return KeyValueEval(O_GT, max_value, value);
    } else UNREACHABLE(NULL, "Logic error.");
}

/* Bin search internal node when case comparison predicate for LE. */
static bool BinSearchMetaColumnFallIntoComparisonPredicateForLE(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    ScalarExpNode *left_scalar, *right_scalar;
    KeyValue *value;

    left_scalar = expr->leftVal;
    right_scalar = expr->rightVal;
    if (left_scalar->type == SCALAR_COLUMN && right_scalar->type == SCALAR_VALUE) {
        value = QueryTupleValueItem(right_scalar->value);
        return KeyValueEval(O_LE, min_value, value);
    } else if (left_scalar->type == SCALAR_VALUE && right_scalar->type == SCALAR_COLUMN) {
        value = QueryTupleValueItem(left_scalar->value);
        return KeyValueEval(O_GE, max_value, value);
    } else UNREACHABLE(NULL, "Logic error.");
}

/* Bin search internal node when case like predicate.. */
static bool BinSearchMetaColumnFallIntoLikePredicate(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    KeyValue *value;
    value = QueryTupleValueItem(expr->rightVal);
    TrimRightEscapeForLikePredicate(value);
    return KeyValueEval(O_LT, min_value, value) && KeyValueEval(O_GE, max_value, value);
}

/* Bin search internal node when case in predicate.. */
static bool BinSearchMetaColumnFallIntoInPredicate(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    KeyValue *value;
    value = QueryTupleValueItem(expr->rightVal);
    return KeyValueEval(O_LT, min_value, value) && KeyValueEval(O_GE, max_value, value);
}

/* Bin search internal node when case EXPR_VAR. */
static bool BinSearchMetaColumnFallIntoExprVar(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    switch (expr->opr) {
        case OP_EQ: return BinSearchMetaColumnFallIntoComparisonPredicateForEQ(select_table_list, meta_column, min_value, max_value, expr);
        case OP_NE: return BinSearchMetaColumnFallIntoComparisonPredicateForNE(select_table_list, meta_column, min_value, max_value, expr);
        case OP_GT: return BinSearchMetaColumnFallIntoComparisonPredicateForGT(select_table_list, meta_column, min_value, max_value, expr);
        case OP_GE: return BinSearchMetaColumnFallIntoComparisonPredicateForGE(select_table_list, meta_column, min_value, max_value, expr);
        case OP_LT: return BinSearchMetaColumnFallIntoComparisonPredicateForLT(select_table_list, meta_column, min_value, max_value, expr);
        case OP_LE: return BinSearchMetaColumnFallIntoComparisonPredicateForLE(select_table_list, meta_column, min_value, max_value, expr);
        case OP_LIKE: return BinSearchMetaColumnFallIntoLikePredicate(select_table_list, meta_column, min_value, max_value, expr);
        case OP_NOT_LIKE: return !BinSearchMetaColumnFallIntoLikePredicate(select_table_list, meta_column, min_value, max_value, expr);
        case OP_IN: return BinSearchMetaColumnFallIntoInPredicate(select_table_list, meta_column, min_value, max_value, expr);
        case OP_NOT_IN: return !BinSearchMetaColumnFallIntoInPredicate(select_table_list, meta_column, min_value, max_value, expr);
        default: UNREACHABLE(false, "Not support expr node type: %d", expr->type);
    }
}


/* Bin search internal node when case EXPR_AND_SET. */
static bool BinSearchMetaColumnFallIntoExprAndSet(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    ListCell *lc;
    foreach (lc, expr->children) {
        ExprNode *child = (ExprNode *)lfirst(lc);
        if (!MetaColumnMatchExprVar(select_table_list, meta_column, child)) continue;
        return BinSearchMetaColumnFallIntoExprVar(select_table_list, meta_column, min_value, max_value, child);
    }
    return true;
}

/* Bin search internal node when case EXPR_OR_SET. */
static bool BinSearchMetaColumnFallIntoExprOrSet(List *select_table_list, MetaColumn *meta_column, KeyValue *min_value, KeyValue *max_value, ExprNode *expr) {
    ListCell *lc;
    foreach (lc, expr->children) {
        ExprNode *child = (ExprNode *)lfirst(lc);
        switch (child->type) {
            case EXPR_VAR: {
                if (!MetaColumnMatchExprVar(select_table_list, meta_column, child)) return true;
                if (BinSearchMetaColumnFallIntoExprVar(select_table_list, meta_column, min_value, max_value, child)) return true;
                else break;
            }
            case EXPR_AND_SET: {
                if (BinSearchMetaColumnFallIntoExprAndSet(select_table_list, meta_column, min_value, max_value, child)) return true;
                else break;;
            }
            default: UNREACHABLE(false, "Expr type %d should not appear here.", child->type);
        }
    }
    return false;
}

/* Bin search internal node when case EXPR_VAR. */
static bool BinSearchInternalNodeCaseExprVar(List *select_table_list, MetaIndex *meta_index, void *min_key, void *max_key, ExprNode *expr) {
    uint32_t offset = 0;
    MetaColumn *meta_column;
    KeyValue *max_value, *min_value;

    ListCell *lc;
    foreach (lc, meta_index->meta_columns) {
        meta_column = (MetaColumn *) lfirst(lc);
        max_value = KeyValueGenerateByMetaColumn(meta_column, max_key + offset);
        min_value = KeyValueGenerateByMetaColumn(meta_column, min_key + offset);
        if (KeyValueEval(O_EQ, max_value, min_value)) { offset += meta_column->column_length; continue; }
        else return BinSearchMetaColumnFallIntoExprVar(select_table_list, meta_column, min_value, max_value, expr);
    }
    return true;
}

/* Bin search internal node when case EXPR_OR_SET. */
static bool BinSearchInternalNodeCaseExprOrSet(List *select_table_list, MetaIndex *meta_index, void *min_key, void *max_key, ExprNode *expr) {
    Assert(meta_index->column_size == 1);
    MetaColumn *meta_column = lfirst(first_cell(meta_index->meta_columns));
    Assert(meta_column != NULL);
    return BinSearchMetaColumnFallIntoExprOrSet(select_table_list, meta_column, min_key, max_key, expr);
}

/* Bin search internal node when case EXPR_AND_SET. */
static bool BinSearchInternalNodeCaseExprAndSet(List *select_table_list, MetaIndex *meta_index, void *min_key, void *max_key, ExprNode *expr) {
    uint32_t offset = 0;
    MetaColumn *meta_column;
    KeyValue *max_value, *min_value;

    ListCell *lc;
    foreach (lc, meta_index->meta_columns) {
        meta_column = (MetaColumn *) lfirst(lc);
        max_value = KeyValueGenerateByMetaColumn(meta_column, max_key + offset);
        min_value = KeyValueGenerateByMetaColumn(meta_column, min_key + offset);
        if (KeyValueEval(O_EQ, max_value, min_value)) { offset += meta_column->column_length; continue; }
        return BinSearchMetaColumnFallIntoExprAndSet(select_table_list, meta_column, min_value, max_value, expr);
    }
    UNREACHABLE(false, "Logic error, can't max_value equals to min_value for each column.");
}

/* Bin search internal node for expr node. */
static bool BinSearchInternalNodeForExpr(List *select_table_list, MetaIndex *meta_index, void *min_key, void *max_key, ExprNode *expr) {
    /* If index is invalid, just return true. */
    if (meta_index == NULL) return true;
    switch (expr->type) {
        case EXPR_VAR: return BinSearchInternalNodeCaseExprVar(select_table_list, meta_index, min_key, max_key, expr);
        case EXPR_OR_SET: return BinSearchInternalNodeCaseExprOrSet(select_table_list, meta_index, min_key, max_key, expr);
        case EXPR_AND_SET: return BinSearchInternalNodeCaseExprAndSet(select_table_list, meta_index, min_key, max_key, expr);
        case EXPR_TRUTH_VALUE: return expr->truthVal;
        default: UNREACHABLE(false, "Not support expr type: %d", expr->type);
    }
}

/* Bin search under conditon for internal node. */
static void BinSearchUnderExprForInternalNode(MetaIndex *meta_index, uint32_t page_num, void *boundary_key, 
                                              SelectResult *select_result, SelectPlan *select_plan) {
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
        if (!BinSearchInternalNodeForExpr(select_plan->selectTableList, meta_index, min_key, max_key, select_plan->condition_expr))
            continue;

        child_page_num = BinInternalNodeGetCellValue(internal_node, meta_index->key_len, i);
        Assert(child_page_num != 0);
        BinSearchUnderExprInner(meta_index, child_page_num, max_key, select_result, select_plan);
    }

    /* Don`t forget the right child. */
    uint32_t right_child_page_num;
    void *right_high_key, *max_cell_key;

    right_child_page_num = BinInternalNodeGetRightNum(internal_node);
    right_high_key = BinInternalNodeGetRightKey(internal_node);
    max_cell_key = BinInternalNodeGetCellKey(internal_node, meta_index->key_len, keys_num - 1);

    /* Filter the internal node by the B+ Tree. */
    if (BinSearchInternalNodeForExpr(select_plan->selectTableList, meta_index, max_cell_key, right_high_key, select_plan->condition_expr))
        BinSearchUnderExprInner(meta_index, right_child_page_num, right_high_key, select_result, select_plan);

    /* The only condition to move to sibling:
     * The target node has spliten. */
    if (CompareKey(meta_index, boundary_key, high_key) > 0) {
        uint32_t next_sibling = BinInternalNodeGetNextSibling(internal_node);
        Assert(next_sibling != 0);
        BinSearchUnderExprInner(meta_index, next_sibling, boundary_key, select_result, select_plan);
    }

    dfree(internal_node);
}

/* Bin search under expr inner. */
void BinSearchUnderExprInner(MetaIndex *meta_index, uint32_t page_num, void *boundary_key, SelectResult *select_result, SelectPlan *select_plan) {
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
        case LEAF_NODE: return BinSearchUnderConditionForLeafNode(meta_index, page_num, boundary_key, select_result, select_plan);
        case INTERNAL_NODE: return BinSearchUnderExprForInternalNode(meta_index, page_num, boundary_key, select_result, select_plan);
        default: UNEXPECTED_VALUE(type);
    }
}

/* Bin search under expr. */
void BinSearchUnderExpr(SelectResult *select_result, SelectPlan *select_plan) {
    Assert(select_plan->hitIndex);
    BinSearchUnderExprInner(select_plan->hitIndex, ROOT_PAGE_NUM, NULL, select_result, select_plan);
}
