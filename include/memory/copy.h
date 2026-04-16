#include <stddef.h>
#include "data.h"
#include "bufmgr.h"

void *copy_value(void *value, DataType data_type); 
void *copy_value2(void *value, MetaColumn *meta_column);
KeyValue *copy_key_value(KeyValue *key_value);
ArrayValue *copy_array_value(ArrayValue *array_value);
Refer *copy_refer(Refer *refer);
Row *copy_row(Row *row);
MetaColumn *copy_meta_column(MetaColumn *meta_column);
MetaIndex *copy_meta_index(MetaIndex *meta_index);
Table *copy_table(Table *table);
ColumnNode *copy_column_node(ColumnNode *column_node);
ValueItemNode *copy_value_item_node(ValueItemNode *value_item_node);
FunctionValueNode *copy_function_value_node(FunctionValueNode *function_value_node);
FunctionNode *copy_function_node(FunctionNode *function_node);
ComparisonNode *copy_comparison_node(ComparisonNode *comparison_node);
LikeNode *copy_like_node(LikeNode *like_node);
InNode *copy_in_node(InNode *in_node);
LimitClauseNode *copy_limit_node(LimitClauseNode *limit_clause_node);
ReferValue *copy_refer_value(ReferValue *refer_value);
TableRefNode *copy_table_ref_node(TableRefNode *table_ref);
TableExpNode *copy_table_exp_node(TableExpNode *table_exp_node);
ScalarExpNode *copy_scalar_exp_node(ScalarExpNode *scalar_exp_node);
void *copy_block(void *value, size_t size);
BufferDesc *copy_buffer_desc(BufferDesc *buff_desc);
