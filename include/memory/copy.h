#include <stddef.h>
#include "data.h"
#include "bufmgr.h"

/* Copy value. */
void *copy_value(void *value, DataType data_type); 

/* Copy value. */
void *copy_value2(void *value, MetaColumn *meta_column);

/* Copy Key value pair. */
KeyValue *copy_key_value(KeyValue *key_value);

/* Copy ArrayValue. */
ArrayValue *copy_array_value(ArrayValue *array_value);

/* Copy refer. */
Refer *copy_refer(Refer *refer);

/* Copy row. */
Row *copy_row(Row *row);

/* Copy meta column. */
MetaColumn *copy_meta_column(MetaColumn *meta_column);

/* Copy meta index. */
MetaIndex *copy_meta_index(MetaIndex *meta_index);

/* Copy Table. */
Table *copy_table(Table *table);

/* Copy column node. */
ColumnNode *copy_column_node(ColumnNode *column_node);

/* Copy value item node. */
ValueItemNode *copy_value_item_node(ValueItemNode *value_item_node);

/* Copy function value node. */
FunctionValueNode *copy_function_value_node(FunctionValueNode *function_value_node);

/* Copy function node. */
FunctionNode *copy_function_node(FunctionNode *function_node);

/* Copy a ComparisonNode. */
ComparisonNode *copy_comparison_node(ComparisonNode *comparison_node);

/* Copy a LikeNode. */
LikeNode *copy_like_node(LikeNode *like_node);

/* Copy an InNode. */
InNode *copy_in_node(InNode *in_node);

/* Copy LimitCluaseNode. */
LimitClauseNode *copy_limit_node(LimitClauseNode *limit_clause_node);

/* Copy ReferValue. */
ReferValue *copy_refer_value(ReferValue *refer_value);

/* Copy TableRefNode. */
TableRefNode *copy_table_ref_node(TableRefNode *table_ref);

/* Copy TableExpNode. */
TableExpNode *copy_table_exp_node(TableExpNode *table_exp_node);

/* Copy a ScalarExpNode. */
ScalarExpNode *copy_scalar_exp_node(ScalarExpNode *scalar_exp_node);

/* Copy a dymamic memory block */
void *copy_block(void *value, size_t size);

/* Copy BufferDesc. */
BufferDesc *copy_buffer_desc(BufferDesc *buff_desc);
