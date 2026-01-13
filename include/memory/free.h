#include "data.h"
#include "xlog.h"
#include "flatten.h"
#include "bufmgr.h"

void free_value(void *value, DataType data_type);
void free_key_value(KeyValue *key_value);
void free_block(void *value);
void free_data_type_node(DataTypeNode *data_type);
void free_row(Row *row);
void free_common_row(Row *row);
void free_select_result(SelectResult *select_result);
void free_meta_column(MetaColumn *meta_column);
void free_meta_table(MetaTable *meta_table);
void free_meta_index(MetaIndex *meta_index);
void free_column_node(ColumnNode *column_node);
void free_buffer_desc(BufferDesc *buf_desc);
void free_pager(Pager *pager);
void free_table(Table *table);
void free_table_buffer_entry(TableBufferEntry *entry);
void free_refer(Refer *refer);
void free_refer_value(ReferValue *refer_value);
void free_array_value(ArrayValue *array_value);
void free_value_item_node(ValueItemNode *value_item_node);
void free_function_value_node(FunctionValueNode *function_value_node);
void free_function_node(FunctionNode *function_node);
void free_scalar_exp_node(ScalarExpNode *scalar_exp_node);
void free_column_def_opt_node(ColumnDefOptNode *column_def_opt);
void free_column_def_name(ColumnDefName *column_def_name);
void free_column_def_node(ColumnDefNode *column_def_node);
void free_primary_key_node(PrimaryKeyNode *primary_key_node);
void free_base_table_element_node(BaseTableElementNode *base_table_element);
void free_select_items_node(SelectItemsNode *select_items_node);
void free_assignment_node(AssignmentNode *assignment_node);
void free_table_ref_node(TableRefNode *table_ref_node);
void free_table_exp_node(TableExpNode *table_exp_node);
void free_expr_node(ExprNode *expr_node);
void free_selection_node(SelectionNode *selection_node);
void free_select_node(SelectNode *select_node);
void free_insert_node(InsertNode *insert_node);
void free_create_table_node(CreateTableNode *create_table_node);
void free_statement(Statement *stmt);
void free_db_result(DBResult *result);
void free_xlog_entry(XLogEntry *xlog_entry);
