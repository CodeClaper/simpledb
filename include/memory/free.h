#include "data.h"
#include "xlog.h"
#include "bufmgr.h"

/* Copy value */
void free_value(void *value, DataType data_type);

/* Free KeyValue */
void free_key_value(KeyValue *key_value);

/* Free Block. */
void free_block(void *value);

/* Free DataTyepNode. */
void free_data_type_node(DataTypeNode *data_type);

/* Free row. */
void free_row(Row *row);

void free_common_row(Row *row);

/* Free select result. */
void free_select_result(SelectResult *select_result);

/* Free meta column. */
void free_meta_column(MetaColumn *meta_column);

/* Free meta table. */
void free_meta_table(MetaTable *meta_table);

/* Free column */
void free_column_node(ColumnNode *column_node);

/* Free BufferDesc. */
void free_buffer_desc(BufferDesc *buf_desc);

/* Free Pager. */
void free_pager(Pager *pager);

/* Free table. */
void free_table(Table *table);

/* Free TableBufferEntry. */
void free_table_buffer_entry(TableBufferEntry *entry);

/* Free cursor. */
void free_cursor(Cursor *cursor);

/* Free Refer. */
void free_refer(Refer *refer);

/* Free ReferValue. */
void free_refer_value(ReferValue *refer_value);

/* Free ArrayValue. */
void free_array_value(ArrayValue *array_value);

/* Free ReferUpdateEntity. */
void free_refer_update_entity(ReferUpdateEntity *refer_update_entity);

/* Free value item node. */
void free_value_item_node(ValueItemNode *value_item_node);

/* Free function value node. */
void free_function_value_node(FunctionValueNode *function_value_node);

/* Free function node. */
void free_function_node(FunctionNode *function_node);

/* Free ScalarExpNode. */
void free_scalar_exp_node(ScalarExpNode *scalar_exp_node);

/* Free ColumnDefOptNode. */
void free_column_def_opt_node(ColumnDefOptNode *column_def_opt);

/* Free ColumnDefName. */
void free_column_def_name(ColumnDefName *column_def_name);

/* Free column def node. */
void free_column_def_node(ColumnDefNode *column_def_node);

/* Free primary key node. */
void free_primary_key_node(PrimaryKeyNode *primary_key_node);

/* Free BaseTableElementNode */
void free_base_table_element_node(BaseTableElementNode *base_table_element);

/* Free select items node. */
void free_select_items_node(SelectItemsNode *select_items_node);

/* Free assignment node. */
void free_assignment_node(AssignmentNode *assignment_node);

/* Free condition node. */
void free_condition_node(ConditionNode *condition_node);

/* Free TableRefNode. */
void free_table_ref_node(TableRefNode *table_ref_node);

/* Free TableExpNode. */
void free_table_exp_node(TableExpNode *table_exp_node);

/* Free SelectionNode. */
void free_selection_node(SelectionNode *selection_node);

/* Free select node. */
void free_select_node(SelectNode *select_node);

/* Free insert node. */
void free_insert_node(InsertNode *insert_node);

/* Free create table node. */
void free_create_table_node(CreateTableNode *create_table_node);

/* Free statement */
void free_statement(Statement *stmt);

/* Free DBResult. */
void free_db_result(DBResult *result);

/* Free XLogEntry. */
void free_xlog_entry(XLogEntry *xlog_entry);
