#include "data.h"

DBResult *new_db_result();
KeyValue *new_key_value(char *key, void *value, DataType data_type, Oid tid, Oid type_oid, uint32_t array_dim);
KeyValue *new_simple_key_value(char *key, void *value, DataType data_type);
ArrayValue *new_array_value(DataType data_type, uint32_t size);
SelectResult *new_select_result(StatementType stype, char *table_name, bool is_head);
Refer *new_refer(Oid oid, int32_t page_num, int32_t cell_num);
