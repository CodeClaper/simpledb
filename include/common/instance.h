#include "data.h"

/* Generate new db result. */
DBResult *new_db_result();

/* New KeyValue instance. */
KeyValue *new_key_value(char *key, void *value, DataType data_type, char *table_name);

/* New ArrayValue instance. */
ArrayValue *new_array_value(DataType data_type, uint32_t size);

/* New select result structure. */
SelectResult *new_select_result(StatementType stype, char *table_name, bool is_head);

