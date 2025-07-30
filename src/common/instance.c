#include <stdbool.h>
#include <string.h>
#include <sys/time.h>
#include "instance.h"
#include "data.h"
#include "mmgr.h"

/* Generate new KeyValue instance. */
KeyValue *new_key_value(char *key, void *value, DataType data_type) {
    KeyValue *key_value = instance(KeyValue);
    key_value->key = key;
    key_value->value = value;
    key_value->data_type = data_type;
    key_value->is_array = false;
    return key_value;
}

/* Generate new row instance. */
Row *new_row(void *key, char *table_name) {
    Row *row = instance(Row);
    row->key = key;
    strcpy(row->table_name, table_name);
    row->data = create_list(NODE_KEY_VALUE);
    return row;
}

/* Generate new ArrayValue instance. */
ArrayValue *new_array_value(DataType data_type, uint32_t size) {
    ArrayValue *array_value = instance(ArrayValue);
    array_value->type = data_type;
    array_value->list = create_list(NODE_VOID);
    return array_value;
}

/* Generate new select result structure. */
SelectResult *new_select_result(StatementType stype, char *table_name, bool is_head) {
    SelectResult *select_result = instance(SelectResult);
    select_result->stype = stype;
    select_result->row_size = 0;
    select_result->table_name = table_name ? dstrdup(table_name) : NULL;
    select_result->range_variable = NULL;
    select_result->rows = CreateQueue(NODE_ROW);
    select_result->tuple = NULL;
    select_result->first_row_flag = true;
    select_result->nested = NULL;
    select_result->head = NULL;
    select_result->columns = NIL;
    if (is_head)
        select_result->head = select_result;
    return select_result;
}

/* Generate new db result. */
DBResult *new_db_result() {
    /* New DbResule and initialize it. */
    DBResult *result = instance(DBResult);
    result->success = false;
    result->message = NULL;
    result->data = NULL;
    result->rows = 0;
    result->duration = 0;
    result->table = NULL;
    gettimeofday(&result->start_time, NULL);
    return result;
}

