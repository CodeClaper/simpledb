#include <stdbool.h>
#include <string.h>
#include <sys/time.h>
#include "instance.h"
#include "data.h"
#include "mmgr.h"
#include "copy.h"
#include "systable.h"

/* Generate new KeyValue instance. */
KeyValue *new_key_value(char *key, void *value, DataType data_type, Oid tid, Oid type_oid) {
    KeyValue *key_value = instance(KeyValue);
    key_value->key = dstrdup(key);
    key_value->value = copy_value(value, data_type);
    key_value->data_type = data_type;
    key_value->tid = tid;
    key_value->type_id = type_oid;
    key_value->is_array = false;
    return key_value;
}

/* Genrate new simple KeyValue instance. */
KeyValue *new_simple_key_value(char *key, void *value, DataType data_type) {
    return new_key_value(key, value, data_type, OID_ZERO, OID_ZERO);
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
    select_result->oid = StrIsEmpty(table_name) ? OID_ZERO : TableNameFindOid(table_name);
    select_result->table_name = StrIsEmpty(table_name) ? NULL : dstrdup(table_name);
    select_result->range_variable = NULL;
    select_result->tuples = CreateQueue(NODE_VOID);
    select_result->rows = CreateQueue(NODE_ROW);
    select_result->row_size = 0;
    select_result->current_tuple = NULL;
    select_result->first_row_flag = true;
    select_result->nested = NULL;
    select_result->head = NULL;
    select_result->columns = NIL;
    select_result->display_colums = NIL;
    select_result->tuple_size = 0;
    if (is_head) select_result->head = select_result;
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


/* Generate new Refer. 
 * Note: if page_num is -1 and cell_num is -1 which means refer null. */
Refer *new_refer(Oid oid, int32_t page_num, int32_t cell_num) {
    Refer *refer = instance(Refer);
    refer->oid = oid;
    refer->page_num = page_num;
    refer->cell_num = cell_num;
    return refer;
}

