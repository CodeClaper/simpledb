#include "data.h"
#include "list.h"

void json_db_result(DBResult *result);
void json_tuple(List *meta_columns, void *tuple);
void json_row(Row *row);
void json_list(List *list);
