#include "data.h"
#include "list.h"

/* Json DBResult. */
void json_db_result(DBResult *result);

/* Json raw row. */
void json_raw_row(List *meta_columns, void *destin);

/* Json row. */
void json_row(Row *row);

/* Send out db execution result set. */
void json_list(List *list);
