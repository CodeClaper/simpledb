#include <stdbool.h>

#define SYS_STRING_TABLE_NAME       "sys_string"
#define SYS_STRING_TABLE_FULL_NAME  "sys_string.dbt"
#define STRING_ROW_NUM  64
#define STRING_ROW_SIZE (PAGE_SIZE / STRING_ROW_NUM)
#define STRING_TABLE_ROOT_PAGE 0

/* Create the string heap table. */
bool CreateStrHeapTable(char *table_name);
