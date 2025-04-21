#include <stdbool.h>
#include "refer.h"

#define SYS_STRING_TABLE_NAME_APPENDIX  "_sys_str"
#define STRING_ROW_NUM  64
#define STRING_ROW_SIZE (PAGE_SIZE / STRING_ROW_NUM)
#define STRING_TABLE_ROOT_PAGE 0

/* StrRefer*/
typedef struct StrRefer {
    Refer refer;
    Size size;
} StrRefer;

/* Create the string heap table. */
bool CreateStrHeapTable(char *table_name);


/* Insert new String value. 
 * Return the Refer value.
 * */
StrRefer *InsertStringValue(char *table_name, char *str_val);


/* Query string value. */
char *QueryStringValue(StrRefer *strRefer);


/* Drop the string heap table. */
bool DropStrHeapTable(char *table_name);

