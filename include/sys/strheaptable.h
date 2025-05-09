#include <stdbool.h>
#include "refer.h"

#define SYS_STRING_TABLE_NAME_APPENDIX  "_sys_str"
#define STRING_ROW_NUM  64
#define STRING_ROW_SIZE (PAGE_SIZE / STRING_ROW_NUM)
#define STRING_TABLE_ROOT_PAGE 0
#define PAGE_STRING_META_SIZE STRING_ROW_SIZE
#define PAGE_STRING_DATA_SIZE (PAGE_SIZE - STRING_ROW_SIZE)
#define STRING_FIRST_CELL_NUM 1

/* StrRefer*/
typedef struct StrRefer {
    Refer refer;
    Size size;
} StrRefer;

/* Compare two StrRefers. */
int CompareStrRefer(StrRefer *source, StrRefer *target);

/* Create the string heap table. */
bool CreateStrHeapTable(char *table_name);

/* Insert new String value. 
 * Return the Refer value.
 * */
StrRefer *InsertStringValue(Oid oid, char *str_val);


/* Query string value. */
char *QueryStringValue(StrRefer *strRefer);


/* Drop the string heap table. */
bool DropStrHeapTable(char *table_name);

