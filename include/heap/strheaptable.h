#include <stdbool.h>
#include "refer.h"

#define STRING_ROW_NUM  64
#define STRING_ROW_SIZE (PAGE_SIZE / STRING_ROW_NUM)
#define STRING_FIRST_CELL_NUM 1
#define STRING_TABLE_ROOT_PAGE 0
#define PAGE_STRING_META_SIZE STRING_ROW_SIZE
#define PAGE_STRING_DATA_SIZE (PAGE_SIZE - STRING_ROW_SIZE)

/* StrRefer*/
typedef struct StrRefer {
    Refer refer;
    Size size;
} StrRefer;

/* If the StrRefer is empyt. */
static inline bool EmptyStrRefer(StrRefer *strRefer) {
    return strRefer->size == 0 && ZERO_OID(strRefer->refer.oid);
}

int CompareStrRefer(StrRefer *source, StrRefer *target);
bool CreateStrHeapTable(Oid oid, Oid tid, char *table_name);
StrRefer *InsertStringValue(Oid oid, char *str_val);
char *QueryStringValue(StrRefer *strRefer);
bool DropStrHeapTable(char *table_name);

