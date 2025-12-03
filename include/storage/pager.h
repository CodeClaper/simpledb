#include <stdint.h>
#include "data.h"

/* Get the page size. */
Size GetPageSize(Oid oid);

/* Get next unused page num. */
uint32_t GetNextUnusedPageNum(Table *table);

/* Get next rid page num. */
uint32_t GetNextUnusedRidPageNum(Table *table);

/* Reset Page. */
void ResetPage(Oid oid, uint32_t page_num);
