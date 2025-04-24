#include <stdint.h>
#include "data.h"


/* Init Pager*/
void InitPager();

/* Get the page size. */
Size GetPageSize(Oid oid);

/* Get next unused page num. */
uint32_t GetNextUnusedPageNum(Table *table);
