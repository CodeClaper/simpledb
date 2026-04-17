#include <stdint.h>
#include "data.h"

Size GetPageSize(Oid oid);
uint32_t GetNextUnusedPageNum(Table *table);
uint32_t GetNextUnusedRidPageNum(Table *table);
uint32_t GetNextUnusedSidPageNum(Table *table);
void ResetPage(Oid oid, uint32_t page_num);
