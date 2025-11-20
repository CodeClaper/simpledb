#include "data.h"

#ifndef REFER_H
#define REFER_H

#define REFER_SIZE sizeof(Refer)

/* Init Refer. */
void InitRefer();

/* Compare two Refers. 
 * ---------------------
 * It`s meanless to compare two different-oid refers.
 * If both Refers has same page_num, compare theirs cell_num
 * Otherwise, compare theirs page_num.
 * */
static inline int CompareRefer(Refer srefer, Refer trefer) {
    if (srefer.oid != trefer.oid)
        Assert(srefer.oid == trefer.oid);
    return srefer.page_num == trefer.page_num
        ? srefer.cell_num - trefer.cell_num
        : srefer.page_num - trefer.page_num;
}


/* Fetch Refer. */
Refer *FetchRefer(MetaColumn *meta_column, SearchConditionNode *condition);

/* Check if refer equals. */
bool ReferIsEqual(Refer *refer1, Refer *refer2);

/* Check if refer empty.
 * If page number is -1 and cell number is -1, it means refer empty. */
bool ReferIsEmpty(Refer *refer);

/* Make a empty Refer. */
Refer *MakeEmptyRefer();

/* Add Refer to UpdateReferLockContent. */
Refer *ReferUpdateLockAdd(Refer *refer);

/* Free refer in UpdateReferLockContent. */
void ReferUpdateLockFree(Refer *refer);

/* Update Refer. */
void UpdateRefer(Oid oid, int32_t old_page_num, int32_t old_cell_num, int32_t new_page_num, int32_t new_cell_num);

#endif
