#include "data.h"
#include <stdint.h>

#ifndef REFER_H
#define REFER_H

#define REFER_SIZE sizeof(Refer)

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


/* Fetch ref id under condition. */
Rid FetchRefIdUnderCondition(Oid oid, SearchConditionNode *condition);

/* Append new tuple and return ref id. */
Rid AppendAndReturnRefId(Oid oid, List *value_list);

/* Check if refer equals. */
bool ReferIsEqual(Refer *refer1, Refer *refer2);

/* Check if refer empty.
 * If page number is -1 and cell number is -1, it means refer empty. */
bool ReferIsEmpty(Refer *refer);

/* Make a empty Refer. */
Refer *MakeEmptyRefer();

#endif
