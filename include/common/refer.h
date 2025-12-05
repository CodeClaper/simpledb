#include <stdbool.h>
#include <stdint.h>
#include "data.h"
#include "mmgr.h"

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


/* Check if two refers are equals. */
static inline bool ReferIsEqual(Refer *refer1, Refer *refer2) {
    return refer1->oid == refer2->oid && 
                refer1->page_num == refer2->page_num && 
                    refer1->cell_num == refer2->cell_num;
}


/* Check if refer empty. 
 * If page number is -1 and cell number is -1, it means refer empty. */
static inline bool ReferIsEmpty(Refer *refer) {
    return refer->page_num == -1 && refer->cell_num == -1;
}

/* Make a empty Refer. */
Refer *MakeEmptyRefer();

/* Fetch ref id under condition. */
Rid FetchRefIdUnderCondition(Oid oid, SearchConditionNode *condition);

/* Append new tuple and return ref id. */
Rid AppendAndReturnRefId(Oid oid, List *value_list);


#endif
