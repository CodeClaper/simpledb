#include "data.h"

#ifndef REFER_H
#define REFER_H

/* Init Refer. */
void init_refer();

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

/* Generate new Refer.
 * Note: if page_num is -1 and cell_num is -1 which means refer null. */
Refer *new_refer(Oid oid, int32_t page_num, int32_t cell_num);

/* Generate new cursor. */
Cursor *new_cursor(Table *table, uint32_t page_num, uint32_t cell_num);

/* Generate new ReferUpdateEntity. */
ReferUpdateEntity *new_refer_update_entity(Refer *old_refer, Refer *new_refer);

/* Define cursor. */
Cursor *define_cursor(Table *table, void *key, bool if_exsits);

/* Define Refer. */
Refer *define_refer(Row *row);

/* Fetch Refer. */
Refer *fetch_refer(MetaColumn *meta_column, ConditionNode *condition_node);

/* Convert to refer from cursor. */
Refer *convert_refer(Cursor *cursor);

/* Convert to Cursor from Refer. */
Cursor *convert_cursor(Refer *refer);

/* Check if refer equals. */
bool refer_equals(Refer *refer1, Refer *refer2);

/* Check if cursor equals. */
bool cursor_equals(Cursor *cursor1, Cursor *cursor2);

/* Check if refer null.
 * If page number is -1 and cell number is -1, it means refer null. */
bool refer_null(Refer *refer);

/* Make a NULL Refer. */
Refer *make_null_refer();

/* Update releated tables reference. */
void update_related_tables_refer(ReferUpdateEntity *refer_update_entity);

/* Update Refer. */
void update_refer(Oid oid, 
                int32_t old_page_num, int32_t old_cell_num,
                int32_t new_page_num, int32_t new_cell_num);

/* Add Refer to UpdateReferLockContent. */
void add_refer_update_lock(Refer *refer);

/* Free refer in UpdateReferLockContent. */
void free_refer_update_lock(Refer *refer);

#endif
