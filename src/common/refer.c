/**
 * ============================================================ Refer Manger ============================================================================
 * The Refer Manager works around the refer. Simpledb supports REFERENCE data type which help you use other table as column data type. 
 * In fact it is a pointer. And, simpledb recommands Refer instead of Join.
 * Refer has more effecitve on DQL operation than Join. If you know the position of data in disk, you can immediately
 * get the data, but if you just have the relationship of tables on columns, you need traverse all data to find out the data satisfied the relationship.
 * Everything has a cost. Refer is good at DQL operation, but has much thing to at DML operation.
 * When inserting or modifying a row, the refer maybe will change and simpledb must manager the changes, this is what the Refer Manager to do. 
 * ======================================================================================================================================================
 */

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include "refer.h"
#include "data.h"
#include "mmgr.h"
#include "copy.h"
#include "free.h"
#include "select.h"
#include "meta.h"
#include "row.h"
#include "tuple.h"
#include "ltindex.h"
#include "ltsearch.h"
#include "xlog.h"
#include "utils.h"
#include "asserts.h"
#include "log.h"
#include "instance.h"
#include "tablecache.h"
#include "optimizer.h"
#include "table.h"
#include "heaptable.h"

typedef struct {
    uint32_t size;
    Refer **list;
} UpdateReferLockContent;

static UpdateReferLockContent *update_refer_lock_content;

/* Check if a refer include in ReferUpdateEntity. */
static bool include_update_refer_lock(Refer *refer);

/* Init Refer. */
void init_refer() {
    update_refer_lock_content = instance(UpdateReferLockContent);
    update_refer_lock_content->list = dalloc(0);
    update_refer_lock_content->size = 0;
}

/* Check refer */
void check_refer(Refer *refer) {
    Assert(refer->page_num >= 0);
    Assert(refer->cell_num >= 0);
}

/* Get refer oid. */
static inline Oid get_refer_oid(ReferUpdateEntity *refer_update_entity) {
    return refer_update_entity->old_refer->oid;
}

/* Add Refer to UpdateReferLockContent. */
void add_refer_update_lock(Refer *refer) {
    if (!include_update_refer_lock(refer)) {
        update_refer_lock_content->list = drealloc(update_refer_lock_content->list, sizeof(Refer *) * (update_refer_lock_content->size + 1));
        update_refer_lock_content->list[update_refer_lock_content->size++] = copy_refer(refer);
    }
}

/* Free refer in UpdateReferLockContent. */
void free_refer_update_lock(Refer *refer) {
    Assert(update_refer_lock_content);
    uint32_t i, j;
    for (i = 0; i < update_refer_lock_content->size; i++) {
        Refer *current = update_refer_lock_content->list[i];
        if (refer_equals(refer, current)) {
            for (j = i; j < update_refer_lock_content->size; j++) {
                memcpy(update_refer_lock_content->list + j, update_refer_lock_content->list + j + 1, sizeof(Refer *));
            }
            memset(update_refer_lock_content->list + j, 0, sizeof(Refer *));

            free_refer(current);

            update_refer_lock_content->size--;
            update_refer_lock_content->list = drealloc(update_refer_lock_content->list, sizeof(Refer *) * update_refer_lock_content->size);
            break;
        }
    }
}

/* Check if a refer include in ReferUpdateEntity. */
static bool include_update_refer_lock(Refer *refer) {
    Assert(update_refer_lock_content);
    uint32_t i;
    for (i = 0; i < update_refer_lock_content->size; i++) {
        Refer *current = update_refer_lock_content->list[i];
        if (refer_equals(refer, current))
            return true;
    }
    return false;
}

/* Generate new Refer. 
 * Note: if page_num is -1 and cell_num is -1 which means refer null. */
Refer *new_refer(Oid oid, int32_t page_num, int32_t cell_num) {
    Refer *refer = instance(Refer);
    refer->oid = oid;
    refer->page_num = page_num;
    refer->cell_num = cell_num;
    return refer;
}

/* Fetch Refer. 
 * If found no one or many one, return NULL.  */
Refer *fetch_refer(MetaColumn *meta_column, SearchConditionNode *condition) {
    Oid oid;
    Table *table;
    SelectResult *select_result;
    uint32_t row_size;
    Row *row;
    void *key;
    Refer *refer = NULL;
    
    table = open_table(meta_column->table_name);
    oid = GET_TABLE_OID(table);
    select_result = new_select_result(UNKONWN_STMT, GET_TABLE_NAME(table), true);

    QueryUnderSearchCondition(
        select_result, 
        SimpleSelectPlan(SelectRow, ARG_NULL, NULL, condition)
    );

    row_size = QueueSize(select_result->rows);
    if (row_size > 1) 
        db_log(ERROR, 
               "Expected to one reference, but found %d, maybe you can use 'in' as for array.", 
               select_result->row_size);
    else if (row_size == 1) {
        /* Take the first row as refered. Maybe row size should be one, 
         * but now there is no check. */
        row = qfirst(QueueHead(select_result->rows));
        key = RowFindKey(row, table->meta_table);
        refer = BtreeSearchRefer(oid, key);
    }

    return refer;
}

/* Check if refer null. 
 * If page number is -1 and cell number is -1, it means refer null. */
bool refer_null(Refer *refer) {
    return refer->page_num == -1 && refer->cell_num == -1;
}

/* Make a NULL Refer. */
Refer *make_null_refer() {
    Refer *refer = instance(Refer);
    refer->page_num = -1;
    refer->cell_num = -1;
    return refer;
}

/* Generate new ReferUpdateEntity. */
ReferUpdateEntity *new_refer_update_entity(Refer *old_refer, Refer *new_refer) {
    ReferUpdateEntity *refer_update_entity = instance(ReferUpdateEntity);
    refer_update_entity->old_refer = old_refer;
    refer_update_entity->new_refer = new_refer;
    return refer_update_entity;
}

/* Check if table has column refer to. */
static bool if_related_table(MetaTable *meta_table, Oid refer_oid) {
    Table *refer_table;

    refer_table = open_table_inner(refer_oid);
    Assert(refer_table);
    Assert(meta_table);

    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->column_type == T_REFERENCE && 
                strcmp(meta_column->table_name, GET_TABLE_NAME(refer_table)) == 0)
            return true;
    }

    return false;
}

/* Check if refer equals. */
bool refer_equals(Refer *refer1, Refer *refer2) {
    return refer1->oid == refer2->oid && 
                refer1->page_num == refer2->page_num && 
                    refer1->cell_num == refer2->cell_num;
}

/* Update single key value refer. */
static bool UpdateReferForSingleValue(void *tuple, MetaColumn *meta_column, ReferUpdateEntity *refer_update_entity) {
    Refer *refer = TupleFindValue(tuple, meta_column);
    if (refer_equals(refer, refer_update_entity->old_refer)) {
        TupleSetValue(tuple, meta_column, refer_update_entity->new_refer);
        return true;
    }
    return false;
}

/* Update array key value refer. */
static bool UpdateReferForArrayValue(void *tuple, MetaColumn *meta_column, ReferUpdateEntity *refer_update_entity) {
    bool flag = false;
    ArrayValue *array_value = TupleFindValue(tuple, meta_column);

    ListCell *lc;
    foreach (lc, array_value->list) {
        if (refer_equals(lfirst(lc), refer_update_entity->old_refer)) {
            lfirst(lc) = copy_refer(refer_update_entity->new_refer);
            flag = true;
        }
    }
    
    if (flag)
        TupleSetValue(tuple, meta_column, array_value);

    return flag;
}

/* Update row key value. */
static void UpdateTupleReferValueExtend(Oid oid, void *tuple, 
                                        Refer *hrefer, MetaColumn *meta_column, 
                                        ReferUpdateEntity *refer_update_entity) {
    bool flag = false;

    if (meta_column->array_dim > 0) {
        if (UpdateReferForArrayValue(tuple, meta_column, refer_update_entity))
            flag = true;
    } else {
        if (UpdateReferForSingleValue(tuple, meta_column, refer_update_entity))
            flag = true;
    }

    /* If satisfied above conditions, update the row. */
    if (flag)
        HeapTableUpdateTuple(oid, hrefer, tuple);
}


/* Update row refer. */
static void UpdateTupleReferValue(void *tuple, SelectResult *select_result, ROW_HANDLER_ARG_TYPE type, void *arg) {
    Oid oid;
    Table *table, *ref_table;
    ReferUpdateEntity *refer_update_entity;
    void *key, *index;

    oid = select_result->oid;
    table = open_table_inner(oid);

    Assert(arg);
    Assert(type == ARG_REFER_UPDATE_ENTITY);
    refer_update_entity = (ReferUpdateEntity *) arg;
    ref_table = open_table_inner(refer_update_entity->old_refer->oid);
    Assert(ref_table);

    key = TupleFindKey(tuple, table->meta_table);
    index = BtreeSearchValue(oid, key);

    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        if (meta_column->column_type == T_REFERENCE && 
            StrEq(meta_column->table_name, GET_TABLE_NAME(ref_table))) 
            UpdateTupleReferValueExtend(oid, tuple, (Refer *) index, meta_column, refer_update_entity);
    }
}

/* Update table refer. */
static void update_table_refer(MetaTable *meta_table, ReferUpdateEntity *refer_update_entity) {
    /* Skip update locked refer. */
    if (include_update_refer_lock(refer_update_entity->old_refer))
        return;

    /* Query with condition, and delete satisfied condition row. */
    SelectResult *select_result = new_select_result(UPDATE_STMT, meta_table->table_name, true);

    /* Traverse rows to update refer. */
    QueryUnderSearchCondition(
        select_result, 
        SimpleSelectPlan(UpdateTupleReferValue, ARG_REFER_UPDATE_ENTITY, refer_update_entity, NULL)
    );
}

/* Update releated tables reference. */
void update_related_tables_refer(ReferUpdateEntity *refer_update_entity) {
    Oid self_oid;
    List *table_list;

    /* Get self name. */
    self_oid = get_refer_oid(refer_update_entity);
    table_list = GetAllTableCache();

    /* Update table refer. */
    ListCell *lc;
    foreach (lc, table_list) {
        /* Check other tables. */
        Table *table = (Table *) lfirst(lc);
        MetaTable *meta_table = table->meta_table;
        if (if_related_table(meta_table, self_oid)) 
            update_table_refer(meta_table, refer_update_entity);
    }
}


/* Update Refer 
 * When referenct target be changed (updated or deleted), 
 * must to update row reference value which pointer to it. */
void update_refer(Oid oid, int32_t old_page_num, int32_t old_cell_num, int32_t new_page_num, int32_t new_cell_num) {
    Refer *oldRefer, *newRefer;
    ReferUpdateEntity *ruEntity;

    oldRefer = new_refer(oid, old_page_num, old_cell_num);
    newRefer = new_refer(oid, new_page_num, new_cell_num);
    ruEntity = new_refer_update_entity(oldRefer, newRefer);
   
    /* Update related tables. */
    update_related_tables_refer(ruEntity);

    /* Update Xlog. */
    UpdateXlogEntryRefer(ruEntity);

    /* Free memory. */
    free_refer_update_entity(ruEntity);
}
