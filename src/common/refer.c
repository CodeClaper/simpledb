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
#include <strings.h>
#include "refer.h"
#include "data.h"
#include "mmgr.h"
#include "copy.h"
#include "free.h"
#include "select.h"
#include "insert.h"
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
#include "refer.h"


/* Make a NULL Refer. */
Refer *MakeEmptyRefer() {
    Refer *refer = instance(Refer);
    refer->page_num = -1;
    refer->cell_num = -1;
    return refer;
}

/* Fetch ref id under condition. 
 * If not found return RID_ZERO.  */
Rid FetchRefIdUnderCondition(Oid oid, SearchConditionNode *condition) {
    Table *table;
    SelectResult *select_result;
    uint32_t size;
    void *tuple;
    
    table = open_table_inner(oid);
    select_result = new_select_result(UNKONWN_STMT, GET_TABLE_NAME(table), true);

    QueryUnderSearchCondition(
        select_result, 
        SimpleSelectPlan(SelectTuple, ARG_NULL, NULL, condition)
    );

    size = QueueSize(select_result->tuples);
    if (size > 1) 
        db_log(ERROR, "Expected to one reference, but found %d.", 
               select_result->row_size);
    else if (size == 1) {
        /* Take the first row as refered. Maybe row size should be one, 
         * but now there is no check. */
        tuple = qfirst(QueueHead(select_result->tuples));
        return TupleGetRefId(tuple, table->meta_table);
    }

    return RID_ZERO;
}


/* Append new tuple and return ref id. */
Rid AppendAndReturnRefId(Oid oid, List *value_list) {
    Table *table;
    InsertNode *insert_node;
    List *ref_ids;

    table = open_table_inner(oid);
    insert_node = GenerateInsertNode(GET_TABLE_NAME(table), value_list);
    ref_ids = InsertForValues(insert_node);
    Assert(len_list(ref_ids) == 1);

    return (Rid) lfirst_long(first_cell(ref_ids));
}


