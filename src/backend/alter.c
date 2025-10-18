/******************************** Alter table statment module *******************************
 * Auth:        JerryZhou
 * Created:     2024/06/28
 * Modify:      2024/09/20
 * Locataion:   src/backend/alter.c
 * support:
 * (1) alter table add column
 * (2) alter table drop column
 * (3) alter table change column
 * (4) alter table rename column
 ********************************************************************************************/
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include "data.h"
#include "alter.h"
#include "mmgr.h"
#include "check.h"
#include "table.h"
#include "tablecache.h"
#include "create.h"
#include "tablelock.h"
#include "utils.h"
#include "free.h"
#include "meta.h"
#include "tuple.h"
#include "row.h"
#include "log.h"
#include "ltree.h"
#include "tablereg.h"
#include "systable.h"
#include "heaptable.h"
#include "ltinsert.h"

/* Try to catpture table.
 * If these other session on the table, wait and test. 
 * */
static void AlterCaptureTable(Oid oid) {
    try_acquire_table(oid);
    /* Wait until capture the table exclusively. */
    while (if_shared_table(oid)) {
        usleep(100);
    }
}

/* Release Table. */
static void AlterReleaseTable(Oid oid) {
    RemoveTableCache(oid);
    try_release_table(oid);
}


static void *SeriableIndexValue(Table *table, void *tuple, Refer *heapRefer) {
    uint32_t value_len;
    void *destination;
    MetaTable *meta_table;

    value_len = table->index_value_len;
    destination = dalloc(value_len);
    meta_table = table->meta_table;

    /* Assign the heap refer value. */
    memcpy(destination, heapRefer, REFER_SIZE);

    uint32_t offset = REFER_SIZE;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->sys_reserved) {
            void *value = TupeFindValue(tuple, meta_column);
            assign_row_value(destination + offset, value, meta_column);
            offset += meta_column->column_length;
        }
    }

    return destination;
}

static int ColumnPositionDefFindPos(MetaTable *meta_table, ColumnPositionDef *position_def) {
    /* If not ColumnPositionDef, append column at last. */
    if (IsNull(position_def))
        return meta_table->column_size;

    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (StrEq(meta_column->column_name, position_def->column)) {
            switch (position_def->type) {
                case POS_BEFORE:
                    return __i;
                case POS_AFTER:
                    return __i + 1;
            }
        }
    }

    db_log(ERROR, "Column '%s' not exists in table '%s'.", 
           position_def->column, 
           meta_table->table_name);

    return -1;
}

/* Append meta column. */
static inline void MetaTableAppendColumn(MetaTable *meta_table, MetaColumn *new_meta_column, int pos) {
    append_list_at(meta_table->meta_columns, new_meta_column, pos);
}

/* Loop heap table and reinsert into btree. */
static void LoopHeapTableAndReinsert(Table *table) {
    Oid oid, hoid;
    Refer *refer;
    MetaColumn *primary_meta_column;
    void *tuple, *key, *value;

    oid = table->oid;
    hoid = table->hoid;
    refer = new_refer(hoid, 0, 0);
    primary_meta_column = MetaTableFindPrimaryKey(table->meta_table);

    while ((tuple = HeapTableLookupTupleLoop(table, refer)) != NULL) {
        key = TupeFindValue(table, primary_meta_column);
        value = SeriableIndexValue(table, tuple, refer);
        BtreeInsert(oid, key, value);
    }
}

/* Add new column inner. */
static bool AlterAddNewColumnInner(Oid oid, MetaColumn *new_meta_column, ColumnPositionDef *position_def) {
    Table *table = open_table_inner(oid);
    int pos = ColumnPositionDefFindPos(table->meta_table, position_def);
    
    /* Append meta column to meta table. */
    MetaTableAppendColumn(table->meta_table, new_meta_column, pos);

    /* Reset the table. */
    shrink_table(oid, table->meta_table);

    /* Append to heap able. */
    HeapTableAppendColumn(table, new_meta_column, pos);
    
    /* Loop heap table and reinsert. */
    LoopHeapTableAndReinsert(table);

    return true;
}

/* Add new Column. */
static void AlterAddNewColumn(AddColumnDef *add_column_def, char *table_name, DBResult *result) {
    Oid oid;
    MetaColumn *new_meta_column;

    oid = TableNameFindOid(table_name);
    new_meta_column = ColumnDefNodeGenerateMetaColumn(add_column_def->column_def, table_name);        

    /* By now, not support primary key alter operation. */
    if (new_meta_column->is_primary)
        db_log(ERROR, "Not support add primary-key column through alter table.");

    /* Capture table exclusively. */
    AlterCaptureTable(oid);

    /* Try to add new column. */
    if (AlterAddNewColumnInner(oid, new_meta_column, add_column_def->position_def)) {
        result->success = true;
        result->message = FormatStr("Add column '%s' for table '%s' successfully.", 
                                    new_meta_column->column_name, table_name);
        db_log(SUCCESS, "Add column '%s' for table '%s' successfully.", 
               new_meta_column->column_name, 
               table_name);
    }

    /* Free memory. */
    free_meta_column(new_meta_column);

    /* Release table. */
    AlterReleaseTable(oid);
}

/* Drop old column. */
static void AlterDropOldColumn(DropColumnDef *drop_column_def, char *table_name, DBResult *result) {
    Oid oid = TableNameFindOid(table_name);

    /* Capture table exclusively. */
    AlterCaptureTable(oid);

    /* Drop column.*/
    if (drop_meta_column(table_name, drop_column_def->column_name)) {
        result->success = true;
        result->message = FormatStr("Drop column '%s' for table '%s' successfully.", 
                                 drop_column_def->column_name, 
                                 table_name);
        db_log(SUCCESS, "Drop column '%s' for table '%s' successfully.", 
               drop_column_def->column_name, 
               table_name);
    }

    /* Release table. */
    AlterReleaseTable(oid);
}

/* Execute alter table statement. */
void exec_alter_statement(AlterTableNode *alter_table_node, DBResult *result) {
    if (CheckForAlterTable(alter_table_node)) {
        AlterTableAction *alter_table_action = alter_table_node->action;
        switch (alter_table_action->type) {
            case ALTER_TO_ADD_COLUMN:
                AlterAddNewColumn(alter_table_action->action.add_column, 
                                  alter_table_node->table_name, result);
                break;
            case ALTER_TO_DROP_COLUMN:
                AlterDropOldColumn(alter_table_action->action.drop_column, 
                                   alter_table_node->table_name, result);
                break;
        }
    }
}
