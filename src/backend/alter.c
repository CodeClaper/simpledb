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
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include "alter.h"
#include "mmgr.h"
#include "check.h"
#include "table.h"
#include "tablecache.h"
#include "create.h"
#include "tablelock.h"
#include "utils.h"
#include "copy.h"
#include "free.h"
#include "meta.h"
#include "tuple.h"
#include "row.h"
#include "ltindex.h"
#include "log.h"
#include "tablereg.h"
#include "systable.h"
#include "sidinsert.h"
#include "ridinsert.h"
#include "heaptable.h"
#include "ltinsert.h"
#include "instance.h"

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

/* Find the postion via ColumnPositionDef. */
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

/* Modify Table for append meta column. */
static void TableModifyForAppendColumn(Table *table, MetaColumn *new_meta_column, int pos) {
    int i, offset = 0;
    MetaTable *meta_table;

    switch_shared();

    meta_table = table->meta_table;
    Assert(meta_table != NULL);

    /* Append new column. */
    MetaColumn *duplica = copy_meta_column(new_meta_column);
    append_list_at(meta_table->meta_columns, duplica, pos);

    /* Increase column number. */
    meta_table->column_size++;
    meta_table->all_column_size++;

    /* Set offset. */
    for (i = 0; i < table->meta_table->all_column_size; i++) {
        MetaColumn *current = lfirst(list_nth_cell(meta_table->meta_columns, i));
        current->offset = offset;
        offset += current->column_length;
    }

    /* Expnad the heap_value_len. */
    table->heap_value_len += duplica->column_length;

    switch_local();
}

/* Modify table for drop meta column. */
static void TableModifyForDropColumn(Table *table, MetaColumn *meta_column, int pos) {
    int i, offset = 0;
    MetaTable *meta_table;

    switch_shared();

    meta_table = table->meta_table;
    Assert(meta_table != NULL);

    /* Delete the column. */
    list_delete_at(meta_table->meta_columns, pos);

    /* Decrease column number. */
    meta_table->column_size--;
    meta_table->all_column_size--;

    /* Set offset. */
    for (i = 0; i < table->meta_table->all_column_size; i++) {
        MetaColumn *current = lfirst(list_nth_cell(meta_table->meta_columns, i));
        current->offset = offset;
        offset += current->column_length;
    }

    /* Delete the heap_value_len. */
    table->heap_value_len -= meta_column->column_length;

    switch_local();
}

/* Loop heap table and reinsert into btree. */
static void LoopHeapTableAndReinsert(Table *table) {
    Oid oid, hoid, soid, roid;
    Refer *refer;
    MetaColumn *primary_meta_column;
    void *tuple, *key, *value;
    Rid rid; Sid sid;

    oid = table->oid;
    hoid = table->hoid;
    soid = table->soid;
    roid = table->roid;
    refer = new_refer(hoid, 0, 0);
    primary_meta_column = MetaTableFindPrimaryKey(table->meta_table);

    /* Keep loop and reinsert until there is no tuple. */
    while ((tuple = HeapTableLookupTuple(oid, refer)) != NULL) {
        key = TupleFindValue(tuple, primary_meta_column);
        value = GenerateIndex(oid, tuple, refer);
        sid = TupleGetSysId(tuple, table->meta_table);
        rid = TupleGetRefId(tuple, table->meta_table);
    
        /* Reinsert into main index table. */
        BtreeInsert(oid, key, value);

        /* Reinsert into sid index table. */
        SidInsert(soid, sid, refer);

        /* Reinsert into rid index table. */
        RidInsert(roid, rid, refer);

        /* Iterate refer. */
        HeapTableIteratorRefer(refer);

        dfree(value);
    }

    dfree(refer);
}

/* Add new column inner. */
static bool AlterAddNewColumnInner(Oid oid, MetaColumn *new_meta_column, ColumnPositionDef *position_def) {
    int pos;
    Table *table;

    table = open_table_inner(oid);
    pos = ColumnPositionDefFindPos(table->meta_table, position_def);

    /* Append to heap able. */
    HeapTableAppendColumn(oid, new_meta_column, pos);
    
    /* Append meta column to meta table. */
    TableModifyForAppendColumn(table, new_meta_column, pos);

    /* Shrink the table. */
    ShrinkTable(table);

    /* Loop heap table and reinsert. */
    LoopHeapTableAndReinsert(table);

    return true;
}

/* Drop column inner. */
static bool AlterDropColumnInnder(Oid oid, DropColumnDef *drop_column_def) {
    Table *table;
    uint32_t pos;
    MetaColumn *meta_column;

    table = open_table_inner(oid);
    pos = NameFindMetaColumnPostion(table->meta_table, drop_column_def->column_name);
    meta_column = NameFindMetaColumn(table->meta_table, drop_column_def->column_name);

    /* Drop heaop table column. */
    HeapTableDropColumn(oid, meta_column, pos);
    
    /* Append meta column to meta table. */
    TableModifyForDropColumn(table, meta_column, pos);

    /* Shrink the table. */
    ShrinkTable(table);
    
    /* Loop heap table and reinsert. */
    LoopHeapTableAndReinsert(table);

    return true;
}

/* Add new Column. */
static void AlterAddNewColumn(AddColumnDef *add_column_def, char *table_name, DBResult *result) {
    Oid oid;
    Table *table;
    MetaColumn *new_meta_column;

    oid = TableNameFindOid(table_name);
    table = open_table_inner(oid);
    new_meta_column = ColumnDefNodeGenerateMetaColumn(oid, table->stid, add_column_def->column_def);        

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
               new_meta_column->column_name, table_name);
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
    if (AlterDropColumnInnder(oid, drop_column_def)) {
        result->success = true;
        result->message = FormatStr("Drop column '%s' for table '%s' successfully.", 
                                    drop_column_def->column_name, table_name);
        db_log(SUCCESS, "Drop column '%s' for table '%s' successfully.", 
               drop_column_def->column_name, table_name);
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
