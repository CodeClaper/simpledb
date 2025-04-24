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
#include <unistd.h>
#include "data.h"
#include "alter.h"
#include "mmgr.h"
#include "check.h"
#include "table.h"
#include "cache.h"
#include "create.h"
#include "tablelock.h"
#include "utils.h"
#include "free.h"
#include "log.h"
#include "tablereg.h"
#include "systable.h"

/* Try to catpture table.
 * If these other session on the table, wait and test. 
 * */
static void try_capture_table(Oid oid) {
    try_acquire_table(oid);
    /* Wait until capture the table exclusively. */
    while (if_shared_table(oid)) {
        usleep(100);
    }
}

/* Release Table. */
static void release_table(Oid oid) {
    remove_table_cache(oid);
    try_release_table(oid);
}

/* Add new Column. */
static void add_new_column(AddColumnDef *add_column_def, char *table_name, DBResult *result) {
    Oid oid = TableNameFindOid(table_name);
    MetaColumn *new_meta_column = combine_user_meta_column(add_column_def->column_def, table_name);        

    if (new_meta_column->is_primary)
        db_log(ERROR, "Not support add primary-key column through alter table.");

    /* Capture table exclusively. */
    try_capture_table(oid);

    if (add_new_meta_column(table_name, new_meta_column, add_column_def->position_def)) {
        result->success = true;
        result->message = format("Add column '%s' for table '%s' successfully.", 
                                 new_meta_column->column_name, 
                                 table_name);
        db_log(SUCCESS, "Add column '%s' for table '%s' successfully.", 
               new_meta_column->column_name, 
               table_name);
    }

    free_meta_column(new_meta_column);

    /* Release table. */
    release_table(oid);
}

/* Drop old column. */
static void drop_old_column(DropColumnDef *drop_column_def, char *table_name, DBResult *result) {
    Oid oid = TableNameFindOid(table_name);

    /* Capture table exclusively. */
    try_capture_table(oid);

    /* Drop column.*/
    if (drop_meta_column(table_name, drop_column_def->column_name)) {
        result->success = true;
        result->message = format("Drop column '%s' for table '%s' successfully.", 
                                 drop_column_def->column_name, 
                                 table_name);
        db_log(SUCCESS, "Drop column '%s' for table '%s' successfully.", 
               drop_column_def->column_name, 
               table_name);
    }

    /* Release table. */
    release_table(oid);
}

/* Execute alter table statement. */
void exec_alter_statement(AlterTableNode *alter_table_node, DBResult *result) {
    if (check_alter_table(alter_table_node)) {
        AlterTableAction *alter_table_action = alter_table_node->action;
        switch (alter_table_action->type) {
            case ALTER_TO_ADD_COLUMN:
                add_new_column(alter_table_action->action.add_column, 
                               alter_table_node->table_name, 
                               result);
                break;
            case ALTER_TO_DROP_COLUMN:
                drop_old_column(alter_table_action->action.drop_column, 
                                alter_table_node->table_name, 
                                result);
                break;
        }
    }
}
