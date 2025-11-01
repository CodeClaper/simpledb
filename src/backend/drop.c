/********************************** Drop Module ********************************************
 * Auth:        JerryZhou
 * Created:     2024/05/21
 * Modify:      2024/05/21
 * Locataion:   src/backend/drop.c
 * Description: Drop modeule is intended to drop table. 
 ********************************************************************************************
 */
#include "data.h"
#include "check.h"
#include "table.h"
#include "index.h"
#include "log.h"
#include "utils.h"
#include "heaptable.h"
#include "strheaptable.h"

/* Execute drop table statment.*/
void ExecuteDropTableStatement(char *table_name, DBResult *result) { 
    if (
        CheckForDropTable(table_name) && 
        drop_table(table_name) && 
        DropHeapTable(table_name) && 
        DropStrHeapTable(table_name)
    ) {
        result->success = true;
        result->rows = 0;
        result->message = FormatStr("Table '%s' droped successfully.", table_name);
        db_log(SUCCESS, "Table '%s' droped successfully.", table_name);
    }
}


/* Execute drop index statment. */
void ExecuteDropIndexStatement(char *index_name, DBResult *result) {
    if (IndexDrop(index_name)) {
        result->success = true;
        result->rows = 0;
        result->message = FormatStr("Index '%s' droped successfully.", index_name);
        db_log(SUCCESS, "Index '%s' droped successfully.", index_name);
    }   
}

