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
#include "log.h"
#include "utils.h"
#include "strheaptable.h"

/* Execute drop table statment.*/
void exec_drop_table_statement(char *table_name, DBResult *result) { 
    if (check_drop_table(table_name) && drop_table(table_name) && DropStrHeapTable(table_name)) {
        result->success = true;
        result->rows = 0;
        result->message = format("Table '%s' droped successfully.", table_name);
        db_log(SUCCESS, "Table '%s' droped successfully.", table_name);
    }
}
