/********************************** Drop Module ********************************************
 * Auth:        JerryZhou
 * Created:     2024/05/21
 * Modify:      2024/05/21
 * Locataion:   src/backend/drop.c
 * Description: Drop modeule is intended to drop table. 
 ********************************************************************************************
 */
#include <unistd.h>
#include "data.h"
#include "check.h"
#include "table.h"
#include "index.h"
#include "log.h"
#include "utils.h"
#include "systable.h"
#include "heaptable.h"
#include "strheaptable.h"
#include "tablelock.h"
#include "tablereg.h"
#include "spinlock.h"
#include "tablecache.h"

/* Try to catpture table.
 * If these other session on the table, wait and test. 
 * */
static void BeforeCaptureTable(Oid oid) {
    try_acquire_table(oid);
    /* Wait until capture the table exclusively. */
    while (if_shared_table(oid)) {
        usleep(100);
    }
}

/* Release Table. */
static void AfterReleaseTable(Oid oid) {
    RemoveTableCache(oid);
    try_release_table(oid);
}

/* Execute drop table statment.*/
void ExecuteDropTableStatement(char *table_name, DBResult *result) { 
    Table *table;
    Oid oid;

    /* Check valid. */
    if (!CheckForDropTable(table_name)) return;

    table = open_table(table_name);
    oid = table->oid;

    BeforeCaptureTable(oid);

    /**
     * It will do:
     * (1) Remove related indexs.
     * (2) Remove heap table.
     * (3) Remove str heap table.
     * (4) Remove table itself.
     */
    if (
        IndexDropByTableName(table_name) &&
        DropHeapTable(table_name) && 
        DropStrHeapTable(table_name) &&
        drop_table(table_name)  
    ) {
        result->success = true;
        result->rows = 0;
        result->message = FormatStr("Table '%s' droped successfully.", table_name);
        db_log(SUCCESS, "Table '%s' droped successfully.", table_name);
    }

    AfterReleaseTable(oid);
}


/* Execute drop index statment. */
void ExecuteDropIndexStatement(char *index_name, DBResult *result) {
    Oid oid;
    Object obj;

    /* Check valid. */
    if (!checkForDropIndex(index_name)) return;

    oid = IndexNameFindOid(index_name);
    obj = OidFindObject(oid);
    
    BeforeCaptureTable(obj.toid);
    
    /* Drop index. */
    if (IndexDrop(oid)) {
        result->success = true;
        result->rows = 0;
        result->message = FormatStr("Index '%s' droped successfully.", index_name);
        db_log(SUCCESS, "Index '%s' droped successfully.", index_name);
    }   

    AfterReleaseTable(obj.toid);
}

