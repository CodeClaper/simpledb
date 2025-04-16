#include <fcntl.h>
#include "data.h"
#include "strtab.h"
#include "table.h"
#include "log.h"
#include "mmgr.h"


/* Create system string table. */
static void CreateSysStringTable(char *file_path) {
    int descr;
    
    AssertFalse(table_file_exist(file_path));
    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(PANIC, "Open database file '%s' fail.\n", file_path);
        dfree(file_path);
    }
}

/* Init system String table. 
 * --------------------------
 * Try to load sys_string table, 
 * if fail, create one.
 * */ 
void InitSysStringTable() {
    char *file_path;

    file_path = table_file_path(SYS_STRING_TABLE_NAME);
    if (table_file_exist(file_path)) {
        dfree(file_path);
        return;
    } else {
        CreateSysStringTable(file_path);
        dfree(file_path);
    }
}
