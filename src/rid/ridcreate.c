#include <fcntl.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "ridcreate.h"
#include "ridbase.h"
#include "systable.h"
#include "table.h"
#include "mmgr.h"
#include "log.h"

/* Rid create. */
bool CreateRidTableInner(Oid roid) {
    char *file_path;
    int descr;
    void *root_node;

    file_path = table_file_path(roid);
    if (table_file_exist(file_path)) {
        db_log(ERROR, "Rid file '%ld' already exists.", roid);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(ERROR, "Open database file '%s' fail.", file_path);
        return false;
    }

    root_node = dalloc(PAGE_SIZE);

    RidLeafNodeInitialize(root_node, true);
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    ssize_t w_size = write(descr, root_node, PAGE_SIZE);
    if (w_size == -1) {
        db_log(ERROR, "Write index meta info error and error message: %s.", strerror(errno));
        return false;
    }

    /* Close desription. */
    close(descr);

    /* Free memory. */
    dfree(file_path);
    dfree(root_node);

    return true;
}

/* Rid create. */
bool CreateRidTable(Oid roid, Oid toid, char *table_name) {
    Object entity = GenerateObjectInner(roid, toid, table_name, ORID_TABLE);
    return CreateRidTableInner(roid) && SaveObject(entity);
}
