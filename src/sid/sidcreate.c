#include <fcntl.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "sidcreate.h"
#include "sidbase.h"
#include "systable.h"
#include "table.h"
#include "mmgr.h"
#include "log.h"
#include "bufmgr.h"

/* Sid create. */
bool CreateSidTableInner(Oid soid) {
    char *file_path;
    int descr;
    void *root_node;

    file_path = table_file_path(soid);
    if (table_file_exist(file_path)) {
        db_log(ERROR, "Sid file '%ld' already exists.", soid);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        db_log(ERROR, "Open database file '%s' fail.", file_path);
        return false;
    }

    root_node = dalloc(PAGE_SIZE);

    SidLeafNodeInitialize(root_node, true);
    
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

/* Sid create. */
bool CreateSidTable(Oid soid, Oid toid, char *table_name) {
    Object entity = GenerateObjectInner(soid, toid, table_name, OSID_TABLE);
    return CreateSidTableInner(soid) && SaveObject(entity);
}


/* Shrink Sid table. */
bool ShrinkSidTable(Oid soid) {
    Buffer root_buffer;
    void *root_node;

    root_buffer = ReadBuffer(soid, ROOT_PAGE_NUM);
    LockBuffer(root_buffer, RW_WRITER);
    root_node = GetBufferPage(root_buffer);

    SidLeafNodeInitialize(root_node, true);

    /* Unlock and release buffer. */
    MakeBufferDirty(root_buffer);
    UnlockBuffer(root_buffer);
    ReleaseBuffer(root_buffer);

    return true;
}

