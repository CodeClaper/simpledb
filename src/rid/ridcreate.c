#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "ridcreate.h"
#include "ridbase.h"
#include "systable.h"
#include "table.h"
#include "mmgr.h"
#include "log.h"
#include "bufmgr.h"
#include "buftable.h"
#include "fdesc.h"
#include "trans.h"

/* Rid create. */
bool CreateRidTableInner(Oid roid) {
    char *file_path;
    int descr;
    void *root_node;

    file_path = table_file_path(roid);
    if (table_file_exist(file_path)) {
        logger(ERROR, "Rid file '%ld' already exists.", roid);
        return false;
    }

    descr = open(file_path, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (descr == -1) {
        logger(ERROR, "Open database file '%s' fail.", file_path);
        return false;
    }

    root_node = dalloc(PAGE_SIZE);

    RidLeafNodeInitialize(root_node, true);
    
    /* Flush to disk. */
    lseek(descr, 0, SEEK_SET);
    ssize_t w_size = write(descr, root_node, PAGE_SIZE);
    if (w_size == -1) {
        logger(ERROR, "Write index meta info error and error message: %s.", strerror(errno));
        return false;
    }

    /* Close desription. */
    close(descr);

    /* Free memory. */
    dfree(file_path);
    dfree(root_node);

    return true;
}

/* Create rid table. */
bool CreateRidTable(Oid roid, Oid toid, char *table_name) {
    Object entity = GenerateObjectInner(roid, toid, table_name, ORID_TABLE);
    return CreateRidTableInner(roid) && SaveObject(entity);
}

/* Drop table file drom disk. */
static bool DropRidTableFromDisk(Oid roid) {
    char *heap_table_file = table_file_path(roid);
    if (!check_table_exist_direct(roid)) {
        logger(ERROR, "Heap table file '%s' not exists, error : %s", 
               heap_table_file, strerror(errno));
        return false;
    }
    
    /* It will do:
     * (1) Remove table buffer. 
     * (2) Remove file from disk. 
     * */
    if (
        RemoveTableBuffer(roid) &&
        remove(heap_table_file) == 0
    ) {
        /* Unregister fdesc. */
        unregister_fdesc(roid);
        return true;
    }

    return false;
}

/* Drop the rid table. */
bool DropRidTable(Oid roid) {
    /* Todo list:
     * (1) Regster the DropRidTableFromDisk to trans commit event. 
     * (2) Remove object systable. */
    if (
        RemoveObject(roid) &&
        RegisterCommitEvent(DropRidTableFromDisk, roid) 
    ) return true;

    /* Not reach here logically. */
    logger(ERROR, "Try to drop rid file '%ld' fail, error : %s", 
           roid, strerror(errno));

    return false;
}


/* Shrink rid table. */
bool ShrinkRidTable(Oid roid) {
    Buffer root_buffer;
    void *root_node;

    root_buffer = ReadBuffer(roid, ROOT_PAGE_NUM);
    LockBuffer(root_buffer, RW_WRITER);
    root_node = GetBufferPage(root_buffer);

    RidLeafNodeInitialize(root_node, true);

    /* Unlock and release buffer. */
    MakeBufferDirty(root_buffer);
    UnlockBuffer(root_buffer);
    ReleaseBuffer(root_buffer);

    return true;
}

