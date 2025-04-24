#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include "fdesc.h"
#include "data.h"
#include "mmgr.h"
#include "systable.h"
#include "table.h"
#include "utils.h"
#include "log.h"

/* FDescEntry cache. */
static List *F_DESC_LIST = NIL;

/* Initilise fdesc. */
void init_fdesc() {
    F_DESC_LIST = create_list(NODE_VOID);
}

/* Find file descriptor in F_DESC_LIST. 
 * Return file descriptor or -1 if not found.
 * */
static FDesc find_fdesc(Oid oid) {
    Assert(F_DESC_LIST != NIL);
    
    ListCell *lc;
    foreach(lc, F_DESC_LIST) {
        FDescEntry *entry = lfirst(lc);
        if (entry->oid == oid)
            return entry->desc;
    }

    return -1;
}


/* Register fdesc. */
static void register_fdesc(Oid oid, FDesc desc) {
    Assert(F_DESC_LIST != NIL);

    /* Switch to CACHE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    if (!IS_SYS_ROOT(oid)) 
        MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

    FDescEntry *entry = instance(FDescEntry);
    entry->desc = desc;
    entry->oid = oid;
    append_list(F_DESC_LIST, entry);

    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
}

/* Unregister fdesc. */
void unregister_fdesc(Oid oid) {

    /* Switch to CACHE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    if (!IS_SYS_ROOT(oid)) 
        MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

    ListCell *lc;
    foreach(lc, F_DESC_LIST) {
        FDescEntry *entry = lfirst(lc);
        if (entry->oid == oid) {
            list_delete(F_DESC_LIST, entry);
            break;
        }
    }

    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
}


/* Load file descriptor. 
 * Notice, if file desc not register, need close it manually.
 * */
static FDesc load_file_desc(char *file_path) {
    FDesc desc= open(file_path, O_RDWR, S_IRUSR | S_IWUSR);
    if (desc == -1) 
        db_log(PANIC, "Open table file %s fail: %s.", 
               file_path, 
               strerror(errno));
    return desc;
}

/* Get file descriptor. 
 * --------------------
 * Fistly find in F_DESC_LIST.
 * If missing, load file descriptor and register it. */
FDesc get_file_desc(Oid oid) {

    /* Fistly find in F_DESC_LIST. */
    FDesc desc = find_fdesc(oid);
    /* If missing cache.*/
    if (desc == -1) {
        char *file_path = table_file_path(oid);
        desc = load_file_desc(file_path);
        register_fdesc(oid, desc);
    }

    return desc;
}

