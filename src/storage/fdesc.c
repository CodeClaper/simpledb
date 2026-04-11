#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include "fdesc.h"
#include "data.h"
#include "mmgr.h"
#include "table.h"
#include "utils.h"
#include "log.h"
#include "sysstate.h"

/* FDescEntry cache. */
static List *fdCache = NIL;

/* Initilise fdesc. */
void init_fdesc() {
    fdCache = create_list(NODE_VOID);
}

/* Find file descriptor in fdCache. 
 * Return file descriptor or -1 if not found.
 * */
static FDesc find_fdesc(Oid oid) {
    Assert(fdCache != NIL);
    
    ListCell *lc;
    foreach(lc, fdCache) {
        FDescEntry *entry = lfirst(lc);
        if (entry->oid == oid)
            return entry->desc;
    }

    return -1;
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

/* Close the file descriptor. */
static void close_file_desc(FDesc fdesc) {
    if (close(fdesc) == -1) {
        db_log(PANIC, "Close table file fail: %s.", strerror(errno));
    }
}

/* Register fdesc. */
static void register_fdesc(Oid oid, FDesc desc) {
    Assert(fdCache != NIL);
    
    /* If sys not running, not register the fdesc. */
    if (!SYS_IS_INITED) return;

    /* Switch to CACHE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

    FDescEntry *entry = instance(FDescEntry);
    entry->desc = desc;
    entry->oid = oid;
    append_list(fdCache, entry);

    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
}

/* Unregister fdesc. */
void unregister_fdesc(Oid oid) {

    /* Switch to CACHE_MEMORY_CONTEXT. */
    MemoryContext oldcontext = CURRENT_MEMORY_CONTEXT;
    MemoryContextSwitchTo(CACHE_MEMORY_CONTEXT);

    ListCell *lc;
    foreach(lc, fdCache) {
        FDescEntry *entry = lfirst(lc);
        if (entry->oid == oid) {
            close_file_desc(entry->desc);
            list_delete(fdCache, entry);
            break;
        }
    }

    /* Recover the MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
}


/* Get file descriptor. 
 * --------------------
 * Fistly find in fdCache.
 * If missing, load file descriptor and register it. */
FDesc get_file_desc(Oid oid) {
    /* Fistly find in fdCache. */
    FDesc desc = find_fdesc(oid);
    /* If missing cache.*/
    if (desc == -1) {
        char *file_path = table_file_path(oid);
        desc = load_file_desc(file_path);
        register_fdesc(oid, desc);
    }
    return desc;
}

