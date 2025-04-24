#include <time.h>
#include "bgwriter.h"
#include "bufmgr.h"
#include "bufpool.h"
#include "defs.h"
#include "asctx.h"
#include "ltree.h"
#include "log.h"

/* Start Memory Context.*/
static void StartMemoryContext() {
    /* Create the TOP_MEMORY_CONTEXT. */
    MASTER_MEMORY_CONTEXT = AllocSetMemoryContextCreate(TOP_MEMORY_CONTEXT, "MasterMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    CACHE_MEMORY_CONTEXT = AllocSetMemoryContextCreate(MASTER_MEMORY_CONTEXT, "CacheMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    MemoryContextSwitchTo(MASTER_MEMORY_CONTEXT);
}

/* Find dirty page and write to storage. */
static void FlushDirtyPage() {
    Index idx;
    for (idx = 0; idx < BUFFER_SLOT_NUM; idx++) {
        BufferDesc *desc = GetBufferDesc(idx);
        if (desc->status == UNPINNED) {
            void *node = GetBufferBlock(desc->buffer);
            if (get_node_state(node) == DIRTY_STATE) {
                PinBuffer(desc);
                /* If write success, make the buffer NORMAL 
                 * to avoid scan it again. */
                if (BufferWriteBlock(desc->buffer))
                    MakeBufferNormal(desc->buffer);
                UnpinBuffer(desc);
            }
        }
    }
}

/* Background writer delay. */
static void BgDelay() {
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = BGWRITER_DELAY;
    nanosleep(&ts, NULL);
}

/* Start the background writer. */
void StartBgWriter() {
    StartMemoryContext();
    forever {
        FlushDirtyPage();
        BgDelay();
    }
}
