/***************************** Memory Context Module. *************************************** 
 * Auth:        JerryZhou 
 * Created:     2024/10/16 
 * Modify:      2024/11/24 
 * Locataion:   src/memory/mectx.c
 *
 *********************************************************************************************/
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "data.h"
#include "mctx.h"
#include "asctx.h"
#include "utils.h"
#include "parall.h"

/* 
 * Current MemoryContext. 
 */
MemoryContext CURRENT_MEMORY_CONTEXT = NULL;

/* 
 * Store start-up and initialization Info
 */
MemoryContext TOP_MEMORY_CONTEXT = NULL;

/* 
 * Store start-up load object info.
 */
MemoryContext SIDE_MEMORY_CONTEXT = NULL;

/* 
 * Store Post master info.
 */
MemoryContext MASTER_MEMORY_CONTEXT = NULL;

/* 
 * Store fdesc ,xlog info. 
 */
MemoryContext CACHE_MEMORY_CONTEXT = NULL;

/*
 * Stolre message.
 */
MemoryContext MESSAGE_MEMORY_CONTEXT = NULL;

static MemContextRecorder contextRecorders[100];
static int recorderSize;


static MemoryContextMethods mctx_methods[] = {
    /* AllocSetContext implements. */
    [ALLOC_SET_ID].alloc = AllocSetAlloc,
    [ALLOC_SET_ID].free = AllocSetFree,
    [ALLOC_SET_ID].realloc = AllocSetRealloc,
    [ALLOC_SET_ID].reset = AllocSetReset,
    [ALLOC_SET_ID].delete_context = AllocSetDelete
};

/* MemoryContextInit.
 * Start up the memory-context subsystem. */
void MemoryContextInit(void) {
    TOP_MEMORY_CONTEXT = AllocSetMemoryContextCreate(NULL, "TopMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    SIDE_MEMORY_CONTEXT = AllocSetMemoryContextCreate(TOP_MEMORY_CONTEXT, "SideMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
    MemoryContextSwitchTo(TOP_MEMORY_CONTEXT);
}

void RegisterContextRecorders(int workerNum, pthread_t workers[]) {    
    Assert(CURRENT_MEMORY_CONTEXT == MASTER_MEMORY_CONTEXT);
    for (int i = 0; i < workerNum; i++) {
        MemoryContext context = AllocSetMemoryContextCreate(MASTER_MEMORY_CONTEXT, "ParallelComputeMemoryContext", DEFAULT_MAX_BLOCK_SIZE);
        contextRecorders[i].context = context;
        contextRecorders[i].worker = &workers[i];
    }
    recorderSize = workerNum;
}

void DestroyContextRecorders() {
    for (int i = 0; i < recorderSize; i++) {
        MemoryContextDelete(contextRecorders[i].context);
    }
    recorderSize = 0;
}

MemContextRecorder *FindMemContextReorder() {
    Assert(GetComputeMode() == PARALLEL_COMPUTE);
    for (int i = 0; i < recorderSize; i++) {
        MemContextRecorder *recorder = &contextRecorders[i];
        if (*recorder->worker == pthread_self())
            return recorder;
    }
    return NULL;
}


/* Create MemoryContext.
 * Thist abstract function not really to create MemoryContext and it just
 * make up base info and link to others MemoryContext.
 * */
void MemoryContextCreate(MemoryContext node, MemoryContext parent, 
                         const char *name, ContextType type, 
                         MemoryContextMethodID id) {
    /* Make up base Info. */
    node->name = name;
    node->type = type;
    node->parent = parent;
    node->allocated_size = 0;
    node->firstChild = NULL;
    node->presChild = NULL;
    node->nextChild = NULL;
    node->context_methods = &mctx_methods[id];
    
    /* Link node to peer nodes. */
    if (non_null(parent)) {
        if (non_null(parent->firstChild)) 
            parent->firstChild->presChild = node;
        parent->firstChild = node;
    }
}

/* MemoryContextReset. 
 * Release all space allocate within a context and also its children contexts. */
void MemoryContextReset(MemoryContext context) {
    context->context_methods->reset(context);
}

/* MemoryContext set its parent. */
void MemoryContextSetParent(MemoryContext context, MemoryContext new_parent) {
    Assert(context);
    Assert(context != new_parent);

    if (context->parent == new_parent)
        return;

    /* Delink. */
    if (context->parent) {
        MemoryContext parent = context->parent;
        
        if (context->presChild) 
            context->presChild->nextChild = context->nextChild;
        else 
        {
            Assert(parent->firstChild == context);
            parent->firstChild = context->nextChild;
        }

        if (context->nextChild) 
            context->nextChild->presChild = context->presChild;
    }

    if (new_parent) {
        context->parent = new_parent;
        context->presChild = NULL;
        context->nextChild = new_parent->firstChild;
        if (new_parent->firstChild) 
            new_parent->firstChild->presChild = context;
        new_parent->firstChild = context;
    } else {
        context->parent = NULL;
        context->presChild = NULL;
        context->nextChild = NULL;
    }
}

/* Delete the MemoryContext only. */
static void MemoryContextDeleteOnly(MemoryContext context) {
    /* Delink the parent. */
    MemoryContextSetParent(context, NULL);
    context->context_methods->delete_context(context);
}

/* Delete the MemoryContext. */
void MemoryContextDelete(MemoryContext context) {
    Assert(context);

    MemoryContext curcontext;
    
    curcontext = context;
    forever {
        MemoryContext parentcontext;
        while (curcontext->firstChild != NULL)
            curcontext = curcontext->firstChild;

        parentcontext = curcontext->parent;
        MemoryContextDeleteOnly(curcontext);

        if (context == curcontext)
            break;

        curcontext = parentcontext;
    }
}

/* Switch to MemoryContext. */
void *MemoryContextSwitchTo(MemoryContext currentConext) {
    /* Not allowd null. */
    Assert(currentConext != NULL);
    /* Not allowed parallel compute there. */
    // Assert(GetComputeMode() != PARALLEL_COMPUTE);
    MemoryContext old = CURRENT_MEMORY_CONTEXT;
    CURRENT_MEMORY_CONTEXT = currentConext;
    return old;
}

static MemoryContext GetCurrentMemoryContext() {
    switch (GetComputeMode()) {
        case NORMAL_COMPUTE:
            return CURRENT_MEMORY_CONTEXT;
        case PARALLEL_COMPUTE: {
            MemContextRecorder *recorder = FindMemContextReorder();
            Assert(recorder);
            return recorder->context;
        }
        default: {
            perror("Unknown MemMode");
            exit(1);
        }
    }
    return CURRENT_MEMORY_CONTEXT;
}

/* Alloc from MemoryContext. */
void *MemoryContextAlloc(size_t size) {
    MemoryContext context = GetCurrentMemoryContext();
    return context->context_methods->alloc(context, size);
}


/* Free from MemoryContext. */
void MemoryContextFree(void *ptr) {
    MemoryContext context = GetCurrentMemoryContext();
    context->context_methods->free(ptr);
}

/* Realloc from MemoryContext. */
void *MemoryContextRealloc(void *pointer, size_t size) {
    MemoryContext context = GetCurrentMemoryContext();
    return context->context_methods->realloc(pointer, size);
}

/* Strdup from MemoryContext. */
char *MemoryContextStrdup(char *str) {
    size_t len = strlen(str) + 1;
    char *nstr = MemoryContextAlloc(len);
    memcpy(nstr, str, len);
    return nstr;
}
