#include "data.h"

#ifndef MCTX_H
#define MCTX_H

typedef struct MemoryContextData* MemoryContext; 

typedef enum {
    ALLOC_SET_CTX
} ContextType;

/* The unique identifier of implements of MemoryContext. */
typedef enum MemoryContextMethodID {
    ALLOC_SET_ID
} MemoryContextMethodID;

typedef struct MemoryContextMethods {
    void *(*alloc)(MemoryContext context, size_t size);
    void (*free)(void *ptr);
    void *(*realloc)(void *ptr, size_t size);
    void (*reset)(MemoryContext context);
    void (*delete_context)(MemoryContext context);
} MemoryContextMethods;

typedef struct MemoryContextData {
    const char *name;                           /* MemoryContext name. */
    ContextType type;                           /* MemoryContext type. */
    uint32_t allocated_size;                    /* Has allocated size.*/
    MemoryContextMethods *context_methods;      /* Vertual functon table. */
    MemoryContext parent;                       /* The parent MemoryContext. */
    MemoryContext firstChild;                   /* The first MemoryContext child. */
    MemoryContext presChild;                    /* The previous MemoryContext of peers. */
    MemoryContext nextChild;                    /* The next MemoryContext of peers. */
} MemoryContextData;

typedef struct MemContextRecorder {
    MemoryContext context;
    pthread_t *worker;
} MemContextRecorder;


void MemoryContextInit(void);
void RegisterContextRecorders(int workerNum, pthread_t workers[]);
void DestroyContextRecorders();

/* Create MemoryContext.
 * Thist abstract function not really to create MemoryContext and it just
 * make up the relation of MemoryContext.
 * */
void MemoryContextCreate(MemoryContext node, MemoryContext parent, const char *name, ContextType type, MemoryContextMethodID id);

/* MemoryContextReset. 
 * Release all space allocate within a context and also its children contexts. */
void MemoryContextReset(MemoryContext context);

/* Delete the MemoryContext. */
void MemoryContextDelete(MemoryContext node);

/* Switch to MemoryContext. */
void *MemoryContextSwitchTo(MemoryContext currentConext);

/* Alloc from MemoryContext. */
void *MemoryContextAlloc(size_t size);

/* Free from MemoryContext. */
void MemoryContextFree(void *ptr);

/* Realloc from MemoryContext. */
void *MemoryContextRealloc(void *pointer, size_t size);

/* Strdup from MemoryContext. */
char *MemoryContextStrdup(char *str);

#endif
