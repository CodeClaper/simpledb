#include <setjmp.h>
#include "data.h"
#include "mctx.h"

#ifndef DEFS_H
#define DEFS_H
extern Conf *conf;                  /* Gloabal Configuration */
extern jmp_buf errEnv;              /* Jmp_buf for error. */
extern const char *program_name;    /* The program name. */

/* Gloabal MemoryContext. */
extern MemoryContext CURRENT_MEMORY_CONTEXT;
extern MemoryContext TOP_MEMORY_CONTEXT;
extern MemoryContext SIDE_MEMORY_CONTEXT;
extern MemoryContext MASTER_MEMORY_CONTEXT;
extern MemoryContext CACHE_MEMORY_CONTEXT;
extern MemoryContext MESSAGE_MEMORY_CONTEXT;
#endif
