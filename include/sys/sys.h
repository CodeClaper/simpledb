#include <stdbool.h>
#include "strheaptable.h"
#include "utils.h"

#ifndef SYS_H
#define SYS_H

/* System reserved table list. */
static char* SYS_RESERVED_TABLE_LIST [] = { };

/* If table is system reserved. */
static inline bool if_table_reserved(char *table_name) {
    return false;
}

#endif 
