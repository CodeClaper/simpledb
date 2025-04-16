#include "strtab.h"
#include "utils.h"
#include <stdbool.h>

#ifndef SYS_H
#define SYS_H

/* System reserved table list. */
static char* SYS_RESERVED_TABLE_LIST [] = { SYS_STRING_TABLE_NAME };

/* If table is system reserved. */
static inline bool if_table_reserved(char *table_name) {
    int i;
    for (i = 0; i < 1; i ++) {
        if (streq(SYS_RESERVED_TABLE_LIST[i], table_name))
            return true;
    }
    return false;
}

#endif 
