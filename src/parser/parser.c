#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "parser.h"
#include "data.h"
#include "utils.h"
#include "log.h"
#include "y.tab.h"
#include "intpr.h"

typedef struct yy_buffer_state *YY_BUFFER_STATE;
extern int yylex(void);
extern YY_BUFFER_STATE yy_scan_string(char *str);
extern void yy_delete_buffer(YY_BUFFER_STATE buffer);
extern int yyparse(List *states);

/* Parse sql and generate statement list. */
List *parse(char *sql) {
    if (sql == NULL) return NULL;
    Trim(sql); 
    db_log(DEBUGER, "Execute sql: %s", sql);

    size_t size = strlen(sql) + 1;
    char buff[size + 1];
    sprintf(buff, "%s%c", sql, '\n');
    buff[size] = '\0';

    /* Scan. */
    YY_BUFFER_STATE buffer = yy_scan_string(buff);
    List *states = create_list(NODE_STATEMENT);
    int ret = yyparse(states);
    yy_delete_buffer(buffer);

    return ret ==  0 ? states : NULL;
}
