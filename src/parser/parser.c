#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "parser.h"
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
    if (sql == NULL)
        return NULL;

    trim(sql); /* Remove space characters, includes '\f', '\n', '\r', '\t', '\v'*/
    db_log(INFO, "Execute sql: %s", sql);

    size_t size = strlen(sql);
    char buff[size + 1];
    sprintf(buff, "%s%c", sql, '\n');
    /* Scan. */
    yy_scan_string(buff);

    List *states = create_list(NODE_STATEMENT);

    return yyparse(states) == 0 ? states : NULL;
}
