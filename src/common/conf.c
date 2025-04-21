/********************************** Conf Module **********************************************************
 * Auth:        JerryZhou
 * Created:     202r/11/23
 * Modify:      2024/11/26
 * Locataion:   src/common/conf.c
 * Description: The conf module is intended to load the conf file.
 *************************************************************************************************************
 */
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <unistd.h>
#include "data.h"
#include "asserts.h"
#include "conf.h"
#include "mmgr.h"
#include "utils.h"

#define DEFAULT_CONF_FILE "/etc/simpledb.cnf"
#define DEFAULT_LOG_LEVEL DEBUGER

/* Read config file. */
char *read_conf(char *title, char *key) {
    FILE *file = fopen(DEFAULT_CONF_FILE, "r");
    if (file == NULL) {
        fprintf(stderr, "Try to open config file in path '%s' fail.\n", DEFAULT_CONF_FILE);
        exit(1);
    }
    char buff[BUFF_SIZE];
    char *p;
    bool inBlock = false;
    while(!feof(file)) {
        char *ret = fgets(buff, BUFF_SIZE, file);
        assert_not_null(ret, "Read conf file error.");
        char *line = trim(buff);
        /* Check if commenter line. */
        if (startwith(line, "//") || startwith(line, "#"))
            continue;
        /* Check if the title */
        if (startwith(line, "[") && startwith(line + 1, title)) {
            inBlock = true;    
            continue;
        }
        /* If not in the title block, skip it. */
        if (!inBlock) continue;
        /* Check if has symbol '='. */
        if (strchr(line, '=') == NULL)
            continue;
        /* Check if this line contains key. */
        if (!startwith(line, key))
            continue;
        p = strstr(line, key);
        assert_not_null(p, "Read configuration error.");
        p += strlen(key);
        trim(p);
        /* skip '=' */
        p += 1;
        return dstrdup(trim(p));
    }
    fprintf(stderr, "Not found [%s].[%s] in configuration file.\n", title, key);
    exit(EXECUTE_FAIL);
}

static LogLevel define_log_level(char *level) {
    if (strcasecmp(level, "TRACE") == 0)
        return TRACE;
    else if (strcasecmp(level, "DEBUG") == 0)
        return DEBUGER;
    else if (strcasecmp(level, "INFO") == 0)
        return INFO;
    else if (strcasecmp(level, "WARN") == 0)
        return WARN;
    else if (strcasecmp(level, "ERROR") == 0)
        return ERROR;
    else
        return DEFAULT_LOG_LEVEL;
}

/* Append directory end character [/] */
static char* append_dir_end(char *dir) {
    Assert(dir);
    size_t size = strlen(dir);
    if (dir[size - 1] == '/')
        return dir;
    else {
        char *append = dalloc(size + 1);
        sprintf(append, "%s/", dir);
        return append;
    }
}

/* Define bool value. */
static bool define_bool_value(char *title, char *key) {
    char *bstr = read_conf(title, key);
    return streq("true", bstr);
}


/* Load configuration. */
Conf *load_conf() {
    Conf *conf = instance(Conf);
    conf->data_dir = append_dir_end(read_conf("data", "dir"));
    conf->port = (ushort)atoi(read_conf("base", "port"));
    conf->log_dir = append_dir_end(read_conf("log", "dir"));
    conf->log_level = define_log_level(read_conf("log", "level"));
    conf->auto_rollback = define_bool_value("transaction", "auto_rollback");
    conf->account = read_conf("auth", "account");
    conf->password = read_conf("auth", "password");
    conf->first_user_oid = atoi(read_conf("file", "first_user_oid"));
    return conf;
}
