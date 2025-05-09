/********************************** Show Module ********************************************
 * Auth:        JerryZhou
 * Created:     2023/08/13
 * Modify:      2024/11/26
 * Locataion:   src/backend/show.c
 * Description: Show module is intended to show owned tables info. 
 ********************************************************************************************
 */
#include <stddef.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/stat.h>
#include "show.h"
#include "mmgr.h"
#include "common.h"
#include "utils.h"
#include "free.h"
#include "copy.h"
#include "list.h"
#include "session.h"
#include "asserts.h"
#include "jsonwriter.h"
#include "instance.h"
#include "log.h"
#include "timer.h"
#include "systable.h"

#define KB_THRESHOLD 1024
#define MB_THRESHOLD 1024 * KB_THRESHOLD
#define GB_THRESHOLD 1024 * MB_THRESHOLD

/*Gen table map list.*/
static List *gen_table_map_list() {
    List *list = create_list(NODE_LIST);
    List *object_list = FindAllObject(); 

    ListCell *lc;
    foreach(lc, object_list) {
        Object *entity = (Object *) lfirst(lc);

        /* Only display table object and view object. */
        if (!TABLE_OR_VIEW(entity->reltype))
            continue;

        /* map */
        List *child_list = create_list(NODE_KEY_VALUE);

        /* oid */
        append_list(
            child_list, 
            new_key_value(dstrdup(SYS_TABLE_OID_NAME ), &entity->oid, T_LONG)
        );

        /* relname */
        append_list(
            child_list, 
            new_key_value(dstrdup(SYS_TABLE_RELNAME_NAME), entity->relname, T_VARCHAR)
        );

        /* object type */
        append_list(
            child_list, 
            new_key_value(dstrdup(SYS_TABLE_RELTYPE_NAME), GetObjectTypeName(entity->reltype), T_VARCHAR)
        );

        append_list(list, child_list);
    }
    return list;
}


/* Execute show statement. */
void exec_show_statement(ShowNode *show_node, DBResult *result) {
    switch(show_node->type) {
        case SHOW_TABLES: {
            List *map_list = gen_table_map_list();
            result->success = true;
            result->data = map_list;
            result->message = dstrdup("Show tables executed successfully.");
            db_log(SUCCESS, "Show tables statement is executed successfully."); 
            break;
        }
    }
}

