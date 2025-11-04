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
#include "meta.h"
#include "utils.h"
#include "free.h"
#include "copy.h"
#include "list.h"
#include "session.h"
#include "asserts.h"
#include "jsonwriter.h"
#include "instance.h"
#include "check.h"
#include "timer.h"
#include "table.h"
#include "systable.h"
#include "log.h"

static List *ShowForTables() {
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
            new_key_value(SYS_TABLE_OID_NAME, &entity->oid, T_LONG, NULL)
        );

        /* relname */
        append_list(
            child_list, 
            new_key_value(SYS_TABLE_RELNAME_NAME, entity->relname, T_VARCHAR, NULL)
        );

        /* object type */
        append_list(
            child_list, 
            new_key_value(SYS_TABLE_RELTYPE_NAME, GetObjectTypeName(entity->reltype), T_VARCHAR, NULL)
        );

        append_list(list, child_list);
    }
    return list;
}

static List *ShowForIndexs(char *table_name) {
    Table *table = open_table(table_name);
    List *list = create_list(NODE_LIST);

    ListCell *lc;
    foreach(lc, table->meta_indexs) {
        MetaIndex *meta_index = (MetaIndex *) lfirst(lc);
        List *child_list = create_list(NODE_KEY_VALUE);

        /* Index name */
        append_list(
            child_list, 
            new_key_value("index_name", meta_index->index_name, T_VARCHAR, NULL)
        );

        /* Table name */
        append_list(
            child_list, 
            new_key_value("table_name", GET_TABLE_NAME(table), T_VARCHAR, NULL)
        );

        /* Is unique. */
        append_list(
            child_list, 
            new_key_value("is_unique", &meta_index->is_unique, T_BOOL, NULL)
        );

        /* Index type. */
        append_list(
            child_list, 
            new_key_value("index_type", GET_INDEX_TYPE_NAME(meta_index->type), T_VARCHAR, NULL)
        );

        append_list(list, child_list);
    }

    return list;
}

/* Execute show statement. */
void ExecuteShowStatement(ShowNode *show_node, DBResult *result) {
    if (!CheckForShow(show_node)) return;
    switch(show_node->type) {
        case SHOW_TABLES: {
            List *tables = ShowForTables();
            result->success = true;
            result->data = tables;
            result->message = dstrdup("Show tables executed successfully.");
            db_log(SUCCESS, "Show tables statement is executed successfully."); 
            break;
        }
        case SHOW_IDNEXS: {
            List *indexs = ShowForIndexs(show_node->table_name);
            result->success = true;
            result->data = indexs;
            result->message = dstrdup("Show indexs executed successfully.");
            db_log(SUCCESS, "Show indexs statement is executed successfully."); 
            break;
        }
    }
}

