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
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include "show.h"
#include "mmgr.h"
#include "common.h"
#include "meta.h"
#include "utils.h"
#include "free.h"
#include "copy.h"
#include "list.h"
#include "asserts.h"
#include "instance.h"
#include "check.h"
#include "table.h"
#include "systable.h"
#include "log.h"

static char *CombineColumnsForShowIndexs(MetaIndex *meta_index) {
    char str[1024];
    uint32_t offset = 0;
    bzero(str, 1024);

    ListCell *lc;
    foreach (lc, meta_index->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *) lfirst(lc);
        uint32_t len = strlen(meta_column->column_name);
        memcpy(str + offset, meta_column->column_name, len);
        offset += len;
        if (last_cell(meta_index->meta_columns) != lc)
            str[offset++] = ',';
    }

    return dstrdup(str);
}

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
            new_key_value(SYS_TABLE_OID_NAME, &entity->oid, T_LONG, OID_ZERO, OID_ZERO)
        );

        /* relname */
        append_list(
            child_list, 
            new_key_value(SYS_TABLE_RELNAME_NAME, entity->relname, T_VARCHAR, OID_ZERO, OID_ZERO)
        );

        /* object type */
        append_list(
            child_list, 
            new_key_value(SYS_TABLE_RELTYPE_NAME, GetObjectTypeName(entity->reltype), T_VARCHAR, OID_ZERO, OID_ZERO)
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
            new_key_value("index_name", meta_index->index_name, T_VARCHAR, OID_ZERO, OID_ZERO)
        );

        /* Table name */
        append_list(
            child_list, 
            new_key_value("table_name", GET_TABLE_NAME(table), T_VARCHAR, OID_ZERO, OID_ZERO)
        );

        /* Is unique. */
        append_list(
            child_list, 
            new_key_value("is_unique", &meta_index->is_unique, T_BOOL, OID_ZERO, OID_ZERO)
        );

        /* Index type. */
        append_list(
            child_list, 
            new_key_value("index_type", GET_INDEX_TYPE_NAME(meta_index->type), T_VARCHAR, OID_ZERO, OID_ZERO)
        );

        /* Columns. */
        append_list(
            child_list,
            new_key_value("columns", CombineColumnsForShowIndexs(meta_index), T_VARCHAR, OID_ZERO, OID_ZERO)
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

