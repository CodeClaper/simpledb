/********************************** Desc Module ********************************************
 * Auth:        JerryZhou
 * Created:     2024/05/21
 * Modify:      2024/05/21
 * Locataion:   src/backend/desc.c
 * Description: Desc modeule is intended to desc or describe table to get table meta info. 
 ********************************************************************************************
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "desc.h"
#include "data.h"
#include "list.h"
#include "mmgr.h"
#include "table.h"
#include "meta.h"
#include "copy.h"
#include "instance.h"
#include "log.h"

/*Get table name.*/
static inline char *DescribeNodeFindTableName(DescribeNode *describe_node) {
    return describe_node->table_name;
}

/* Generate DescribeResult. */
static List *MetaTableGenerateDescribeResult(Oid tid, MetaTable *meta_table) {
    List *list = create_list(NODE_LIST);

    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        List *child_list;
        Table *subTable;
        MetaColumn *meta_column;
        uint32_t column_length;
        bool is_array;

        meta_column = (MetaColumn *)lfirst(lc);
        /* Skip system-reserved column. */
        if (meta_column->sys_reserved) continue;

        child_list = create_list(NODE_KEY_VALUE);
        column_length = CalcUserMetaColumnLen(meta_column);
        is_array = meta_column->array_dim > 0;
        subTable = meta_column->column_type == T_RID || meta_column->column_type == T_STRING 
                 ? open_table_inner(meta_column->type_oid) 
                 : NULL;

        /* filed */
        append_list(
            child_list, 
            new_key_value("field", meta_column->column_name, T_VARCHAR, tid, meta_column->type_oid)
        );
    
        /* key */
        append_list(
            child_list, 
            new_key_value("key", GetKeyTypeName(meta_column), T_VARCHAR, tid, meta_column->type_oid)
        );
    
        /* type */
        append_list(
            child_list, 
            new_key_value("type", 
                          meta_column->column_type == T_RID ?  GET_TABLE_NAME(subTable): GET_DATA_TYPE_NAME(meta_column->column_type), 
                          T_VARCHAR, 
                          tid, meta_column->type_oid)
        );

        /* length */
        append_list(
            child_list, 
            new_key_value("length", &column_length, T_INT, tid, meta_column->type_oid)
        );


        /* array dim */
        append_list(
            child_list, 
            new_key_value("array", &is_array, T_BOOL, tid, meta_column->type_oid)
        );

        /* primary key */
        if (is_array)  {
            append_list(
                child_list, 
                new_key_value("array_dim", &meta_column->array_dim, T_BOOL, tid, meta_column->type_oid)
            );
        }

        /* Default value. */
        switch (meta_column->default_value_type) {
            case DEFAULT_VALUE_NONE:
                break;
            case DEFAULT_VALUE_NULL:
                append_list(
                    child_list, 
                    new_key_value("default", NULL, meta_column->column_type, tid, meta_column->type_oid)
                );
                break;
            case DEFAULT_VALUE:
                append_list(
                    child_list, 
                    new_key_value("default", meta_column->default_value, meta_column->column_type, tid, meta_column->type_oid)
                );
                break;
                
        }

        /* Comment */
        if (meta_column->has_comment) {
            append_list(
                child_list, 
                new_key_value("comment", meta_column->comment, T_VARCHAR, tid, meta_column->type_oid)
            );
        }


        append_list(list, child_list);
    }

    return list;
}

/* Execute describe statment. */
List *exec_describe_statement(DescribeNode *describe_node) {
    char *table_name;
    Table *table;

    table_name = DescribeNodeFindTableName(describe_node); 
    table = open_table(table_name);
    if (table == NULL) {
        db_log(ERROR, "Table '%s' not exists.", table_name);
        return NULL;
    }

    return MetaTableGenerateDescribeResult(GET_TABLE_OID(table), table->meta_table);
}
