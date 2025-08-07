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
#include "data.h"
#include "desc.h"
#include "list.h"
#include "mmgr.h"
#include "table.h"
#include "index.h"
#include "meta.h"
#include "copy.h"
#include "session.h"
#include "instance.h"
#include "log.h"

/*Get table name.*/
static inline char *get_desc_table_name(DescribeNode *describe_node) {
    return describe_node->table_name;
}

/* Generate DescribeResult. */
static List *gen_describe_result(MetaTable *meta_table) {

    List *list = create_list(NODE_LIST);

    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        /* Skip system-reserved column. */
        if (meta_column->sys_reserved)
            continue;

        List *child_list = create_list(NODE_KEY_VALUE);

        /* filed */
        append_list(
            child_list, 
            new_key_value("field", meta_column->column_name, T_VARCHAR, meta_table->table_name)
        );
    
        /* key */
        append_list(
            child_list, 
            new_key_value("key", key_type_name(meta_column), T_VARCHAR, meta_table->table_name)
        );

        /* type */
        append_list(
            child_list, 
            new_key_value("type", 
                          meta_column->column_type == T_REFERENCE ? meta_column->table_name : data_type_name(meta_column->column_type), 
                          T_VARCHAR, 
                          meta_table->table_name)
        );

        /* length */
        uint32_t column_length = calc_raw_meta_column_len(meta_column);
        append_list(
            child_list, 
            new_key_value("length", &column_length, T_INT, meta_table->table_name)
        );


        /* array dim */
        bool is_array = meta_column->array_dim > 0;
        append_list(
            child_list, 
            new_key_value("array", &is_array, T_BOOL, meta_table->table_name)
        );

        /* primary key */
        if (is_array)  {
            append_list(
                child_list, 
                new_key_value("array_dim", &meta_column->array_dim, T_BOOL, meta_table->table_name)
            );
        }

        /* Default value. */
        switch (meta_column->default_value_type) {
            case DEFAULT_VALUE_NONE:
                break;
            case DEFAULT_VALUE_NULL:
                append_list(
                    child_list, 
                    new_key_value("default", NULL, meta_column->column_type, meta_table->table_name)
                );
                break;
            case DEFAULT_VALUE:
                append_list(
                    child_list, 
                    new_key_value("default", meta_column->default_value, meta_column->column_type, meta_table->table_name)
                );
                break;
                
        }

        /* Comment */
        if (meta_column->has_comment) {
            append_list(
                child_list, 
                new_key_value("comment", meta_column->comment, T_VARCHAR, meta_table->table_name)
            );
        }


        append_list(list, child_list);
    }

    return list;
}

/* Execute describe statment. */
List *exec_describe_statement(DescribeNode *describe_node) {
    char *table_name = get_desc_table_name(describe_node); 
    Table *table = open_table(table_name);
    if (table == NULL) {
        db_log(ERROR, "Table '%s' not exists.", table_name);
        return NULL;
    }
    return gen_describe_result(table->meta_table);
}
