/********************************** Create Module ********************************************
 * Auth:        JerryZhou
 * Created:     2023/11/08
 * Modify:      2024/11/26
 * Locataion:   src/backend/create.c
 * Description: Create Module is intended to create a table.
 ********************************************************************************************
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "create.h"
#include "strheaptable.h"
#include "data.h"
#include "const.h"
#include "common.h"
#include "table.h"
#include "mmgr.h"
#include "meta.h"
#include "session.h"
#include "asserts.h"
#include "utils.h"
#include "check.h"
#include "copy.h"
#include "free.h"
#include "log.h"
#include "sysTable.h"

/* Get system reserved columns length. */
uint32_t sys_reserved_column_count() {
    return SYS_RESERVED_COLUMNS_LENGTH;
}

/* Calculate meta column length. 
 * If define data len, use defined data length, note that, T_STRING & T_VARCHAR data length will increase 1 for '\0' as end.
 * Otherwise, use system default data length.
 * Note: when array cap more than zere, it means column is array, 
 * column length = data type length * array cap + reserved array number length (sizeof(uint32_t));
 * */
uint32_t calc_column_len(ColumnDefNode *column_def, uint32_t array_cap) {
    DataTypeNode *data_type = column_def->data_type;
    uint32_t column_length = 0;
    switch (data_type->type) {
        case T_VARCHAR: {
            column_length = data_type->len;
            /* Increase for reserving a char of '\0' as end of string. */
            column_length++;
            break;
        }
        case T_CHAR:
        case T_STRING: {
            column_length = default_data_len(data_type->type);
            /* Increase for reserving a char of '\0' as end of string. */
            column_length++;
            break;
        }
        default: {
            column_length = default_data_len(data_type->type);
            break;
        }
    }
    /* If type is array, single data type length multiply by array cap. */
    return array_cap == 0 
            ? (column_length + LEAF_NODE_CELL_NULL_FLAG_SIZE)
            : (column_length * array_cap + LEAF_NODE_ARRAY_NUM_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Column Operation. */
static void operate_column(MetaColumn *meta_column, List *column_def_opt_list) {
    if (!column_def_opt_list) 
        return;

    ListCell *lc;
    foreach (lc, column_def_opt_list) {
        ColumnDefOptNode *column_def_opt = lfirst(lc);
        switch (column_def_opt->opt_type) {
            case OPT_NOT_NULL:
                meta_column->not_null = true;
                break;
            case OPT_UNIQUE:
                meta_column->is_unique = true;
                break;
            case OPT_PRIMARY_KEY:
                meta_column->is_primary = true;
                meta_column->not_null = true;
                break;
            case OPT_DEFAULT_VALUE:
                meta_column->default_value_type = DEFAULT_VALUE;
                meta_column->default_value = get_value_from_value_item_node(column_def_opt->value, meta_column);
                break;
            case OPT_DEFAULT_NULL: 
                meta_column->default_value_type = DEFAULT_VALUE_NULL;
                break;
            case OPT_COMMENT: 
                meta_column->has_comment = true;
                strcpy(meta_column->comment, column_def_opt->comment);
                break;
            case OPT_CHECK_CONDITION:
            case OPT_REFERENECS:
                db_log(ERROR, "Not support thus column def operation yet");
                break;
        }
    }
}

/* Combine user-level column. */
MetaColumn *combine_user_meta_column(ColumnDefNode *column_def, char *table_name) {

    MetaColumn *meta_column = instance(MetaColumn);

    /* Base info. */
    strcpy(meta_column->column_name, column_def->column->column);
    meta_column->is_primary = false;
    meta_column->is_unique = false;
    meta_column->not_null = false;
    meta_column->column_type = column_def->data_type->type;
    meta_column->sys_reserved = false;
    meta_column->array_dim = column_def->array_dim;
    meta_column->array_cap = column_def->array_dim * ARRAY_FLARE_FACTOR;
    meta_column->column_length = calc_column_len(column_def, meta_column->array_cap);
    meta_column->default_value_type = DEFAULT_VALUE_NONE;
    meta_column->default_value = NULL;

    /* Special handling Reference. */
    if (column_def->data_type->type == T_REFERENCE) {
        Table *sub_table = open_table(column_def->data_type->table_name);
        if (sub_table) 
            strcpy(meta_column->table_name, column_def->data_type->table_name);
        else 
            db_log(ERROR, "Table '%s' not exists.", column_def->data_type->table_name);
    }

    /* Operate column. */
    operate_column(meta_column, column_def->column_def_opt_list);

    return meta_column;
}

/* Combine sys-level column. */
MetaColumn *combine_sys_meta_column(char *table_name, int index) {
    MetaColumn *meta_column = instance(MetaColumn);
    memcpy(meta_column, SYS_RESERVED_COLUMNS + index, sizeof(MetaColumn));
    return meta_column;
}

/* Get column def size in create table statement. */
static uint32_t get_column_def_size(CreateTableNode *create_table_node) {
    uint32_t size = 0;
    
    ListCell *lc;
    foreach (lc, create_table_node->base_table_element_commalist) {
        BaseTableElementNode *base_table_element = lfirst(lc);
        if (base_table_element->type == TELE_COLUMN_DEF)
            size++;
    }

    return size;
}

/* Operate table about unique-key columns/ */
static void operate_table_unique(MetaTable *meta_table, List *commalist) {
    ListCell *lc;
    foreach (lc, commalist) {
        ColumnDefName *column_def_name = lfirst(lc);
        MetaColumn *meta_column = get_meta_column_by_name(meta_table, column_def_name->column);
        meta_column->is_unique = true;
        break; /* Not support mult-columns as unique key. */
    }
}

/* Operate table abount primary-key columns */
static void operate_table_primary_key(MetaTable *meta_table, List *commalist) {
    ListCell *lc;
    foreach (lc, commalist) {
        ColumnDefName *column_def_name = lfirst(lc);
        MetaColumn *meta_column = get_meta_column_by_name(meta_table, column_def_name->column);
        meta_column->is_primary = true;
        break; /* Not support mult-columns as primary key. */
    }
}

/* Operate table contraint. */
static void table_operate_contraint(MetaTable *meta_table, TableContraintDefNode *table_contraint) {
    switch (table_contraint->type) {
        case TCONTRAINT_UNIQUE:
            operate_table_unique(meta_table, table_contraint->column_commalist);
            break;
        case TCONTRAINT_PRIMARY_KEY: 
            operate_table_primary_key(meta_table, table_contraint->column_commalist);
            break;
        case TCONTRAINT_FOREIGN_KEY:
        case TCONTRAINT_CHECK:
            db_log(ERROR, "Not support table contraint yet.");
            break;
    }
}

/* Handler user-level none define primary key 
 * In this case, use system reserved column 'sys_id' as primary key.
 * */
void handler_user_none_priamry_key(MetaTable *meta_table) {
    uint32_t i;
    for (i = 0; i < meta_table->column_size; i++) {
        MetaColumn *meta_column = meta_table->meta_column[i];
        if (meta_column->is_primary)
            return;
    }
    
    MetaColumn *sys_id_column = get_all_meta_column_by_name(meta_table, SYS_RESERVED_ID_COLUMN_NAME);
    Assert(sys_id_column);
    sys_id_column->is_primary = true;
}

/* Combine meta table by create table node. */
static MetaTable *combine_meta_table(CreateTableNode *create_table_node) {

    MetaTable *meta_table = instance(MetaTable);

    meta_table->table_name = dstrdup(create_table_node->table_name);
    meta_table->column_size = get_column_def_size(create_table_node); 
    meta_table->all_column_size = meta_table->column_size + SYS_RESERVED_COLUMNS_LENGTH;
    meta_table->meta_column = dalloc(sizeof(MetaColumn *) * meta_table->all_column_size);

    /* User-level defination. */
    uint32_t j = 0;
    ListCell *lc;
    foreach (lc, create_table_node->base_table_element_commalist) {
        BaseTableElementNode *base_table_element = lfirst(lc);
        switch (base_table_element->type) {
            case TELE_COLUMN_DEF:
                meta_table->meta_column[j++] = combine_user_meta_column(base_table_element->column_def, create_table_node->table_name);
                break;
            case TELE_TABLE_CONTRAINT_DEF:
                table_operate_contraint(meta_table, base_table_element->table_contraint_def);
                break;
        }
    }

    /* System-level defination. */
    uint32_t k;
    for (k = j; k < meta_table->all_column_size; k++) {
        meta_table->meta_column[k] = combine_sys_meta_column(meta_table->table_name, (k - j));
    }
    
    /* Handler if user not define priamry key. */
    handler_user_none_priamry_key(meta_table);
    
    return meta_table;
}

/* Execute create table statement. */
void exec_create_table_statement(CreateTableNode *create_table_node, DBResult *result) {

    /* Check valid. */
    if (!check_create_table_node(create_table_node)) 
        return;

    /* Combine MetaTable. */
    MetaTable *meta_table = combine_meta_table(create_table_node);
    if (meta_table == NULL) 
        return;

    /* Create table. 
     * Besides the normal table itself, we alse create its string heap table.
     * Although the table maybe not have any string column.
     * */
    if (create_table(meta_table) && CreateStrHeapTable(meta_table->table_name)) {
        result->success = true;
        result->rows = 0;
        result->message = format("Table '%s' created successfully.", 
                                 create_table_node->table_name);

        db_log(SUCCESS, "Table '%s' created successfully.", 
               create_table_node->table_name);
    }

    free_meta_table(meta_table);
}
