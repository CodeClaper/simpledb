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
#include <string.h>
#include <unistd.h>
#include "create.h"
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
#include "table.h"
#include "index.h"
#include "tablelock.h"
#include "tablereg.h"
#include "tablecache.h"
#include "strheaptable.h"
#include "arrheaptable.h"
#include "heaptable.h"
#include "systable.h"
#include "sidcreate.h"
#include "ridcreate.h"

/* Calculate meta column length. 
 * -----------------------------
 * If define data len, use defined data length.
 * Note that: 
 * (1) T_STRING & T_VARCHAR data length will increase 1 for '\0' as end. Otherwise, use system default data length.
 * (2) When array cap more than zere, it means column is array. We use array heap table to store array values. 
 *     So column length = REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE;
 * */
static uint32_t CalcMetaColumnLength(ColumnDefNode *column_def) {
    DataTypeNode *data_type = column_def->data_type;
    uint32_t column_length = 0;
    switch (data_type->type) {
        case T_VARCHAR: {
            column_length = data_type->len;
            /* Increase for reserving a char of '\0' as end of string. */
            column_length++;
            break;
        }
        case T_CHAR: {
            column_length = DataTypeDefaultLength(data_type->type);
            /* Increase for reserving a char of '\0' as end of string. */
            column_length++;
            break;
        }
        default: {
            column_length = DataTypeDefaultLength(data_type->type);
            break;
        }
    }
    /* If type is array, single data type length multiply by array cap. */
    return column_def->array_dim == 0 
            ? (column_length + LEAF_NODE_CELL_NULL_FLAG_SIZE)
            : (REFER_SIZE + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Column Operation. */
static void ColumnDefOptListForMetaColumn(MetaColumn *meta_column, List *column_def_opt_list) {
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
                meta_column->is_unique = true;
                meta_column->not_null = true;
                break;
            case OPT_DEFAULT_VALUE:
                meta_column->default_value_type = DEFAULT_VALUE;
                meta_column->default_value = ValueItemNodeAssignValue(column_def_opt->value, meta_column);
                /* You can use indirect refer value as default value, but is must exist. */
                if (meta_column->column_type == T_RID) {
                    Rid rid = *(Rid *) meta_column->default_value;
                    if (ZERO_RID(rid))
                        logger(ERROR, "Try to use refer value as default value, but it does not exist.");
                }
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
                logger(ERROR, "Not support thus column def operation yet");
                break;
        }
    }
}

/* Combine user-level column. */
MetaColumn *ColumnDefNodeGenerateMetaColumn(Oid tid, Oid stid, Oid aoid, ColumnDefNode *column_def) {
    MetaColumn *meta_column = instance(MetaColumn);

    /* Base info. */
    meta_column->tid = tid;
    memcpy(meta_column->column_name, column_def->column->column, MAX_COLUMN_NAME_LEN);
    meta_column->is_primary = false;
    meta_column->is_unique = false;
    meta_column->not_null = false;
    meta_column->column_type = column_def->data_type->type;
    meta_column->sys_reserved = false;
    meta_column->array_dim = column_def->array_dim;
    meta_column->column_length = CalcMetaColumnLength(column_def);
    meta_column->default_value_type = DEFAULT_VALUE_NONE;
    meta_column->default_value = NULL;

    /* Special handling Reference, record the refer table name. */
    if (column_def->data_type->type == T_RID) {
        Table *sub_table = open_table(column_def->data_type->table_name);
        if (sub_table) 
            meta_column->type_oid = GET_TABLE_OID(sub_table);
        else 
            logger(ERROR, "Table '%s' not exists.", column_def->data_type->table_name);
    }

    /* Special handling STRING, record the strheaptable name. */
    if (column_def->data_type->type == T_STRING) 
        meta_column->type_oid = stid;

    /* Special handling ARRAY. */
    if (column_def->array_dim > 0)
        meta_column->aoid = aoid;

    /* Operate column. */
    ColumnDefOptListForMetaColumn(meta_column, column_def->column_def_opt_list);

    return meta_column;
}

/* Combine sys-level column. */
static MetaColumn *GenerateSystemMetaColumn(char *table_name, int index) {
    MetaColumn *meta_column = instance(MetaColumn);
    memcpy(meta_column, SYS_RESERVED_COLUMNS + index, sizeof(MetaColumn));
    return meta_column;
}

/* Get column def size in create table statement. */
static uint32_t CreateTableNodeColumnSize(CreateTableNode *create_table_node) {
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
static void OperateContraintForUnique(MetaTable *meta_table, List *commalist) {
    ListCell *lc;
    foreach (lc, commalist) {
        ColumnDefName *column_def_name = lfirst(lc);
        MetaColumn *meta_column = NameFindMetaColumn(meta_table, column_def_name->column);
        meta_column->is_unique = true;
        break; /* Not support mult-columns as unique key. */
    }
}

/* Operate table abount primary-key columns */
static void OperateContraintForPrimaryKey(MetaTable *meta_table, List *commalist) {
    ListCell *lc;
    foreach (lc, commalist) {
        ColumnDefName *column_def_name = lfirst(lc);
        MetaColumn *meta_column = NameFindMetaColumn(meta_table, column_def_name->column);
        meta_column->is_primary = true;
        meta_column->is_unique = true;
        meta_column->not_null = true;
    }
}

/* Operate table contraint. */
static void OperateContraint(MetaTable *meta_table, TableContraintDefNode *table_contraint) {
    switch (table_contraint->type) {
        case TCONTRAINT_UNIQUE:
            OperateContraintForUnique(meta_table, table_contraint->column_commalist);
            break;
        case TCONTRAINT_PRIMARY_KEY: 
            OperateContraintForPrimaryKey(meta_table, table_contraint->column_commalist);
            break;
        case TCONTRAINT_FOREIGN_KEY:
        case TCONTRAINT_CHECK:
            logger(ERROR, "Not support table contraint yet.");
            break;
    }
}

/* Handler user-level none define primary key 
 * In this case, use system reserved column 'sys_id' as primary key.
 * */
static void GenerateSystemPrimaryKey(MetaTable *meta_table) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->is_primary)
            return;
    }
    
    MetaColumn *sys_id_column = NameFindAllMetaColumn(meta_table, SYS_RESERVED_ID_COLUMN_NAME);
    Assert(sys_id_column);
    sys_id_column->is_primary = true;
}

/* Combine meta table by create table node. */
static MetaTable *CreateTableNodeGenerateMetaTable(Oid toid, Oid stoid, Oid aoid, CreateTableNode *create_table_node) {
    MetaTable *meta_table = instance(MetaTable);
    meta_table->table_name = dstrdup(create_table_node->table_name);
    meta_table->column_size = CreateTableNodeColumnSize(create_table_node); 
    meta_table->all_column_size = meta_table->column_size + SYS_RESERVED_COLUMNS_LENGTH;
    meta_table->meta_columns = create_list(NODE_META_COLUMN);

    /* User-level defination. */
    uint32_t j = 0, offset = 0;
    ListCell *lc;
    foreach (lc, create_table_node->base_table_element_commalist) {
        BaseTableElementNode *base_table_element = lfirst(lc);
        switch (base_table_element->type) {
            case TELE_COLUMN_DEF: {
                MetaColumn *current = ColumnDefNodeGenerateMetaColumn(toid, stoid, aoid, base_table_element->column_def);
                current->offset = offset;
                append_list(meta_table->meta_columns, current);
                offset += current->column_length;
                j++;
                break;
            }
            case TELE_TABLE_CONTRAINT_DEF:
                OperateContraint(meta_table, base_table_element->table_contraint_def);
                break;
        }
    }

    /* System-level defination. */
    uint32_t k;
    for (k = j; k < meta_table->all_column_size; k++) {
        MetaColumn *current = GenerateSystemMetaColumn(meta_table->table_name, (k - j));
        current->offset = offset;
        append_list(meta_table->meta_columns, current);
        offset += current->column_length;
    }
    
    /* Handler if user not define priamry key. */
    GenerateSystemPrimaryKey(meta_table);
    
    return meta_table;
}

static MetaIndex *GenerateMetaIndexForCreateIndex(Oid oid, Table *table, CreateIndexNode *create_index_node) {
    MetaIndex *meta_index = instance(MetaIndex);
    meta_index->oid = oid;
    meta_index->tid = GET_TABLE_OID(table);
    meta_index->index_name = dstrdup(create_index_node->index_name);
    meta_index->is_unique = create_index_node->is_unique;
    meta_index->type = create_index_node->type;
    meta_index->column_size = create_index_node->columns->size;
    meta_index->meta_columns = create_list(NODE_META_COLUMN);
    meta_index->page_num = 1;
    meta_index->key_len = 0;
    meta_index->value_len = REFER_SIZE;
    
    ListCell *lc;
    foreach (lc, create_index_node->columns) {
        ColumnNode *column_node = (ColumnNode *) lfirst(lc);
        MetaColumn *meta_column = NameFindMetaColumn(table->meta_table, column_node->column_name);
        append_list(meta_index->meta_columns, meta_column);
        meta_index->key_len += meta_column->column_length;
    }

    return meta_index;
}

/* Save to table cache. */
static bool PrepareSaveTableCache(Oid oid, Oid soid, Oid roid, Oid hid, Oid stid, MetaTable *meta_table) {
    /* Combine table. */
    Table *table = instance(Table);
    table->oid = oid;
    table->soid = soid;
    table->roid = roid;
    table->hoid = hid;
    table->stid = stid;
    table->meta_table = meta_table;
    table->root_page_num = ROOT_PAGE_NUM;
    table->page_size = 1;
    table->rid_page_size = 1;
    table->sid_page_size = 1;
    table->creator = getpid();
    table->key_len = TableCalcPrimaryKeyLength(table);
    table->index_value_len = TableCalcIndexLength(table);
    table->heap_value_len = TableCalcRowLength(table);
    table->meta_indexs = LoadMetaIndex(oid, table); 

    /* Save into cache. */
    SaveTableCache(table);

    return true;
}

/* Save table object. */
static bool SaveTableObject(Oid oid, char *table_name) {
    Object entity = GenerateObjectInner(oid, oid, table_name, OTABLE);
    return SaveObject(entity);
}

/* Save index object. */
static bool SaveIndexObject(Oid oid, Oid toid, char *index_name) {
    Object entity = GenerateObjectInner(oid, toid, index_name, OINDEX);
    return SaveObject(entity);
}

/* Try to catpture table.
 * If these other session on the table, wait and test. 
 * */
static void BeforeCaptureTable(Oid oid) {
    try_acquire_table(oid);
    /* Wait until capture the table exclusively. */
    while (if_shared_table(oid)) {
        usleep(100);
    }
}

/* Release Table. */
static void AfterReleaseTable(Oid oid) {
    RemoveTableCache(oid);
    try_release_table(oid);
}

/* Execute create table statement. */
void ExecuteCreateTableStatement(CreateTableNode *create_table_node, DBResult *result) {
    Oid toid = FindNextOid();
    Oid soid = FindNextOid();
    Oid roid = FindNextOid();
    Oid hoid = FindNextOid();
    Oid stoid = FindNextOid();
    Oid aoid = FindNextOid();

    Assert(toid != hoid && hoid != stoid && toid != stoid);

    /* Check valid. */
    if (!CheckForCreateTable(create_table_node)) return;

    /* Combine MetaTable. */
    MetaTable *meta_table = CreateTableNodeGenerateMetaTable(toid, stoid, aoid, create_table_node);
    if (meta_table == NULL) return;

    /* Will do these: 
     * (1) Create table.
     * (2) Save table object.
     * (3) Create sid table. 
     * (4) Create rid table. 
     * (5) Create heap table.
     * (6) Create string heap table.
     * (7) Create array heap table.
     * (8) Create array heap table.
     * (9) Prepare save table cache.
     * Note: we will create its string heap table and array heap table ahead,
     * although the table maybe not have any string column or array column, just in case.
     * */
    if (
        create_table(toid, meta_table) && 
        SaveTableObject(toid, GET_METATABLE_NAME(meta_table)) &&
        CreateSidTable(soid, toid, GET_METATABLE_NAME(meta_table)) &&
        CreateRidTable(roid, toid, GET_METATABLE_NAME(meta_table)) &&
        CreateHeapTable(hoid, toid, meta_table->table_name) &&
        CreateStrHeapTable(stoid, toid, meta_table->table_name) &&
        CreateArrayHeapTable(aoid, toid, meta_table->table_name) &&
        PrepareSaveTableCache(toid, soid, roid, hoid, stoid, meta_table)
    ) {
        result->success = true;
        result->rows = 0;
        result->message = FormatStr("Table '%s' created successfully.", create_table_node->table_name);
        logger(SUCCESS, "Table '%s' created successfully.", create_table_node->table_name);
    }

    free_meta_table(meta_table);
}


/* Execute create table statement. */
void ExecuteCreateIndexStatement(CreateIndexNode *create_index_node, DBResult *result) {
    Oid oid, toid;
    MetaIndex *meta_index;
    Table *table;

    /* Check valid. */
    if (!CheckForCreateIndex(create_index_node)) return;

    /* Fetch next oid. */
    oid = FindNextOid();
    table = open_table(create_index_node->table_name);
    if (table == NULL) {
        logger(ERROR, "Table '%s' not exist.", create_index_node->table_name);
        return;
    }
    
    toid = GET_TABLE_OID(table);
    meta_index = GenerateMetaIndexForCreateIndex(oid, table, create_index_node);

    BeforeCaptureTable(toid);
    
    /* Will do these: 
     * (1) Create index table.
     * (2) Save table object.
     * */
    if (IndexCreate(meta_index) && 
        SaveIndexObject(oid, toid, create_index_node->index_name)
    ) {
        result->success = true;
        result->rows = 0;
        result->message = FormatStr("Index '%s' created successfully.", create_index_node->index_name);
        logger(SUCCESS, "Index '%s' created successfully.", create_index_node->index_name);
    }

    AfterReleaseTable(toid);
}
