#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define _XOPEN_SOURCE
#define __USE_XOPEN
#include <time.h>
#include "meta.h"
#include "data.h"
#include "table.h"
#include "mmgr.h"
#include "copy.h"
#include "free.h"
#include "log.h"
#include "insert.h"
#include "utils.h"
#include "common.h"
#include "const.h"
#include "ltree.h"
#include "list.h"
#include "instance.h"
#include "asserts.h"
#include "pager.h"
#include "check.h"
#include "tablelock.h"
#include "systable.h"
#include "select.h"
#include "strheaptable.h"

#define DEFAULT_BOOL_LENGTH         2
#define DEFAULT_STRING_LENGTH       48
#define DEFAULT_DATE_LENGTH         20
#define DEFAULT_TIMESTAMP_LENGTH    20
#define DEFAULT_REFERENCE_LENGTH    48

/* Column type length */
uint32_t DataTypeDefaultLength(DataType column_type) {
    switch (column_type) {
        case T_BOOL:
            return DEFAULT_BOOL_LENGTH;
        case T_CHAR:
            return sizeof(char);
        case T_VARCHAR:
            return DEFAULT_STRING_LENGTH;
        case T_INT:
            return sizeof(int32_t);
        case T_LONG:
            return sizeof(int64_t);
        case T_DOUBLE:
            return sizeof(double);
        case T_FLOAT:
            return sizeof(float);
        /* For String type, use StrRefer to store the refer info. */
        case T_STRING:
            return sizeof(StrRefer);
        case T_DATE:
            return DEFAULT_DATE_LENGTH;
        case T_TIMESTAMP:
            return DEFAULT_TIMESTAMP_LENGTH;
        case T_REFERENCE:
            return DEFAULT_REFERENCE_LENGTH;
        default:
            UNEXPECTED_VALUE("Unknown column type");
            return -1;
  }
}

/* Convert AtomType to DataType. */
DataType AtomTypeConvertDataType(AtomType atom_type) {
    switch (atom_type) {
        case A_INT:
            return T_LONG;
        case A_BOOL:
            return T_BOOL;
        case A_FLOAT: 
            return T_DOUBLE;
        case A_STRING:
            return T_VARCHAR;
        case A_REFERENCE:
            return T_REFERENCE;
        default:
            UNEXPECTED_VALUE(atom_type);
    }
}

/* Assign value from atom*/
static void *AtomNodeAssignValue(AtomNode *atom_node, MetaColumn *meta_column) {
    /* Assign new value. */
    switch(meta_column->column_type) {
        case T_BOOL: 
            return copy_value2(&atom_node->value.boolval, meta_column);
        case T_INT: {
            int32_t val = (int32_t)atom_node->value.intval;
            return copy_value2(&val, meta_column);
        }
        case T_LONG: {
            int64_t val = (int64_t)atom_node->value.intval;
            return copy_value2(&val, meta_column);
        }
        case T_FLOAT: {
            float val = 0;
            switch (atom_node->type) {
                case A_INT:
                    val = (float)atom_node->value.intval;
                    break;
                case A_FLOAT:
                    val = (float)atom_node->value.floatval;
                    break;
                default:
                    db_log(ERROR, "Can`t convert to data type [%s] for column '%s'", 
                           AtomTypeConvertDataType(atom_node->type), 
                           meta_column->column_name);
                    break;
            }
            return copy_value2(&val, meta_column);
        }
        case T_DOUBLE: {
            double val = 0;
            switch (atom_node->type) {
                case A_INT:
                    val = (double)atom_node->value.intval;
                    break;
                case A_FLOAT:
                    val = (double)atom_node->value.floatval;
                    break;
                default:
                    db_log(ERROR, "Can`t convert to data type [%s] for column '%s'", 
                           AtomTypeConvertDataType(atom_node->type), 
                           meta_column->column_name);
                    break;
            }
            return copy_value2(&val, meta_column);
        }
        case T_CHAR:
        case T_VARCHAR: 
            return copy_value2(atom_node->value.strval, meta_column);
        case T_DATE: {
            struct tm tmp_time;
            memset(&tmp_time, 0, sizeof(struct tm));
            strptime(atom_node->value.strval, "%Y-%m-%d", &tmp_time);
            tmp_time.tm_sec = 0;
            tmp_time.tm_min = 0;
            tmp_time.tm_hour = 0;
            time_t tmp = mktime(&tmp_time);
            return copy_value2(&tmp, meta_column);
        }
        case T_TIMESTAMP: {
            struct tm tmp_time;
            memset(&tmp_time, 0, sizeof(struct tm));
            strptime(atom_node->value.strval, "%Y-%m-%d %H:%M:%S", &tmp_time);
            time_t tmp = mktime(&tmp_time);
            return copy_value2(&tmp, meta_column);
        }
        /* Note: for STRING type, it will insert into the target strheaptable
         * and return the strRefer. */
        case T_STRING: {
            Oid oid = StrTableNameFindOid(meta_column->table_name);
            AssertFalse(ZERO_OID(oid));
            return InsertStringValue(oid, atom_node->value.strval);
        } 
        /* Note: for REFERENCE type, it will insert into the referd target table 
         * and return the refer. */
        case T_REFERENCE: {
            ReferValue *refer_value = atom_node->value.referval;
            switch (refer_value->type) {
                case DIRECTLY: {
                    InsertNode *insert_node = GenerateInsertNode(meta_column->table_name, refer_value->nest_value_list);
                    List *refer_list = insert_for_values(insert_node);
                    AssertFalse(list_empty(refer_list));
                    free_insert_node(insert_node);
                    return lfirst(first_cell(refer_list));
                }
                case INDIRECTLY: {
                    return fetch_refer(meta_column, refer_value->condition);
                }
                default:
                    UNEXPECTED_VALUE(refer_value->type);
                    return NULL;
            }
            break;
        }
        default:
            UNEXPECTED_VALUE(meta_column->column_type);
            return NULL;
    }    
}

/* Assign value from array. */
static void *ValueListAssignValue(List *value_list, MetaColumn *meta_column) {
    ArrayValue *array_value = instance(ArrayValue);
    array_value->type = meta_column->column_type;
    array_value->list = create_list(NODE_VOID);

    ListCell *lc;
    foreach (lc, value_list) {
        ValueItemNode *value_item = lfirst(lc);
        append_list(
            array_value->list, 
            ValueItemNodeAssignValue(value_item, meta_column)
        );
    }

    return array_value;
}

/* Assign value from ValueItemNode. 
 * --------------------------------
 * Notice: the difference between <ValueItemNodeAssignValue> and <ValueItemNodeFindValue> is that 
 * <ValueItemNodeAssignValue> works for DML operation, like update, insert as value. 
 * <ValueItemNodeFindValue> works for DQL operation, like select as search condition value.
 * */
void *ValueItemNodeAssignValue(ValueItemNode *value_item_node, MetaColumn *meta_column) {
    switch (value_item_node->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item_node->value.atom;
            return AtomNodeAssignValue(atom_node, meta_column);
        }
        case V_ARRAY: {
            List *value_list = value_item_node->value.value_list;
            return ValueListAssignValue(value_list, meta_column);
        }
        case V_NULL:
            return NULL;
        default:
            UNEXPECTED_VALUE(value_item_node->type);
    }
}

/* Get value from atom. */
static void *AtomNodeFindValue(AtomNode *atom_node) {
    switch (atom_node->type) {
        case A_INT: 
            return &atom_node->value.intval;
        case A_BOOL:
            return &atom_node->value.boolval;
        case A_FLOAT:
            return &atom_node->value.floatval;
        case A_STRING:
            return atom_node->value.strval;
        case A_REFERENCE:
            return atom_node->value.referval;
        default:
            UNEXPECTED_VALUE(atom_node->type);
            return NULL;
    }
    return NULL;
}

/* Get value from value item node. 
 * -------------------------------
 * Return pointer that needs be free`d by caller. 
 * And return null for V_NULL type ValueItemNode.
 * */
void *ValueItemNodeFindValue(ValueItemNode *value_item_node) {
    /* For array, return value set. */
    switch (value_item_node->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item_node->value.atom;
            return AtomNodeFindValue(atom_node);
        }
        case V_ARRAY:
            return value_item_node->value.value_list;
        case V_NULL:
            return NULL;
        default:
            UNEXPECTED_VALUE(value_item_node->type);
    }
}

/* Get Comparable value. 
 * ----------------------
 * This function will convert value to comparable value.
 * By now, it only works for T_STRING type value.
 * For T_STRING value, we will compare the string value rather than the StrRefer value.
 * But for T_REFERENCE, we will still compare its refer value. 
 * */
void *GetComparableValue(void *value, DataType type) {
    if (value == NULL)
        return NULL;
    switch (type) {
        /* For STRING, convert to real string value instead of refer value. */
        case T_STRING:
            return QueryStringValue((StrRefer *) value);
        default:
            return value;
    }
} 

/* Get key string value for user.*/
char *KeyGetUserStrValue(void *key, DataType ptype) {
    switch(ptype) {
        case T_BOOL: 
            return *(bool *)key ? "true" : "false";
        case T_CHAR:
        case T_VARCHAR:
        case T_STRING:
            return (char *)key;
        case T_INT: {
            char *str = dalloc(50);
            sprintf(str, "%d", *(int32_t *)key);
            return str;
        }
        case T_LONG: {
            char *str = dalloc(100);
            sprintf(str, "%ld", *(int64_t *)key);
            return str;
        }
        case T_DOUBLE: {
            char *str = dalloc(50);
            sprintf(str, "%lf", *(double *)key);
            return str;
        }
        case T_FLOAT: {
            char *str = dalloc(50);
            sprintf(str, "%f", *(float *)key);
            return str;
        }
        case T_DATE: {
            char *str = dalloc(30);
            struct tm *tmp_time = localtime(key);
            strftime(str, strlen(str), "%Y-%m-%d", tmp_time);
            return str;
        }
        case T_TIMESTAMP: {
            char *str = dalloc(40);
            struct tm *tmp_time = localtime(key);
            strftime(str, strlen(str), "%Y-%m-%d %H:%M:%S", tmp_time);
            return str;
        } default: {
            db_log(ERROR, "Not allowed data type as primary key.");
            return NULL;
        }
    }
}

/* Get key string value for system.*/
char *KeyGetSysStrValue(void *key, DataType ptype) {
    switch(ptype) {
        case T_BOOL: 
            return *(bool *)key ? "true" : "false";
        case T_CHAR:
        case T_VARCHAR:
            return (char *)key;
        case T_STRING:
            return QueryStringValue((StrRefer *)key);
        case T_INT: {
            char *str = dalloc(50);
            sprintf(str, "%d", *(int32_t *)key);
            return str;
        }
        case T_LONG: {
            char *str = dalloc(100);
            sprintf(str, "%ld", *(int64_t *)key);
            return str;
        }
        case T_DOUBLE: {
            char *str = dalloc(50);
            sprintf(str, "%lf", *(double *)key);
            return str;
        }
        case T_FLOAT: {
            char *str = dalloc(50);
            sprintf(str, "%f", *(float *)key);
            return str;
        }
        case T_DATE: {
            char *str = dalloc(30);
            struct tm *tmp_time = localtime(key);
            strftime(str, strlen(str), "%Y-%m-%d", tmp_time);
            return str;
        }
        case T_TIMESTAMP: {
            char *str = dalloc(40);
            struct tm *tmp_time = localtime(key);
            strftime(str, strlen(str), "%Y-%m-%d %H:%M:%S", tmp_time);
            return str;
        } default: {
            db_log(ERROR, "Not allowed data type as primary key.");
            return NULL;
        }
    }
}

/* Calculate the length of table row. */
uint32_t TableCalcRowLength(Table *table) {
    uint32_t row_len = 0;
    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        row_len += meta_column->column_length;
    }
    return row_len;
}

/* Calculate the length of table row. */
uint32_t MetaTableCalcRowLenght(MetaTable *meta_table) {
    uint32_t row_len = 0;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        row_len += meta_column->column_length;
    }
    return row_len;
}

/* Calculate primary key lenght. 
 * Return primary-key column length.
 * Panic if not found. */
uint32_t TableCalcPrimaryKeyLength(Table *table) {
    ListCell *lc;
    foreach(lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->is_primary)
            return meta_column->column_length;
    }
    panic("Not found primary key.");
    return -1;
}


/* Calculate primary index value length. */
uint32_t TableCalcIndexLength(Table *table) {
    uint32_t value_len;
    value_len = REFER_SIZE;
    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->sys_reserved)
            value_len += meta_column->column_length;
    }
    return value_len;
}

/* Get column meta info by index. */
static MetaColumn *GetMetaColumnByIndex(void *root_node, uint32_t index, uint32_t offset) {
    void *destination = get_meta_column_pointer(root_node, index);
    MetaColumn *meta_column = deserialize_meta_column(destination);
    if (meta_column->default_value_type == DEFAULT_VALUE) {
        void *default_value_dest = get_default_value_cell(root_node);
        meta_column->default_value = copy_value(default_value_dest + offset, meta_column->column_type);
    }
    return meta_column;
}

/* Get key type name. */
char *GetKeyTypeName(MetaColumn *meta_column) {
    if (meta_column->is_primary) 
        return "primary";
    else if (meta_column->is_unique)
        return "unique";
    else
        return NULL;
}


/* Find MetaColumn by column name. */
MetaColumn *NameFindMetaColumnInner(List *meta_columns, char *column_name) {
    Assert(meta_columns != NIL);
    ListCell *lc;
    foreach (lc, meta_columns) {
        MetaColumn *current = (MetaColumn *) lfirst(lc);
        if (StrEq(current->column_name, column_name))
            return current;
    }
    return NULL;
}

/* Find MetaColumn by table name and column name. */
MetaColumn *TableColumnNameFindMetaColumn(List *meta_columns, char *table_name, char *column_name) {
    Assert(meta_columns != NIL);
    ListCell *lc;
    foreach (lc, meta_columns) {
        MetaColumn *current = (MetaColumn *) lfirst(lc);
        if (StrEq(current->own_table_name, table_name) && StrEq(current->column_name, column_name))
            return current;
    }
    return NULL;
}

/* Get meta column info by column name. */
MetaColumn *NameFindMetaColumn(MetaTable *meta_table, char *column_name) {
    return NameFindMetaColumnInner(meta_table->meta_columns, column_name);
}

/* Get meta columnn postion by column name.
 * Return -1 if missing. 
 * */
int NameFindMetaColumnPostion(MetaTable *meta_table, char *column_name) {
    uint32_t i = 0;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (StrEq(meta_column->column_name, column_name))
            return i;
        i++;
    }
    return -1;
}

/* Get meta column of primary key. */
MetaColumn *MetaTableFindPrimaryKey(MetaTable *meta_table) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->is_primary)
            return meta_column;
    }
    return NULL; 
}

/* Get meta column data type. */
DataType MetaTableFindPrimaryDataType(MetaTable *meta_table) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->is_primary)
            return meta_column->column_type;
    }
    return T_UNKNOWN; 
}

/* Get all meta column info by column name including system reserved column. 
 * Return NULL if not found. */
MetaColumn *NameFindAllMetaColumn(MetaTable *meta_table, char *name) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (StrEq(meta_column->column_name, name))
            return meta_column;
    }
    return NULL;
}

/* Generate table meta info. */
MetaTable *GenerateMetaTable(Oid oid) {
    MetaTable *meta_table = instance(MetaTable);
    Buffer buffer = ReadBuffer(oid, ROOT_PAGE_NUM);
    void *root_node = GetBufferPage(buffer);
    uint32_t column_size = get_column_size(root_node);

    meta_table->table_name = IS_SYS_ROOT(oid) ? dstrdup(SYS_TABLE_NAME) : OidFindRelName(oid);
    meta_table->column_size = 0;
    meta_table->all_column_size = 0;
    meta_table->meta_columns = create_list(NODE_META_COLUMN);

    uint32_t offset = 0;
    uint32_t i;
    for (i = 0; i < column_size; i++) {
        MetaColumn *current = GetMetaColumnByIndex(root_node, i, offset);
        memcpy(current->own_table_name, meta_table->table_name, MAX_COLUMN_NAME_LEN);
        append_list(meta_table->meta_columns, current);
        /* Skip to system reserved column. */
        if (!current->sys_reserved)
            meta_table->column_size++;
        meta_table->all_column_size++;
        current->offset = offset;
        offset += current->column_length;
    }

    Assert(meta_table->all_column_size == column_size);

    /* Release the buffer. */
    ReleaseBuffer(buffer);

    return meta_table;
}

/* Check if table exists the column. */
bool ColumnExistsInTable(char *column_name, char *table_name) {
    Table *table = open_table(table_name);
    MetaTable *meta_table = table->meta_table;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (StrEq(column_name, meta_column->column_name))
            return true;
    }
    return false;
}


/* Calculate User-level meta column length.
 * Notice, T_STRING data has added on extra char. */
uint32_t CalcUserMetaColumnLen(MetaColumn *meta_column) {
    switch (meta_column->column_type) {
        case T_CHAR:
        case T_STRING:
        case T_VARCHAR:
            return meta_column->column_length - 2;
        default:
            return meta_column->column_length - 1;
    }
}

/* Check if user has defined primary key.*/
bool UserPrimaryKeyExists(MetaTable *meta_table) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->is_primary)
            return true;
    }
    return false;
}

/* Seriable meta column. */
void *MetaColumnSeriable(MetaColumn *meta_column) {
    void *destination= dalloc(ROOT_NODE_META_COLUMN_SIZE);
    strcpy(destination, meta_column->column_name);
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE) = (uint32_t) meta_column->column_type;
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE) = (uint32_t) meta_column->column_length;
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE) = meta_column->is_primary;  
    if (meta_column->column_type == T_REFERENCE || meta_column->column_type == T_STRING)
        strcpy(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE, meta_column->table_name);
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE) = meta_column->sys_reserved;  
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE) = meta_column->is_unique;  
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE) = meta_column->not_null;  
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE) = meta_column->array_dim;  
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE) = meta_column->array_cap;
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE) = meta_column->default_value_type;
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE) = meta_column->has_comment;  
    memcpy(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE + ROOT_NODE_HAS_COMMENT_SIZE, 
           meta_column->comment, 
           ROOT_NODE_COMMENT_STRING_SIZE);
    return destination;
}

/* Deseriable meta column. */
MetaColumn *MetaColumnDeseriable(void *destination) {
    MetaColumn *meta_column = instance(MetaColumn);
    strcpy(meta_column->column_name, destination); 
    meta_column->column_type = (DataType)*(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE);
    meta_column->column_length = *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE);
    meta_column->is_primary = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE);
    if (meta_column->column_type == T_REFERENCE || meta_column->column_type == T_STRING)
        strcpy(meta_column->table_name, destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE);
    meta_column->sys_reserved = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE);
    meta_column->is_unique = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE);
    meta_column->not_null = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE);
    meta_column->array_dim = *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE);
    meta_column->array_cap = *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE);
    meta_column->default_value_type = (DefaultValueType)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE);
    meta_column->has_comment = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE);
    strcpy(meta_column->comment, destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE + ROOT_NODE_HAS_COMMENT_SIZE);
    return meta_column;
}

static void MetaColumnAssginArrayNumToDestination(void *destination, uint32_t array_num) {
    memcpy(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, &array_num, LEAF_NODE_ARRAY_NUM_SIZE);
}

/* Assign array value to destination. */
static void MetaColumnAssginArrayValueToDestination(void *destination, ArrayValue *array_value, MetaColumn *meta_column) {
    uint32_t len, span;

    len =len_list(array_value->list);
    /* User insert arrary values number integer multiple of array dim. */
    Assert(len % meta_column->array_dim == 0);

    /* Assign array number. */
    MetaColumnAssginArrayNumToDestination(destination, len);
    /* span: every value in array data lenght. */
    span = (meta_column->column_length - LEAF_NODE_ARRAY_NUM_SIZE - LEAF_NODE_CELL_NULL_FLAG_SIZE) / meta_column->array_cap;

    ListCell *lc;
    foreach (lc, array_value->list) {
        void *value = lfirst(lc);
        memcpy((destination + LEAF_NODE_CELL_NULL_FLAG_SIZE + LEAF_NODE_ARRAY_NUM_SIZE + span * __i), value, span);
    }
}

/* Assign value to destination. */
void MetaColumnAssignValueToDestination(void *destination, void *value, MetaColumn *meta_column) {
    /* If not array value, assign value to destination, 
     * if array value, assign values one by one. */
    if (meta_column->array_dim == 0) {
        bool nflag = value == NULL ? true : false;
        memcpy(destination, &nflag, LEAF_NODE_CELL_NULL_FLAG_SIZE);
        if (!nflag)
            memcpy(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, value, meta_column->column_length - LEAF_NODE_CELL_NULL_FLAG_SIZE);
        else
            memset(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, 0, meta_column->column_length - LEAF_NODE_CELL_NULL_FLAG_SIZE);
    } else {
        bool nflag = value == NULL ? true : false;
        memcpy(destination, &nflag, LEAF_NODE_CELL_NULL_FLAG_SIZE);
        if (!nflag)
            MetaColumnAssginArrayValueToDestination(destination, value, meta_column);
        else
            memset(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, 0, meta_column->column_length - LEAF_NODE_CELL_NULL_FLAG_SIZE);
    } 
}
