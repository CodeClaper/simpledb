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

/* DataTypeNames */
static char *DATA_TYPE_NAMES[] =  {
    "unknown", 
    "bool",  
    "char", 
    "varchar", 
    "int", 
    "long", 
    "double", 
    "float", 
    "string", 
    "date", 
    "timestamp",  
    "reference", 
    "array" 
};

/* Function type names*/
static char *FUNCTION_TYPE_NAMES[] = { "COUNT", "MAX", "MIN", "SUM", "AVG" };

/* Column type length */
uint32_t default_data_len(DataType column_type) {
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
DataType convert_data_type(AtomType atom_type) {
    switch (atom_type) {
        case A_INT:
            return T_INT;
        case A_BOOL:
            return T_BOOL;
        case A_FLOAT:
            return T_DOUBLE;
        case A_STRING:
            return T_STRING;
        case A_REFERENCE:
            return T_REFERENCE;
        default:
            UNEXPECTED_VALUE(atom_type);
    }
}

/* Data type name. */
inline char *data_type_name(DataType data_type) {
    return DATA_TYPE_NAMES[data_type];
}

/* Function type name. */
inline char *function_type_name(FunctionType function_type) {
    return FUNCTION_TYPE_NAMES[function_type];
}

/* Assign value from atom*/
static void *assign_value_from_atom(AtomNode *atom_node, MetaColumn *meta_column) {
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
                           convert_data_type(atom_node->type), 
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
                           convert_data_type(atom_node->type), 
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

/* Check if system built-in primary key.*/
bool built_in_primary_key(MetaTable *meta_table) {
    MetaColumn *primary_meta_column = get_primary_key_meta_column(meta_table);
    return streq(primary_meta_column->column_name, SYS_RESERVED_ID_COLUMN_NAME);
}

/* Assign value from array. */
void *assign_value_from_array(List *value_list, MetaColumn *meta_column) {
    ArrayValue *array_value = instance(ArrayValue);
    array_value->type = meta_column->column_type;
    array_value->list = create_list(NODE_VOID);

    ListCell *lc;
    foreach (lc, value_list) {
        ValueItemNode *value_item = lfirst(lc);
        append_list(
            array_value->list, 
            assign_value_from_value_item_node(value_item, meta_column)
        );
    }

    return array_value;
}

/* Assign value from ValueItemNode. */
void *assign_value_from_value_item_node(ValueItemNode *value_item_node, MetaColumn *meta_column) {
    switch (value_item_node->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item_node->value.atom;
            return assign_value_from_atom(atom_node, meta_column);
        }
        case V_ARRAY: {
            List *value_list = value_item_node->value.value_list;
            return assign_value_from_array(value_list, meta_column);
        }
        case V_NULL:
            return NULL;
        default:
            UNEXPECTED_VALUE(value_item_node->type);
    }
}

/* Get value from atom. */
static void *get_value_from_atom(AtomNode *atom_node, MetaColumn *meta_column) {
    /* User can use '%s' fromat in sql to pass multiple types value 
     * including char, string, date, timestamp. So we must use meta 
     * column data type to define which data type of the value. */
    switch (meta_column->column_type) {
        case T_BOOL: 
            return copy_value(&atom_node->value.boolval, meta_column->column_type);
        case T_INT: {
            int32_t val = (int32_t)atom_node->value.intval;
            return copy_value(&val, meta_column->column_type);
        }
        case T_LONG: {
            int64_t val = (int64_t)atom_node->value.intval;
            return copy_value(&val, meta_column->column_type);
        }
        case T_FLOAT: {
            float val = 0;
            switch (atom_node->type) {
                case A_INT:
                    val = (double)atom_node->value.intval;
                    break;
                case A_FLOAT:
                    val = (double)atom_node->value.floatval;
                    break;
                default:
                    db_log(ERROR, "Can`t convert to data type [%s] for column '%s'", 
                           convert_data_type(atom_node->type), 
                           meta_column->column_name);
                    break;
            }
            return copy_value(&val, meta_column->column_type);
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
                           convert_data_type(atom_node->type), 
                           meta_column->column_name);
                    break;
            }
            return copy_value(&val, meta_column->column_type);
        }
        case T_CHAR:
        case T_STRING: 
        case T_VARCHAR: 
            return copy_value(atom_node->value.strval, meta_column->column_type);
        case T_DATE: {
            struct tm tmp_time = {0};
            time_t *time = instance(time_t);  
            strptime(atom_node->value.strval, "%Y-%m-%d", &tmp_time);
            tmp_time.tm_sec = 0;
            tmp_time.tm_min = 0;
            tmp_time.tm_hour = 0;
            time_t tmp = mktime(&tmp_time);
            memcpy(time, &tmp, sizeof(time_t));
            return time;
        }
        case T_TIMESTAMP: {
            struct tm tmp_time = {0};
            time_t *time = instance(time_t);  
            strptime(atom_node->value.strval, "%Y-%m-%d %H:%M:%S", &tmp_time);
            time_t tmp = mktime(&tmp_time);
            memcpy(time, &tmp, sizeof(time_t));
            return time;
        }
        case T_REFERENCE: {
            ReferValue *refer_value = atom_node->value.referval;
            switch (refer_value->type) {
                case DIRECTLY:
                    db_log(WARN, "Not support directly fetch refer when query.");
                    return make_null_refer();
                case INDIRECTLY: 
                    return fetch_refer(meta_column, refer_value->condition);
            }
            break;
        }
        default:
            db_log(PANIC, "Not implement yet.");
    }
    return NULL;
}

/* Get value from value item node. 
 * -------------------------------
 * Return pointer that needs be free`d by caller. 
 * And return null for V_NULL type ValueItemNode.
 * */
void *get_value_from_value_item_node(ValueItemNode *value_item_node, MetaColumn *meta_column) {
    /* For array, return value set. */
    switch (value_item_node->type) {
        case V_ATOM: {
            AtomNode *atom_node = value_item_node->value.atom;
            /* Check default value valid. */
            check_value_valid(meta_column, atom_node);
            return get_value_from_atom(atom_node, meta_column);
        }
        case V_ARRAY:
            return list_copy_deep(value_item_node->value.value_list);
        case V_NULL:
            return NULL;
        default:
            UNEXPECTED_VALUE(value_item_node->type);
    }
}

/* Get row array value. 
 * Return ArrayValue.
 * */
static ArrayValue *get_row_array_value(void *destination, MetaColumn *meta_column) {
    uint32_t array_num = get_array_number(destination);

    /* Generate ArrayValue instance. */
    ArrayValue *array_value = new_array_value(meta_column->column_type, array_num);
    uint32_t span = (meta_column->column_length - LEAF_NODE_ARRAY_NUM_SIZE - LEAF_NODE_CELL_NULL_FLAG_SIZE) / meta_column->array_cap;

    uint32_t i;
    for (i = 0; i < array_num; i++) {
        void *value = get_array_value(destination, i, span);
        append_list(array_value->list, copy_value(value, meta_column->column_type));
    }
    return array_value;
}

/* Assignment row value. */ 
static void *define_row_value(void *destination, MetaColumn *meta_column) {
    return (meta_column->array_dim == 0)
            /* For non-array data. */
            ? destination + LEAF_NODE_CELL_NULL_FLAG_SIZE 
            /* For array data. */
            : get_row_array_value(destination, meta_column); 
}


/* Get Really value. */
void *get_real_value(void *value, DataType type) {
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

/* Get value in tuple. */
void *get_value_in_tuple(void *tuple, MetaColumn *meta_column) {
    bool nflag =  *(bool *)(tuple + meta_column->offset);
    return nflag ? NULL : define_row_value((tuple + meta_column->offset), meta_column);
}

/* Combine AtomNode by column and value. */
AtomNode *combine_atom_node(MetaColumn *meta_column, void *value) {
    AtomNode *atom_node = instance(AtomNode);
    switch (meta_column->column_type) {
        case T_BOOL: {
            atom_node->type = A_BOOL;
            atom_node->value.boolval =  *(bool *)value;  
            break;
        }
        case T_CHAR: 
        case T_STRING:
        case T_DATE:
        case T_TIMESTAMP:
        case T_VARCHAR: {
            atom_node->type = A_STRING;
            atom_node->value.strval = value;  
            break;
        }
        case T_INT: 
        case T_LONG: {
            atom_node->type = A_INT;
            atom_node->value.intval = *(int64_t *) value;  
            break;
        }
        case T_DOUBLE:
        case T_FLOAT: {
            atom_node->type = A_INT;
            atom_node->value.floatval = *(double *) value;  
            break;
        }
        case T_REFERENCE:
        case T_ROW:
        case T_UNKNOWN:
            panic("Cant convert type to AtomNode.");
        break;
    }   
    return atom_node;
}


/* Calculate the length of table row. */
uint32_t calc_table_row_length(Table *table) {
    uint32_t row_len = 0;
    ListCell *lc;
    foreach (lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        row_len += meta_column->column_length;
    }
    return row_len;
}

/* Calculate the length of table row. */
uint32_t calc_table_row_length2(MetaTable *meta_table) {
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
uint32_t calc_primary_key_length(Table *table) {
    ListCell *lc;
    foreach(lc, table->meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->is_primary)
            return meta_column->column_length;
    }
    panic("Not found primary key.");
    return -1;
}


/* Calculate primary key lenght. 
 * Return primary-key column length.
 * Panic if not found. */
uint32_t calc_primary_key_length2(MetaTable *meta_table) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
       if (meta_column->is_primary)
           return meta_column->column_length;
    }
    panic("Not found primary key.");
    return -1;
}

/* Calculate primary index value length. */
uint32_t calc_primary_index_value_length(Table *table) {
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


/* Calculate primary index value length. */
uint32_t calc_primary_index_value_length2(MetaTable *meta_table) {
    uint32_t value_len;
    value_len = REFER_SIZE;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
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

/* Get meta column info by column name. */
MetaColumn *get_meta_column_by_name(MetaTable *meta_table, char *column_name) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (streq(meta_column->column_name, column_name))
            return meta_column;
    }
    return NULL;
}


/* Get meta columnn postion by column name.
 * Return -1 if missing. 
 * */
int get_meta_column_pos_by_name(MetaTable *meta_table, char *column_name) {
    uint32_t i = 0;
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (streq(meta_column->column_name, column_name))
            return i;
        i++;
    }
    return -1;
}

/* Get meta column of primary key. */
MetaColumn *get_primary_key_meta_column(MetaTable *meta_table) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (meta_column->is_primary)
            return meta_column;
    }
    return NULL; 
}

/* Get meta column of primary key type. */
DataType get_primary_key_type(MetaTable *meta_table) {
    MetaColumn *primary_meta_column = get_primary_key_meta_column(meta_table);
    Assert(primary_meta_column);
    return primary_meta_column->column_type;
}


/* Get all meta column info by column name including system reserved column. 
 * Return NULL if not found. */
MetaColumn *NameFindAllMetaColumn(MetaTable *meta_table, char *name) {
    ListCell *lc;
    foreach (lc, meta_table->meta_columns) {
        MetaColumn *meta_column = (MetaColumn *)lfirst(lc);
        if (streq(meta_column->column_name, name))
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
        if (streq(column_name, meta_column->column_name))
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

/* Find MetaColumn by table name and column name. */
MetaColumn *TableColumnNameFindMetaColumn(List *meta_columns, char *table_name, char *column_name) {
    Assert(meta_columns != NIL);
    ListCell *lc;
    foreach (lc, meta_columns) {
        MetaColumn *current = (MetaColumn *) lfirst(lc);
        if (streq(current->own_table_name, table_name) && streq(current->column_name, column_name))
            return copy_meta_column(current);
    }
    return NULL;
}

/* Find MetaColumn by column name. */
MetaColumn *NameFindMetaColumn(List *meta_columns, char *column_name) {
    Assert(meta_columns != NIL);
    ListCell *lc;
    foreach (lc, meta_columns) {
        MetaColumn *current = (MetaColumn *) lfirst(lc);
        if (streq(current->column_name, column_name))
            return copy_meta_column(current);
    }
    return NULL;
}
