#include "sys.h"
#include "data.h"
#include "const.h"

#ifndef SYS_TABLE_H
#define SYS_TABLE_H

#define SYS_ROOT_OID 1235

#define SYS_TABLE_NAME "sys_table"
#define SYS_TABLE_OID_NAME "oid"
#define SYS_TABLE_RELNAME_NAME "rel_name"
#define SYS_TABLE_RELTYPE_NAME "rel_type"

/* System table meta column list. */
static MetaColumn SYS_TABLE_COLUMNS[] = {
    { SYS_TABLE_OID_NAME, T_INT, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int32_t)), true, false, false, false, 0, 0},
    { SYS_TABLE_RELNAME_NAME, T_VARCHAR, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + MAX_COLUMN_SIZE), false, false, false, false, 0, 0},
    { SYS_TABLE_RELTYPE_NAME, T_INT, SYS_TABLE_NAME, (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int32_t)), false, false, false, false, 0, 0}
};
/* System table column size. */
#define SYS_TABLE_COLUMNS_LENGTH  sizeof(SYS_TABLE_COLUMNS) / sizeof(SYS_TABLE_COLUMNS[0])

/* System reserved columns. */
static MetaColumn SYS_RESERVED_COLUMNS[] = {
    { SYS_RESERVED_ID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 },
    { CREATED_XID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 },
    { EXPIRED_XID_COLUMN_NAME, T_LONG, "", (LEAF_NODE_CELL_NULL_FLAG_SIZE + sizeof(int64_t)), false, false, false, true, 0, 0 }
}; 
/* System reserved columns length. */
#define SYS_RESERVED_COLUMNS_LENGTH sizeof(SYS_RESERVED_COLUMNS) / sizeof(SYS_RESERVED_COLUMNS[0])

/* Create the sys table. */
void CreateSysTable();

/* RefIdFindObject. */
ObjectEntity RefIdFindObject(Oid refId);

#endif
