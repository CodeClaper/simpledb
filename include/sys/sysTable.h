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
extern MetaColumn SYS_TABLE_COLUMNS[];
/* System table column size. */
#define SYS_TABLE_COLUMNS_LENGTH 3

/* System reserved columns. */
extern MetaColumn SYS_RESERVED_COLUMNS[];
/* System reserved columns length. */
#define SYS_RESERVED_COLUMNS_LENGTH 3

/* Create the sys table. */
void CreateSysTable();

/* RefIdFindObject. */
ObjectEntity RefIdFindObject(Oid refId);

#endif
