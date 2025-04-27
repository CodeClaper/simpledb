#include "sys.h"
#include "data.h"
#include "const.h"

#ifndef SYS_TABLE_H
#define SYS_TABLE_H

#define SYS_ROOT_OID 1235
#define SYS_TABLE_NAME "sys_table"
#define SYS_TABLE_OID_NAME "oid"
#define SYS_TABLE_RELNAME_NAME "relname"
#define SYS_TABLE_RELTYPE_NAME "reltype"

/* System table meta column list. */
extern MetaColumn SYS_TABLE_COLUMNS[];
/* System table column size. */
#define SYS_TABLE_COLUMNS_LENGTH 3

/* System reserved columns. */
extern MetaColumn SYS_RESERVED_COLUMNS[];
/* System reserved columns length. */
#define SYS_RESERVED_COLUMNS_LENGTH 3

#define IS_SYS_ROOT(oid) (oid == SYS_ROOT_OID)

/* Init the sys table. */
void InitSysTable();

/* Find next Oid. */
Oid FindNextOid();

/* RefIdFindObject. */
Object OidFindObject(Oid oid);

/* Find oid by table name. */
Oid TableNameFindOid(char *tableName);

/* Find oid of string table by table name. */
Oid StrTableNameFindOid(char *tableName);

/* Find relname by oid. */
char *OidFindRelName(Oid oid);

/* Find all object list. */
List *FindAllObject();

/* Geneate Object entity. */
Object GenerateObjectInner(Oid oid, char *relname, ObjectType reltype);

/* Generate Object entity. */
Object GenerateObject(char *relname, ObjectType reltype);

/* Save Object. */
bool SaveObject(Object entity);

/* Remove the object. */
bool RemoveObject(Oid oid);

#endif
