#include "sys.h"
#include "data.h"
#include "const.h"

#ifndef SYS_TABLE_H
#define SYS_TABLE_H

#define SYS_ROOT_OID            12356
#define SYS_ROOT_HEAP_OID       12357
#define SYS_ROOT_RID_OID        12358
#define SYS_ROOT_SID_OID        12359
#define SYS_TABLE_NAME          "sys_table"
#define SYS_TABLE_OID_NAME      "oid"
#define SYS_TABLE_TOID_NAME     "toid"
#define SYS_TABLE_RELNAME_NAME  "relname"
#define SYS_TABLE_RELTYPE_NAME  "reltype"

/* System table meta column list. */
extern MetaColumn SYS_TABLE_COLUMNS[];
/* System table column size. */
#define SYS_TABLE_COLUMNS_LENGTH 4

/* System reserved columns. */
extern MetaColumn SYS_RESERVED_COLUMNS[];
/* System reserved columns length. */
#define SYS_RESERVED_COLUMNS_LENGTH 4

#define IS_SYS_ROOT(oid) (oid == SYS_ROOT_OID)
#define IS_SYS_ROOT_SID(oid) (oid == SYS_ROOT_SID_OID)
#define IS_SYS_ROOT_RID(oid) (oid == SYS_ROOT_RID_OID)
#define IS_SYS_ROOT_HEAP(oid) (oid == SYS_ROOT_HEAP_OID)

void InitSysTable();
Oid FindNextOid();
Oid TableNameFindOid(char *tableName);
Oid IndexNameFindOid(char *indexName);
Oid StrTableNameFindOid(char *tableName);
Oid ArrayTableNameFindOid(char *tableName);
Oid TableNameFindHeapOid(char *tableName);
Oid ToidFindStoid(Oid toid);
Oid ToidFindAoid(Oid toid);
Oid ToidFindRoid(Oid toid);
Oid ToidFindSoid(Oid toid);
Oid ToidFindHoid(Oid toid);
Oid TableNameFindHeapOid(char *tableName);
List *ToidFindIndexs(Oid toid);
List *FindAllObject();
char *OidFindRelName(Oid oid);
Object OidFindObject(Oid oid);
Object GenerateObjectInner(Oid oid, Oid toid, char *relname, ObjectType reltype);
Object GenerateObject(Oid toid, char *relname, ObjectType reltype);
bool SaveObject(Object entity);
bool RemoveObject(Oid oid);

#endif
