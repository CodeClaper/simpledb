#include "data.h"
#include "refer.h"

void *TupleFindValue(void *tuple, MetaColumn *meta_column);
void *TupleFindKey(void *tuple, Table *table);
Xid TupleFindCreatedXid(void *tuple, MetaTable *meta_table);
void TupleSetValue(void *tuple, MetaColumn *meta_column, void *value);
void TupleSetCreatedXid(void *tuple, MetaTable *meta_table, Xid created_xid);
Xid TupleFindExpiredXid(void *tuple, MetaTable *meta_table);
void TupleSetExpiredXid(void *tuple, MetaTable *meta_table, Xid expired_xid);
int64_t TupleGetSysId(void *tuple, MetaTable *meta_table);
void TupleSetSysId(void *tuple, MetaTable *meta_table, int64_t sys_id);
Rid TupleGetRefId(void *tuple, MetaTable *meta_table);
void TupleSetRefId(void *tuple, MetaTable *meta_table, int64_t sys_id);
void *FetchTupleViaRid(Oid toid, Rid ref_id);
