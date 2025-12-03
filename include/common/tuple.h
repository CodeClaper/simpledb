#include "data.h"
#include "refer.h"

/* Get value in tuple. */
void *TupleFindValue(void *tuple, MetaColumn *meta_column);

/* Set value in tuple.*/
void TupleSetValue(void *tuple, MetaColumn *meta_column, void *value);

/* Get primary key value in tuple. */
void *TupleFindKey(void *tuple, Table *table);

/* Get created xid in tuple. */
Xid TupleFindCreatedXid(void *tuple, MetaTable *meta_table);

/* Set created xid in tuple. */
void TupleSetCreatedXid(void *tuple, MetaTable *meta_table, Xid created_xid);

/* Get created xid in tuple. */
Xid TupleFindExpiredXid(void *tuple, MetaTable *meta_table);

/* Set expired xid in tuple. */
void TupleSetExpiredXid(void *tuple, MetaTable *meta_table, Xid expired_xid);

/* Get sys id in tuple. */
int64_t TupleGetSysId(void *tuple, MetaTable *meta_table);

/* Set sys id in tuple. */
void TupleSetSysId(void *tuple, MetaTable *meta_table, int64_t sys_id);

/* Get ref id in tuple. */
Rid TupleGetRefId(void *tuple, MetaTable *meta_table);

/* Set ref id in tuple. */
void TupleSetRefId(void *tuple, MetaTable *meta_table, int64_t sys_id);

/* Fetch tuple via rid. */
void *FetchTupleViaRid(Oid toid, Rid ref_id);
