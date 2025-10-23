#include "data.h"

/* Modify the btree. */
Refer *BtreeModify(Oid oid, void *key, void *value);

/* Modify the btree expired_xid. */
Refer *BtreeModifyExpiredXid(Oid oid, void *key, Xid expired_xid);
