#include "data.h"

/* Check if key already exists. */
bool check_duplicate_key(void *key, Refer *refer);

/* Get key type name. */
char *key_type_name(MetaColumn *meta_column);

/* Get index created xid. */
Xid IndexGetCreatedXid(void *index);

/* Set index created xid. */
void IndexSetCreatedXid(void *index, Xid created_xid);

/* Get index expired xid. */
Xid IndexGetExpiredXid(void *index);

/* Set index expired xid. */
void IndexSetExpiredXid(void *index, Xid expired_xid);
