#include "data.h"

/* Check if key already exists. */
bool check_duplicate_key(Cursor *cursor, void *key);

/* Get key string value.*/
char *get_key_str(void *key, DataType data_type);

/* Get key type name. */
char *key_type_name(MetaColumn *meta_column);
