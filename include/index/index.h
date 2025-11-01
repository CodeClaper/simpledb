#include <stdbool.h>
#include "data.h"

/* Index create. */
bool IndexCreate(MetaIndex *meta_index);

/* Index load. */
MetaIndex *IndexLoad(char *index_name);

/* Index drop. */
bool IndexDrop(char *index_name);
