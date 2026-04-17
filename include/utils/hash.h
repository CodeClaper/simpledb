#include "c.h"
#include "sys.h"

/* HashCode. */
typedef unsigned long Hash;

Hash StringHash(char *strVal, Size size);
Hash OidHash(Oid oid, Size size);
