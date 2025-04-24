#include "hash.h"

/* Hash the String value. */
Hash StringHash(char *strVal, Size size) {
    /* Use DJB2 hash alg. */
    unsigned long hash = 5381;
    int c;
    while ((c = *strVal++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % size;
}


/* Has an Oid value. */
Hash OidHash(Oid oid, Size size) {
    /* Use SplitMix64 has alg. */
    oid = (oid ^ (oid >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    oid = (oid ^ (oid >> 27)) * UINT64_C(0x94d049bb133111eb);
    return (oid^ (oid >> 31)) % size;
}
