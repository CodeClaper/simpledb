#include "gtest/gtest.h"
#include <cstring>

extern "C" {
#include "hash.h"
#include "systable.h"
}

#define BULK_SIZE 1000

static char *randomString(size_t len, const char *charset) {
    char* str = (char *) malloc(len + 1);
    if (!str) return NULL;
    
    size_t charset_len = strlen(charset);
    for (size_t i = 0; i < len; i++) {
        int key = rand() % charset_len;
        str[i] = charset[key];
    }
    str[len] = '\0';
    return str;
}

/* Test for OidHash. */
TEST(hash, OidHash) {
    int collision = 0;
    int bulk[BULK_SIZE];
    for (int i = 0; i < BULK_SIZE; i++) {
        bulk[i] = 0;
    }
    
    for (int i = 0; i < BULK_SIZE; i++) {
        Hash hash = OidHash(FindNextOid(), BULK_SIZE);
        if (bulk[hash] > 0)
            collision++;
        bulk[hash]++;
    }
    
    float per = (float) collision / (BULK_SIZE * 100);
    ASSERT_LE(per, 0.1);
}

/* Test for StringHash. */
TEST(hash, StringHash) {
    int collision = 0;
    int bulk[BULK_SIZE];
    for (int i = 0; i < BULK_SIZE; i++) {
        bulk[i] = 0;
    }
    
    for (int i = 0; i < BULK_SIZE; i++) {
        const char* charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";       
        Hash hash = StringHash(randomString(10, charset), BULK_SIZE);
        if (bulk[hash] > 0)
            collision++;
        bulk[hash]++;
    }
    
    float per = (float) collision / (BULK_SIZE * 100);
    ASSERT_LE(per, 0.1);
}

