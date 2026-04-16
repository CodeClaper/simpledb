#include <stdlib.h>

void *lodalloc(size_t size);
void lofree(void *ptr);
void *lodrealloc(void *ptr, size_t size);
char *lodstrdup(char *str);
