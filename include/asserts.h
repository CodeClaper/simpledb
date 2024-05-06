#include <stdbool.h>
#include <stdlib.h>

#ifdef DEBUG
#include "assert.h"
#define Assert(condition) assert(condition)
#else
#define Assert(condition) ((void)true)
#endif

#define UNEXPECTED_VALUE(EXPR) do {                    \
    fprintf(stderr,                                    \
      "%s:%d: %lld: unexpected value for " #EXPR "\n", \
      __FILE__, __LINE__, (long long)(EXPR)            \
    );                                                 \
    abort();                                           \
  } while (0)

/* Assert condition is true. */
void assert_true(bool condition, char *format, ...);

/* Assert condition is false. */
void assert_false(bool condition, char *format, ...);

/* Assert expression not null. */
void assert_not_null(void *express, char *format, ...);

/* Assert expression not null. */
void assert_null(void *express, char *format, ...);
