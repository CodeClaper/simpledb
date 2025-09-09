#include <stdint.h>
#include "asserts.h"

/* Leftmost 32bit word postion. */
int leftmost_32_pos(uint32_t word);

/* Check if the pinter is align. */
int is_aligned(void *ptr, size_t alignment);
