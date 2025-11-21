#include <stdint.h>
#include "random.h"
#include "timer.h"

static uint32_t random_loop = 0;

/* random an uint64_t number. */
uint64_t RandomUint64() {
    random_loop++;
    if (random_loop > 12345) random_loop = 0;
    int64_t timestamp = get_timestamp(NANOSECOND);
    return (timestamp & 0xFFFFFFFFFFFFF00) | (random_loop % 8);
}
