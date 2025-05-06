#include <stdbool.h>
#include <stdint.h>

/* Atomically add to ptr. 
 * ----------------------
 * Return the value the old value.
 * Memory barrier semantics.
 * */
static inline uint32_t atmomic_fetch_add_uint32(volatile uint32_t *ptr, uint32_t add) {
    return __sync_fetch_and_add(ptr, add);
}

/* Atomically compare the expected value and swap to newval. 
 * -----------------------
 * Return true if current ptr value and expected have the same value,
 * and store the newval to ptr. Otherwise, return false.
 * The expected will be updated to the ptr value.
 * */
static inline bool atmomic_compare_swap_uint32(volatile uint32_t *ptr, uint32_t *expected, uint32_t newval) {
    bool ret;
    uint32_t current;
    current = __sync_val_compare_and_swap(ptr, *expected, newval);
    ret = (current == *expected);
    *expected = current;
    return ret;
}
