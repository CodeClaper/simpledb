#include <stdint.h>
#include "minunit.h"
#include "db.h"
#include "./test_flatten.c"
#include "./test_log_macros.c"

void test_setup(void) {
	/* Nothing */
}

void test_teardown(void) {
	/* Nothing */
}

MU_TEST_SUITE(simpledb) {
	MU_SUITE_CONFIGURE(&test_setup, &test_teardown);
    MU_RUN_TEST(test_flatten);
    MU_RUN_TEST(test_panic_exits_with_failure);
    MU_RUN_TEST(test_unreachable_exits_with_failure);
}

int main(int argc, char *argv[]) {
    init_db();
    parse_argv(argc, argv);
    MU_RUN_SUITE(simpledb);
	MU_REPORT();
	return MU_EXIT_CODE;
}
