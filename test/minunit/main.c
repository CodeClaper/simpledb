#include "minunit.h"

void test_setup(void) {
	/* Nothing */
}

void test_teardown(void) {
	/* Nothing */
}

MU_TEST_SUITE(simpledb) {
	MU_SUITE_CONFIGURE(&test_setup, &test_teardown);
}

int main(int argc, char *argv[]) {
    MU_RUN_SUITE(simpledb);
	MU_REPORT();
	return MU_EXIT_CODE;
}
