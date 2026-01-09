#include <setjmp.h>
#include <stdint.h>
#include "minunit.h"
#include "data.h"
#include "mmgr.h"
#include "./test_flatten.c"

/* 
 * Conf 
 */
Conf *conf; 

/* 
 * jmp_buf for error. 
 */
jmp_buf errEnv; 

/* 
 * program name
 */
const char *program_name;  


void test_setup(void) {
	/* Nothing */
}

void test_teardown(void) {
	/* Nothing */
}

MU_TEST_SUITE(simpledb) {
	MU_SUITE_CONFIGURE(&test_setup, &test_teardown);
    MU_RUN_TEST(test_flatten);
}

int main(int argc, char *argv[]) {
    conf = instance(Conf);
    MU_RUN_SUITE(simpledb);
	MU_REPORT();
	return MU_EXIT_CODE;
}
