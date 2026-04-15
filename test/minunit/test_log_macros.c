#include <unistd.h>
#include <sys/wait.h>
#include <stdlib.h>
#include "minunit.h"
#include "log.h"
#include "mmgr.h"

/* Helper to run a function in a child process and return exit status */
static int run_in_child(int (*func)(void)) {
    pid_t pid = fork();
    if (pid < 0) {
        return -1; /* fork failed */
    }
    if (pid == 0) {
        /* Child */
        func();
        /* Should not reach here */
        _exit(EXIT_FAILURE);
    }
    /* Parent */
    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        return -WTERMSIG(status);
    }
    return -999;
}

static int run_in_child2(void (*func)(void)) {
    pid_t pid = fork();
    if (pid < 0) {
        return -1; /* fork failed */
    }
    if (pid == 0) {
        /* Child */
        func();
        /* Should not reach here */
        _exit(EXIT_FAILURE);
    }
    /* Parent */
    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        return -WTERMSIG(status);
    }
    return -999;
}

/* Test functions that will be called in child */
static void test_panic_macro(void) {
    panic("panic test message");
}

static int test_unreachable_macro(void) {
    unreachable(72, "unreachable test message");
}

MU_TEST(test_panic_exits_with_failure) {
    int exit_status = run_in_child2(test_panic_macro);
    mu_assert_int_eq(EXIT_FAILURE, exit_status);
}

MU_TEST(test_unreachable_exits_with_failure) {
    int exit_status = run_in_child(test_unreachable_macro);
    mu_assert_int_eq(EXIT_FAILURE, exit_status);
}

