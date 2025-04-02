#include "gtest/gtest.h"
#include <cstddef>
#include <cstring>
#include <stdlib.h>
#include <stdio.h>

extern "C" {
#include "list.h"
#include "qsort.h"
}

/* Test for sort int list asc. */
TEST(qsort, int_sort_asc) {
    List *list = create_list(NODE_INT);

    int i;
    for (i = 0; i < 100; i++) {
        int r = rand() % 100;
        append_list(list, &r);
    }

    list_qsort(list, list_int_sort_comparator);

    ListCell *prev = NULL;
    ListCell *lc;
    foreach (lc, list) {
        if (prev != NULL)
            ASSERT_LE(lfirst_int(prev), lfirst_int(lc));
        prev = (lc);
    }
}


/* Test for sort int list asc. */
TEST(qsort, int_sort_desc) {
    List *list = create_list(NODE_INT);

    int i;
    for (i = 0; i < 100; i++) {
        int r = rand() % 100;
        append_list(list, &r);
    }

    list_qsort(list, list_int_sort_comparator_desc);

    ListCell *prev = NULL;
    ListCell *lc;
    foreach (lc, list) {
        if (prev != NULL)
            ASSERT_GE(lfirst_int(prev), lfirst_int(lc));
        prev = (lc);
    }
}

/* Test string value list qsort. */
TEST(qsort, string_sort) {
    List *list = create_list(NODE_STRING);
    append_list(list, (char *)"Beijing");
    append_list(list, (char *)"Nanjing");
    append_list(list, (char *)"Hangzhou");
    append_list(list, (char *)"Shanghai");
    append_list(list, (char *)"Shenzhen");
    append_list(list, (char *)"Xian");
    append_list(list, (char *)"Hongkong");
    append_list(list, (char *)"Chengdu");
    append_list(list, (char *)"Hosten");
    append_list(list, (char *)"Lindon");
    append_list(list, (char *)"Jinan");
    append_list(list, (char *)"Tokyo");
    append_list(list, (char *)"LosAngle");

    list_qsort(list, implement_list_sort_comparator(T_STRING));

    ListCell *prev = NULL;
    ListCell *lc;
    foreach (lc, list) {
        if (prev != NULL)
            ASSERT_TRUE(strcmp((char *) lfirst(prev), (char *) lfirst(lc)));
        prev = (lc);
    }

}
