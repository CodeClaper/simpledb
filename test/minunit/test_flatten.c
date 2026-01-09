#include "minunit.h"
#include "data.h"
#include "parser.h"

MU_TEST(test_flatten) {
    char *sql = "select * from a left join b where (a.A = 1 or b.B = 2) and (a.A = 2 or b.B = 3) or b.D = 3;";
    List *list = parse(sql);
    mu_check(len_list(list) == 1);
}
