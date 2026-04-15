#include "gtest/gtest.h"

extern "C" {
#include "db.h"
}

int main(int argc, char **argv) {
    init_db();
    parse_argv(argc, argv);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
