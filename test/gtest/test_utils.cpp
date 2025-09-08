#include "gtest/gtest.h"
#include <cstddef>
#include <cstdint>

extern "C" {
#include "data.h"
#include "mmgr.h"
#include "utils.h"
}

/* Test for ltrim. */
TEST(utils, LeftTrim) {
    char *string = dstrdup("  hello");
    ASSERT_STREQ(LeftTrim(string), "hello");
}

/* Test for ltrim. */
TEST(utils, RightTrim) {
    char *string = dstrdup("hello  ");
    ASSERT_STREQ(RightTrim(string), "hello");
}

/* Test for trim. */
TEST(utils, Trime) {
    char *string = dstrdup("  hello  ");
    ASSERT_STREQ(Trim(string), "hello");
}

/* Test for contains. */
TEST(utils, Contains) {
    ASSERT_TRUE(Contains("hello world", "ello"));
    ASSERT_FALSE(Contains("hello world", "war"));
}

/* Test for startwith. */
TEST(utils, StartWith) {
    ASSERT_TRUE(StartWith("hello world", "hello"));
    ASSERT_FALSE(StartWith("hello world", "world"));
}

/* Test for startwith. */
TEST(utils, EndWith) {
    ASSERT_TRUE(EndWith("hello world", "world"));
    ASSERT_FALSE(EndWith("hello world", "hello"));
}

/* Test for substr. */
TEST(utils, SubStr) {
    ASSERT_STREQ(SubStr("hello world", 2, 4), "llo");
    ASSERT_STREQ(SubStr("hello world", 1, 7), "ello wo");
}

/* Test for replace_onece. */
TEST(utils, ReplaceOnce) {
    ASSERT_STREQ(ReplaceOnce("hello world", "world", "nihao"), "hello nihao");
}

/* Test for replace. */
TEST(utils, ReplaceAll) {
    ASSERT_STREQ(ReplaceAll("There are a hundred Hamlets in a hundred people's eyes", "hundred", "thousand"), 
                 "There are a thousand Hamlets in a thousand people's eyes");
    ASSERT_STREQ(ReplaceAll("There are a thousand Hamlets in a thousand people's eyes", "thousand", "hundred"), 
                 "There are a hundred Hamlets in a hundred people's eyes");
}

/* Test for is_empty. */
TEST(utils, StrIsEmpty) {
    ASSERT_TRUE(StrIsEmpty(NULL));
    ASSERT_TRUE(StrIsEmpty(""));
    ASSERT_TRUE(StrIsEmpty("  "));
    ASSERT_FALSE(StrIsEmpty("\t"));
    ASSERT_FALSE(StrIsEmpty("\v"));
    ASSERT_FALSE(StrIsEmpty("\n"));
    ASSERT_FALSE(StrIsEmpty("\r"));
}

/* Test for format. */
TEST(utils, FormatStr) {
    ASSERT_STREQ(FormatStr("hello, %s!", "everyone"), "hello, everyone!");
    ASSERT_STREQ(FormatStr("%d percents", 100), "100 percents");
    ASSERT_STREQ(FormatStr("%0.1f kilometers left", 2.5), "2.5 kilometers left");
}

/* Test for streq. */
TEST(utils, StrEq) {
    ASSERT_TRUE(StrEq("hello", "hello"));
    ASSERT_FALSE(StrEq("hello", "nihao"));
    ASSERT_FALSE(StrEq("hello", NULL));
    ASSERT_FALSE(StrEq(NULL, NULL));
}

/* Test for streq. */
TEST(utils, StrEqOrNull) {
    ASSERT_TRUE(StrEqOrNull("hello", "hello"));
    ASSERT_FALSE(StrEqOrNull("hello", "nihao"));
    ASSERT_FALSE(StrEqOrNull("hello", NULL));
    ASSERT_TRUE(StrEqOrNull(NULL, NULL));
}

/* Test for itos. */
TEST(utils, IntToStr) {
    ASSERT_STREQ(IntToStr(32), "32");
    ASSERT_STRNE(IntToStr(61), "32");
}

/* Test for ltos. */
TEST(utils, LongToStr) {
    ASSERT_STREQ(LongToStr(1302323), "1302323");
    ASSERT_STREQ(LongToStr(662323232), "662323232");
}

/* Test for ftos*/
TEST(utils, FloatToStr) {
    ASSERT_STREQ(FloatToStr(3.14), "3.140000");
    ASSERT_STREQ(FloatToStr(6.68232), "6.682320");
}


/* Test for dtos*/
TEST(utils, DoubleToStr) {
    ASSERT_STREQ(DoubleToStr(3.14159265358979323846264338327950288419716939937510), "3.141592653589793");
}

/* Test for stoi32. */
TEST(utils, StrToInt) {
    int32_t ret;
    ASSERT_EQ(StrToInt("121211213232323442322", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToInt("1212asbaddd", &ret), ST_INVALID);
    ASSERT_EQ(StrToInt("124322787873871871983727462737647826737627", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToInt("-1217284658497394738478324878437847288784744323", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToInt("-882732832748329724959437223", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToInt("-2372877477774837276372637627367263726376273627637263786273672637263762783", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToInt("2327837473478374872837487487238472387483274872387482374823784723847823748358928482378547237482374823748", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToInt("2147483648", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToInt("2147483647", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, 2147483647);
    ASSERT_EQ(StrToInt("-2147483648", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, -2147483648);
    ASSERT_EQ(StrToInt("232873823", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, 232873823);
    ASSERT_EQ(StrToInt("-2337827", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, -2337827);
    ASSERT_EQ(StrToInt("-1122323233", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, -1122323233);
}


/* Test for StrToLong. */
TEST(utils, StrToLong) {
    int64_t ret;
    ASSERT_EQ(StrToLong("121211213232323442322", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToLong("1212asbaddd", &ret), ST_INVALID);
    ASSERT_EQ(StrToLong("124322787873871871983727462737647826737627", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToLong("-1217284658497394738478324878437847288784744323", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToLong("-882732832748329724959437223", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToLong("-2372877477774837276372637627367263726376273627637263786273672637263762783", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToLong("2327837473478374872837487487238472387483274872387482374823784723847823748358928482378547237482374823748", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToLong("9223372036854775808", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToLong("9223372036854775807", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, 9223372036854775807);
    ASSERT_EQ(StrToLong("-9223372036854775808", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, -9223372036854775808);
    ASSERT_EQ(StrToLong("232873823", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, 232873823);
    ASSERT_EQ(StrToLong("-2337827", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, -2337827);
    ASSERT_EQ(StrToLong("-1122323233", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, -1122323233);
}

/* Test for StrToFloat. */
TEST(utils, StrToFloat) {
    float ret;
    ASSERT_EQ(StrToFloat("12121321312312312312312314123124134742748723743772472384732874894872374327489273481784723374723487283478237487324.277761327462839188378274372747234762374672364762874672647627467236472647627346723648726347234678236476234757823748572985782738582749823784728374", &ret), ST_OVERFLOW);
    ASSERT_EQ(StrToFloat("12.232323u", &ret), ST_INVALID);
    ASSERT_EQ(StrToFloat("0.23", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, 0.23f);
}


/* Test for StrToDouble. */
TEST(utils, StrToDouble) {
    double ret;
    ASSERT_EQ(StrToDouble("12121321312312312312312314123124134742748723743772472384732874894872374327489273481784723374723487283478237487324.277761327462839188378274372747234762374672364762874672647627467236472647627346723648726347234678236476234757823748572985782738582749823784728374", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, 12121321312312312312312314123124134742748723743772472384732874894872374327489273481784723374723487283478237487324.277761327462839188378274372747234762374672364762874672647627467236472647627346723648726347234678236476234757823748572985782738582749823784728374);
    ASSERT_EQ(StrToDouble("12.232323u", &ret), ST_INVALID);
    ASSERT_EQ(StrToDouble("0.23", &ret), ST_SUCCESS);
    ASSERT_EQ(ret, 0.23);
}
