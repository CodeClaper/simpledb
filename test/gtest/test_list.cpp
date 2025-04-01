#include "gtest/gtest.h"
#include <cstring>

extern "C" {
#include "list.h"
#include "data.h"
#include "mmgr.h"
}

/* Test for create list. */
TEST(list, create_list) {
    List *list = create_list(NODE_INT);
    ASSERT_NE(list, NIL);
    ASSERT_EQ(list->size, 0);
}

/* Test for enlarg list. */
TEST(list, enlarge_list) {
    List *list = create_list(NODE_INT);

    int i;
    for (i = 0; i < 30; i++) {
        append_list(list, &i);       
    }

    ASSERT_EQ(list->size, 30);
    ASSERT_EQ(list->capacity, 32);

    ListCell *lc;
    int j = 0;
    foreach (lc, list) {
        ASSERT_EQ(lfirst_int(lc), j);
        j++;
    }
}

/* Test for append_list. */
TEST(list, append_list) {
     char* strings[10] = {
        "hello everyone!", 
        "¡Hola a todos!", 
        "大家好!", 
        "Bonjour à tous !", 
        "Guten Tag, alle zusammen!", 
        "Ciao a tutti!", 
        "Здравствуйте, все!", 
        "こんにちは、皆さん！", 
        " 여러분, 안녕하세요!", 
        "مرحباً جميعًا!" 
    };
    
    List *list = create_list(NODE_STRING);
    for (int i = 0; i < 10; i++) {
        append_list(list, strings[i]);
    }
    
    ListCell *lc;
    int j = 0;
    foreach (lc, list) {
        ASSERT_STREQ((char *)lfirst(list_nth_cell(list, j)), strings[j]);
        j++;
    }
    ASSERT_EQ(j, 10);
}

/* Test for foreach macro. */
TEST(list, foreach) {
    List *list = create_list(NODE_INT);

    for (int i = 0; i < 10; i++) {
        append_list(list, &i);    
    }

    int k = 0;

    ListCell *lc;
    foreach (lc, list) {
        ASSERT_EQ(lfirst_int(lc), k++);    
    }
}

/* Test for forboth marco. */
TEST(list, forboth) {
     char* strings[10] = {
        "hello everyone!", 
        "¡Hola a todos!", 
        "大家好!", 
        "Bonjour à tous !", 
        "Guten Tag, alle zusammen!", 
        "Ciao a tutti!", 
        "Здравствуйте, все!", 
        "こんにちは、皆さん！", 
        " 여러분, 안녕하세요!", 
        "مرحباً جميعًا!" 
    };

    List *ls1 = create_list(NODE_INT);
    List *ls2 = create_list(NODE_STRING);

    for (int i = 0; i < 10; i++) {
        append_list(ls1, &i);
        append_list(ls2, strings[i]);
    }

    ListCell *lc1, *lc2;

    int k = 0;
    forboth(lc1, ls1, lc2, ls2) {
        ASSERT_EQ(lfirst_int(lc1), k);
        ASSERT_STREQ((char *)lfirst(lc2), strings[k]);
        k++;
    }
}

/* Test for checking item in list member. */
TEST(list, list_member_int) {
    List *list = create_list(NODE_INT);

    for (int i = 0; i < 10; i++) {
        append_list(list, &i);    
    }

    ASSERT_TRUE(list_member_int(list, 2));
    ASSERT_TRUE(list_member_int(list, 9));
    ASSERT_FALSE(list_member_int(list, 10));
}

/* Test for list delete item. */
TEST(list, list_delete_int) {
    List *list = create_list(NODE_INT);

    for (int i= 0; i < 10; i++) {
        append_list(list, &i);    
    }
    
    list_delete_int(list, 2);
    list_delete_int(list, 6);
    list_delete_int(list, 8);

    ASSERT_FALSE(list_member_int(list, 2));
    ASSERT_FALSE(list_member_int(list, 6));
    ASSERT_TRUE(list_member_int(list, 5));

    ASSERT_TRUE(len_list(list) == 7);
}


/* Test for free_list. */
TEST(list, free_list) {

     char* strings[10] = {
        "hello everyone!", 
        "¡Hola a todos!", 
        "大家好!", 
        "Bonjour à tous !", 
        "Guten Tag, alle zusammen!", 
        "Ciao a tutti!", 
        "Здравствуйте, все!", 
        "こんにちは、皆さん！", 
        " 여러분, 안녕하세요!", 
        "مرحباً جميعًا!" 
    };

    List *list = create_list(NODE_STRING);

    for (int i = 0 ; i < 10; i++) {
        append_list(list, strings[i]);
    }

    free_list(list);
}

/* Test for free list deep. */
TEST(list, free_list_deep) {

    List *list = create_list(NODE_COLUMN_DEF);

    int i;
    for (i = 0; i < 30; i++) {
        ColumnDefNode *node = instance(ColumnDefNode);
        append_list(list, node);       
    }

    free_list_deep(list);
}


/* Test for free_list. */
TEST(list, list_copy_deep) {
    
     char* strings[10] = {
        "hello everyone!", 
        "¡Hola a todos!", 
        "大家好!", 
        "Bonjour à tous !", 
        "Guten Tag, alle zusammen!", 
        "Ciao a tutti!", 
        "Здравствуйте, все!", 
        "こんにちは、皆さん！", 
        " 여러분, 안녕하세요!", 
        "مرحباً جميعًا!" 
    };

    List *list1 = create_list(NODE_STRING);

    for (int i = 0 ; i < 10; i++) {
        append_list(list1, strdup(strings[i]));
    }

    List *list2 = list_copy_deep(list1);

    ASSERT_NE(list1, list2);
    ASSERT_NE(first_cell(list1), first_cell(list2));
    ASSERT_NE(lfirst(first_cell(list1)), lfirst(first_cell(list2)));

}


/* Test for free_list. */
TEST(list, list_copy) {
    
     char* strings[10] = {
        "hello everyone!", 
        "¡Hola a todos!", 
        "大家好!", 
        "Bonjour à tous !", 
        "Guten Tag, alle zusammen!", 
        "Ciao a tutti!", 
        "Здравствуйте, все!", 
        "こんにちは、皆さん！", 
        " 여러분, 안녕하세요!", 
        "مرحباً جميعًا!" 
    };

    List *list1 = create_list(NODE_STRING);

    for (int i = 0 ; i < 10; i++) {
        append_list(list1, strings[i]);
    }

    List *list2 = list_copy(list1);

    ASSERT_NE(list1, list2);
    ASSERT_NE(first_cell(list1), first_cell(list2));
    ASSERT_EQ(lfirst(first_cell(list1)), lfirst(first_cell(list2)));

}


/* Test for list_replace_at. */
TEST(list, list_replace_at) {
    
     char* strings[10] = {
        "hello everyone!", 
        "¡Hola a todos!", 
        "大家好!", 
        "Bonjour à tous !", 
        "Guten Tag, alle zusammen!", 
        "Ciao a tutti!", 
        "Здравствуйте, все!", 
        "こんにちは、皆さん！", 
        " 여러분, 안녕하세요!", 
        "مرحباً جميعًا!" 
    };

    List *list = create_list(NODE_STRING);

    for (int i = 0; i < 10; i++) {
        append_list(list, strings[i]);
    }

    char *item = "hello world";

    list_replace_at(list, 2, item);

    ASSERT_EQ(lfirst(list_nth_cell(list, 2)), "hello world");

}
