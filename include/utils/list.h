#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include "asserts.h"

#ifndef LIST_H
#define LIST_H

#define INIT_LIST_CELL_SIZE 8
#define NIL (List *)(NULL)

typedef enum NodeTag {
    NODE_INT,
    NODE_BOOL,
    NODE_FLOAT,
    NODE_DOUBLE,
    NODE_LONG,
    NODE_VOID,
    NODE_STRING,
    NODE_LIST,
    NODE_KEY_VALUE,
    NODE_ROW,
    NODE_REFER,
    NODE_COLUMN,
    NODE_PAGE,
    NODE_BUFFER_DESC,
    NODE_META_COLUMN,
    NODE_META_INDEX,
    NODE_TABLE,
    NODE_EXPR_NODE,
    NODE_STATEMENT,
    NODE_DB_RESULT,
    NODE_COLUMN_DEF,
    NODE_COLUMN_DEF_OPT,
    NODE_COLUMN_DEF_NAME,
    NODE_SCALAR_EXP,
    NODE_VALUE_ITEM,
    NODE_BASE_TABLE_ELEMENT,
    NODE_TABLE_REFER,
    NODE_ASSIGNMENT,
    NODE_TABLE_BUFFER_ENTRY,
    NODE_INTERNAL_NODE_CELL_DESC
} NodeTag;

/* Cell in List.*/
typedef union ListCell {
    void *ptr_value;
    int int_value;
    bool bool_value;
    float float_value;
    double double_value;
    int64_t long_value;
} ListCell;

/* List */
typedef struct List {
    NodeTag type;
    volatile uint32_t size;
    volatile uint32_t capacity;
    ListCell *elements;
    ListCell initial_elements[INIT_LIST_CELL_SIZE];
} List;


#define lfirst(l) ((l)->ptr_value)
#define lfirst_int(l) ((l)->int_value)
#define lfirst_bool(l) ((l)->bool_value)
#define lfirst_float(l) ((l)->float_value)
#define lfirst_double(l) ((l)->double_value)
#define lfirst_long(l) ((l)->long_value)

/* foreach: a macro for looping through a list.
 * Notice: there use __i rather than i as iterators, 
 * user habitually input i at the loop body that can cause mess. */
#define foreach(lc, list) \
        for (uint32_t __i = 0; __i < list->size ? (lc = &list->elements[__i], true) : (lc = NULL, false); __i++)

/* forboth: a macro for looping through both lists and stoping when either list runs out of elements. */
#define forboth(lc1, list1, lc2, list2) \
        for (uint32_t __i = 0; __i < list1->size && __i < list2->size ? (lc1 = &list1->elements[__i], lc2 = &list2->elements[__i], true) : (lc1 = NULL, lc2 = NULL,  false); __i++)

#define stepback() \
        (__i--)

/* Last list cell. */
static inline ListCell *first_cell(List *list) {
    Assert(list != NIL);
    return list->size == 0 
            ? NULL 
            : &list->elements[0];
}

/* Last list cell. */
static inline ListCell *last_cell(List *list) {
    Assert(list != NIL);
    return list->size == 0 
            ? NULL 
            : &list->elements[list->size - 1];
}

/* Second to last cell. */
static inline ListCell *second_last_cell(List *list) {
    Assert(list != NIL);
    return list->size >= 2 
            ? &list->elements[list->size - 2]
            : NULL;
}

/* Third to last cell. */
static inline ListCell *third_last_cell(List *list) {
    Assert(list != NIL);
    return list->size >= 3 
            ? &list->elements[list->size - 3]
            : NULL;
}

/* Locate the n'th cell (starts from 0) of the list.
 * It is an assertion failure if there is no such cell.
 * */
static inline ListCell *list_nth_cell(List *list, int nth) {
    Assert(list != NIL);
    assert_true(nth >= 0 && nth < list->size, "nth: %d, size: %d.", nth, list->size);
    Assert(nth >= 0 && nth < list->size);

    return &list->elements[nth];
}


/* Length of list. */
static inline uint32_t len_list(List *list) {
    Assert(list != NIL);
    return list->size;
}

/* List is empty. */
static inline bool list_empty(List *list) {
    return list->size == 0;
}

/* List is null or empty. */
static inline bool list_null_or_empty(List *list) {
    return list == NIL || list->size == 0;
}

List *create_list(NodeTag type);
List *create_list_init(NodeTag type, List *init_list);
void append_list_int(List *list, int item);
void append_list_long(List *list, int64_t item);
void append_list(List *list, void *item);
void append_list_at(List *list, void *item, uint32_t index);
bool list_member_int(List *list, int item);
bool list_member_bool(List *list, bool item);
bool list_member_float(List *list, float item);
bool list_member_double(List *list, double item);
bool list_member_ptr(List *list, void *ptr);
bool list_member(List *list, void *item);
bool list_all_int(List *list, int item);
void list_delete_cell(List *list, ListCell *lc);
void list_delete_int(List *list, int item);
void list_delete_bool(List *list, bool item);
void list_delete_float(List *list, float item);
void list_delete_double(List *list, double item);
void list_delete(List *list, void *item);
void list_delete_at(List *list, int index);
void list_delete_int_first(List *list, int item);
void list_delete_tail(List *list, int num);
void list_replace_at(List *list, int n, void *item);
List *list_copy(List *old_list);
List *list_copy_deep(List *old_list);
void free_list(List *list);
void free_list_deep(List *list);

#endif
