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
    NODE_VOID,
    NODE_STRING,
    NODE_LIST,
    NODE_KEY_VALUE,
    NODE_ROW,
    NODE_REFER,
    NODE_COLUMN,
    NODE_PAGE,
    NODE_BUFFER_DESC,
    NODE_TABLE,
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
    NODE_TABLE_BUFFER_ENTRY
} NodeTag;

/* Cell in List.*/
typedef union ListCell {
    void *ptr_value;
    int int_value;
    bool bool_value;
    float float_value;
    double double_value;
} ListCell;

/* List */
typedef struct List {
    NodeTag type;
    uint32_t size;
    uint32_t capacity;
    ListCell *elements;
    ListCell initial_elements[INIT_LIST_CELL_SIZE];
} List;


#define lfirst(l) ((l)->ptr_value)
#define lfirst_int(l) ((l)->int_value)
#define lfirst_bool(l) ((l)->bool_value)
#define lfirst_float(l) ((l)->float_value)
#define lfirst_double(l) ((l)->double_value)

/* foreach: a macro for looping through a list.
 * Notice: there use __i rather than i as iterators, 
 * user habitually input i at the loop body that can cause mess. */
#define foreach(lc, list) \
        for (uint32_t __i = 0; __i < list->size ? (lc = &list->elements[__i], true) : (lc = NULL, false); __i++)

/* forboth: a macro for looping through both lists and stoping when either list runs out of elements. */
#define forboth(lc1, list1, lc2, list2) \
        for (uint32_t __i = 0; __i < list1->size && __i < list2->size ? (lc1 = &list1->elements[__i], lc2 = &list2->elements[__i], true) : (lc1 = NULL, lc2 = NULL,  false); __i++)

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

/* Create List and initialization. 
 * Return the created list.
 * */
List *create_list(NodeTag type);

/* Append item to list. */
void append_list(List *list, void *item);

/* Check if the int item is the member of list. */
bool list_member_int(List *list, int item);

/* Check if the bool item is the member of list. */
bool list_member_bool(List *list, bool item);

/* Check if the float item is the member of list. */
bool list_member_float(List *list, float item);

/* Check if the double item is the member of list. */
bool list_member_double(List *list, double item);

/* Check if the pointer is the member of list. */
bool list_member_ptr(List *list, void *ptr);

/* Check if the item is the memeber of list. */
bool list_member(List *list, void *item);

/* Delete int item in List. 
 * Skip if not found in list.
 * */
void list_delete_int(List *list, int item);

/* Delete bool item in List. 
 * Skip if not found in list.
 * */
void list_delete_bool(List *list, bool item);

/* Delete float item in List. 
 * Skip if not found in list.
 * */
void list_delete_float(List *list, float item);

/* Delete double item in List. 
 * Skip if not found in list.
 * */
void list_delete_double(List *list, double item);


/* Delete item in List. 
 * Skip if not found in list.
 * */
void list_delete(List *list, void *item);


/* Delete the last n item */
void list_delete_tail(List *list, int num);


/* Replace list at n. */
void list_replace_at(List *list, int n, void *item);


/* List copy. */
List *list_copy(List *old_list);


/* List copy deep. */
List *list_copy_deep(List *old_list);


/* Free all cells and objects those are 
 * point-to by cells in list DOES NOT be freed*/
void free_list(List *list);

/* Free all cells and any object that are 
 * point-to by cells in list will be freed*/
void free_list_deep(List *list);

#endif
