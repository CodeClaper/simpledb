/****************************************List Modele********************************************
 * Intface of List package.
 **********************************************************************************************/

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "list.h"
#include "mmgr.h"
#include "free.h"
#include "asserts.h"
#include "data.h"
#include "copy.h"

/* Create List and initialization. 
 * Return the created list.
 * */
List *create_list(NodeTag type) {
    List *list = instance(List);
    list->type = type;
    list->size = 0;
    list->capacity = INIT_LIST_CELL_SIZE;
    list->elements = list->initial_elements;
    return list;
}

/* Create List with init list. */
List *create_list_init(NodeTag type, List *init_list) {
    List *list = create_list(type);
    ListCell *lc;
    foreach (lc, init_list) {
        void *item = lfirst(lc);
        append_list(list, item);
    }
    return list;
}

/* Enlarge the list and expand the capacity. 
 * List will growth capacity with the power of 2. 
 * */
static void enlarge_list(List *list) {
    list->capacity = list->capacity * 2;
    
    if (list->elements == list->initial_elements) {
        list->elements = dalloc(sizeof(ListCell) * list->capacity);
        memcpy(list->elements, list->initial_elements, sizeof(ListCell) * list->size);
    } else {
        list->elements = drealloc(list->elements, sizeof(ListCell) * list->capacity);
    }
}

/* Append int item to list. */
void append_list_int(List *list, int item) {
    if (list->size >= list->capacity)
        enlarge_list(list);
    list->size++;
    lfirst_int(last_cell(list)) = item;
}

/* Append bool item to list. */
void append_list_bool(List *list, bool item) {
    if (list->size >= list->capacity)
        enlarge_list(list);
    list->size++;
    lfirst_bool(last_cell(list)) = item;
}

/* Append float item to list. */
void append_list_float(List *list, float item) {
    if (list->size >= list->capacity)
        enlarge_list(list);
    list->size++;
    lfirst_float(last_cell(list)) = item;
}

/* Append float item to list. */
void append_list_double(List *list, double item) {
    if (list->size >= list->capacity)
        enlarge_list(list);
    list->size++;
    lfirst_double(last_cell(list)) = item;
}

void append_list_long(List *list, int64_t item) {
    if (list->size >= list->capacity)
        enlarge_list(list);
    list->size++;
    lfirst_long(last_cell(list)) = item;
}


void append_list_ptr(List *list, void *item) {
    if (list->size >= list->capacity)
        enlarge_list(list);
    list->size++;
    lfirst(last_cell(list)) = item;
}

/* Append item to list. */
void append_list(List *list, void *item) {
    switch (list->type) {
        case NODE_INT:
            append_list_int(list, *(int *)item);
            break;
        case NODE_BOOL:
            append_list_bool(list, *(bool *)item);
            break;
        case NODE_FLOAT:
            append_list_float(list, *(float *)item);
            break;
        case NODE_DOUBLE:
            append_list_double(list, *(double *)item);
            break;
        case NODE_LONG:
            append_list_long(list, *(int64_t *)item);
            break;
        default:
            append_list_ptr(list, item);
            break;
    }
}

static void append_list_int_at(List *list, bool item, uint32_t index) {
    if (list->size >= list->capacity)
        enlarge_list(list);

    for (int i = list->size; i > index; i--) {
        memcpy(list->elements + i, list->elements + i - 1, sizeof(ListCell));
    }
    lfirst_int(list->elements + index) = item;
    list->size++;
}

static void append_list_bool_at(List *list, bool item, uint32_t index) {
    if (list->size >= list->capacity)
        enlarge_list(list);

    for (int i = list->size; i > index; i--) {
        memcpy(list->elements + i, list->elements + i - 1, sizeof(ListCell));
    }
    lfirst_bool(list->elements + index) = item;
    list->size++;
}

static void append_list_float_at(List *list, float item, uint32_t index) {
    if (list->size >= list->capacity)
        enlarge_list(list);

    for (int i = list->size; i > index; i--) {
        memcpy(list->elements + i, list->elements + i - 1, sizeof(ListCell));
    }
    lfirst_float(list->elements + index) = item;
    list->size++;
}

static void append_list_double_at(List *list, double item, uint32_t index) {
    if (list->size >= list->capacity)
        enlarge_list(list);

    for (int i = list->size; i > index; i--) {
        memcpy(list->elements + i, list->elements + i - 1, sizeof(ListCell));
    }
    lfirst_double(list->elements + index) = item;
    list->size++;
}


static void append_list_long_at(List *list, int64_t item, uint32_t index) {
    if (list->size >= list->capacity)
        enlarge_list(list);

    for (int i = list->size; i > index; i--) {
        memcpy(list->elements + i, list->elements + i - 1, sizeof(ListCell));
    }
    lfirst_long(list->elements + index) = item;
    list->size++;
}

static void append_list_ptr_at(List *list, void *item, uint32_t index) {
    if (list->size >= list->capacity)
        enlarge_list(list);

    for (int i = list->size; i > index; i--) {
        memcpy(list->elements + i, list->elements + i - 1, sizeof(ListCell));
    }
    lfirst(list->elements + index) = item;
    list->size++;
}

/* Append item to list at index. */
void append_list_at(List *list, void *item, uint32_t index) {
    switch (list->type) {
        case NODE_INT:
            append_list_int_at(list, *(int *)item, index);
            break;
        case NODE_BOOL:
            append_list_bool_at(list, *(bool *)item, index);
            break;
        case NODE_FLOAT:
            append_list_float_at(list, *(float *)item, index);
            break;
        case NODE_DOUBLE:
            append_list_double_at(list, *(double *)item, index);
            break;
        case NODE_LONG:
            append_list_long_at(list, *(int64_t *)item, index);
            break;
        default:
            append_list_ptr_at(list, item, index);
            break;
    }
}

/* Check if the int item is the member of list. */
bool list_member_int(List *list, int item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_int(lc) == item) 
            return true;
    }
    return false;
}

/* Check if the bool item is the member of list. */
bool list_member_bool(List *list, bool item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_bool(lc) == item) 
            return true;
    }
    return false;
}

/* Check if the float item is the member of list. */
bool list_member_float(List *list, float item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_float(lc) == item) 
            return true;
    }
    return false;
}

/* Check if the double item is the member of list. */
bool list_member_double(List *list, double item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_double(lc) == item) 
            return true;
    }
    return false;
}

/* Check if the long item is the member of list. */
bool list_member_long(List *list, int64_t item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_long(lc) == item) 
            return true;
    }
    return false;
}

/* Check if the pointer is the member of list. */
bool list_member_ptr(List *list, void *ptr) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst(lc) == ptr) 
            return true;
    }
    return false;
}

/* Check if the item is the memeber of list. */
bool list_member(List *list, void *item) {
    switch (list->type) {
        case NODE_INT:
            return list_member_int(list, *(int *)item);
        case NODE_BOOL:
            return list_member_bool(list, *(bool *)item);
        case NODE_FLOAT:
            return list_member_float(list, *(float *)item);
        case NODE_DOUBLE:
            return list_member_double(list, *(double *)item);
        case NODE_LONG:
            return list_member_long(list, *(int64_t *)item);
        default:
            return list_member_ptr(list, item);
    }
}

/* Check if the int item is all the member of list. */
bool list_all_int(List *list, int item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_int(lc) != item) 
            return false;
    }
    return true;
}

/* Delete nth cell in List. */
void list_delete_nth_cell(List *list, int n) {
    Assert(n >= 0 && n < list->size);
    memcpy(list->elements + n, list->elements + n + 1, 
           sizeof(ListCell) * (list->size - n - 1));
    list->size--;
}

/* Delete ListCell in List. */
void list_delete_cell(List *list, ListCell *lc) {
    list_delete_nth_cell(list, lc - list->elements);
}

/* Delete int item in List. 
 * Skip if not found in list.
 * */
void list_delete_int(List *list, int item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_int(lc) == item) {
            list_delete_cell(list, lc);
        }
    }
}

/* Delete bool item in List. 
 * Skip if not found in list.
 * */
void list_delete_bool(List *list, bool item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_bool(lc) == item) {
            list_delete_cell(list, lc);
        }
    }
}

/* Delete float item in List. 
 * Skip if not found in list.
 * */
void list_delete_float(List *list, float item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_float(lc) == item) {
            list_delete_cell(list, lc);
        }
    }
}


/* Delete double item in List. 
 * Skip if not found in list.
 * */
void list_delete_double(List *list, double item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_double(lc) == item) {
            list_delete_cell(list, lc);
        }
    }
}


/* Delete long item in List. 
 * Skip if not found in list.
 * */
void list_delete_long(List *list, int64_t item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_long(lc) == item) {
            list_delete_cell(list, lc);
        }
    }
}

/* Delete pointer item in List. 
 * Skip if not found in list.
 * */
void list_delete_ptr(List *list, void *item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst(lc) == item) {
            list_delete_cell(list, lc);
        }
    }
}

/* Delete item in List. 
 * Skip if not found in list.
 * Note: if item is a pointer, 
 * this function will not automatically free the memory.
 * */
void list_delete(List *list, void *item) {
    switch (list->type) {
        case NODE_INT:
            list_delete_int(list, *(int *)item);
            break;
        case NODE_BOOL:
            list_delete_bool(list, *(bool *)item);
            break;
        case NODE_FLOAT:
            list_delete_float(list, *(float *)item);
            break;
        case NODE_DOUBLE:
            list_delete_double(list, *(double *)item);
            break;
        case NODE_LONG:
            list_delete_long(list, *(int64_t *)item);
            break;
        default:
            list_delete_ptr(list, item);
            break;
    }
}

/* Delete item in list at index. */
void list_delete_at(List *list, int index) {
    list_delete_nth_cell(list, index);
}


/* Delete first target int item in List. 
 * Skip if not found in list.
 * */
void list_delete_int_first(List *list, int item) {
    ListCell *lc;
    foreach (lc, list) {
        if (lfirst_int(lc) == item) {
            list_delete_cell(list, lc);
            break;
        }
    }
}

/* Delete the last n item */
void list_delete_tail(List *list, int num) {
    Assert(num <= list->size);
    memset(list->elements + list->size - num, 0, sizeof(ListCell) * num);
    list->size = list->size - num;
}


void list_replace_at_int(List *list, int n, int item) {
    ListCell *lc = list_nth_cell(list, n);
    lfirst_int(lc) = item;
}


/* Replace list at n. */
void list_replace_at_bool(List *list, int n, bool item) {
    ListCell *lc = list_nth_cell(list, n);
    lfirst_bool(lc) = item;
}


/* Replace list at n. */
void list_replace_at_float(List *list, int n, float item) {
    ListCell *lc = list_nth_cell(list, n);
    lfirst_float(lc) = item;
}


/* Replace list at n. */
void list_replace_at_double(List *list, int n, double item) {
    ListCell *lc = list_nth_cell(list, n);
    lfirst_double(lc) = item;
}


/* Replace list at n. */
void list_replace_at_long(List *list, int n, long item) {
    ListCell *lc = list_nth_cell(list, n);
    lfirst_long(lc) = item;
}


/* Replace list at n. */
void list_replace_at_ptr(List *list, int n, void *item) {
    ListCell *lc = list_nth_cell(list, n);
    lfirst(lc) = item;
}

/* Replace list at n. */
void list_replace_at(List *list, int n, void *item) {

    Assert(n >= 0 && n < list->size);

    switch (list->type) {
        case NODE_INT:
            list_replace_at_int(list, n, *(int *)item);
            break;
        case NODE_BOOL:
            list_replace_at_bool(list, n, *(bool *)item);
            break;
        case NODE_FLOAT:
            list_replace_at_float(list, n, *(float *)item);
            break;
        case NODE_DOUBLE:
            list_replace_at_double(list, n, *(double *)item);
            break;
        case NODE_LONG:
            list_replace_at_long(list, n, *(int64_t *) item);
            break;
        default:
            list_replace_at_ptr(list, n, item);
            break;
    }

}


/* List copy shallow. 
 * Notice: the replica list must be free`d by free_list if necessary.
 * */
List *list_copy(List *old_list) {
    if (old_list == NIL)
        return NIL;

    List *new_list = create_list(old_list->type);
    new_list->size = old_list->size;
    new_list->capacity = old_list->capacity;
    memcpy(new_list->initial_elements, old_list->initial_elements, sizeof(ListCell) * INIT_LIST_CELL_SIZE);
    
    if (old_list->elements == old_list->initial_elements) {
        new_list->elements = new_list->initial_elements;
    } else {
        new_list->elements = dalloc(sizeof(ListCell) * new_list->capacity);
        memcpy(new_list->elements, old_list->elements, sizeof(ListCell) * new_list->size);
    }
    return new_list;
}

/* List copy deep. 
 * Notice: the replica list must be free`d by free_list_deep if necessary.
 * */
List *list_copy_deep(List *old_list) {
    if (old_list == NIL)
        return NIL;

    List *new_list = create_list(old_list->type);

    switch (new_list->type) {
        case NODE_INT: {
            ListCell *lc;
            foreach (lc, old_list) {
                append_list_int(new_list, lfirst_int(lc));
            }
            break;
        }
        case NODE_BOOL: {
            ListCell *lc;
            foreach (lc, old_list) {
                append_list_bool(new_list, lfirst_bool(lc));
            }
            break;
        }
        case NODE_FLOAT: {
            ListCell *lc;
            foreach (lc, old_list) {
                append_list_float(new_list, lfirst_float(lc));
            }
            break;
        }
        case NODE_DOUBLE: {
            ListCell *lc;
            foreach (lc, old_list) {
                append_list_double(new_list, lfirst_double(lc));
            }
            break;
        }
        case NODE_LONG: {
            ListCell *lc;
            foreach (lc, old_list) {
                append_list_long(new_list, lfirst_long(lc));
            }
            break;
        }
        case NODE_STRING: {
            ListCell *lc;
            foreach (lc, old_list) {
                char *replica = dstrdup(lfirst(lc));
                append_list(new_list, replica);
            }
            break;
        }
        case NODE_LIST: {
            ListCell *lc;
            foreach (lc, old_list) {
                List *replica = list_copy_deep(lfirst(lc));
                append_list(new_list, replica);
            }
            break;
        }
        case NODE_SCALAR_EXP: {
            ListCell *lc;
            foreach (lc, old_list) {
                ScalarExpNode *replica = copy_scalar_exp_node(lfirst(lc));
                append_list_ptr(new_list, replica);
            }
            break;
        }
        case NODE_VALUE_ITEM: {
            ListCell *lc;
            foreach (lc, old_list) {
                ValueItemNode *replica = copy_value_item_node(lfirst(lc));
                append_list_ptr(new_list, replica);
            }
            break;
        }
        case NODE_TABLE_REFER: {
            ListCell *lc;
            foreach (lc, old_list) {
                TableRefNode *replica = copy_table_ref_node(lfirst(lc));
                append_list(new_list, replica);
            }
            break;
        }
        case NODE_PAGE: {
            ListCell *lc;
            foreach (lc, old_list) {
                void *replica = copy_block(lfirst(lc), PAGE_SIZE);
                append_list(new_list, replica);
            }
            break;
        }
        case NODE_BUFFER_DESC: {
            ListCell *lc;
            foreach (lc, old_list) {
                void *replica = copy_buffer_desc(lfirst(lc));
                append_list(new_list, replica);
            }
            break;
        }
        case NODE_META_COLUMN: {
            ListCell *lc;
            foreach (lc, old_list) {
                void *replica = copy_meta_column(lfirst(lc));
                append_list(new_list, replica);
            }
            break;
        }
        case NODE_META_INDEX: {
            ListCell *lc;
            foreach (lc, old_list) {
                void *replica = copy_meta_index(lfirst(lc));
                append_list(new_list, replica);
            }
            break;
        }
        default:
            UNEXPECTED_VALUE("Not support this node to copy.");
    }
    return new_list;
}


/* Free all cells and objects that are 
 * point-to by cells in list DOES NOT be freed*/
void free_list(List *list) {
    if (list != NIL) {
        if (list->elements != list->initial_elements)
            dfree(list->elements);
        dfree(list);
    }
}

/* Free all cells and any object that are 
 * point-to by cells in list will be freed*/
void free_list_deep(List *list) {
    if (list != NIL) {
        switch (list->type) {
            case NODE_INT:
            case NODE_BOOL:
            case NODE_FLOAT:
            case NODE_DOUBLE:
            case NODE_LONG:
                break;
            case NODE_LIST: {
                ListCell *lc;
                foreach (lc, list) {
                    free_list_deep(lfirst(lc));
                }
                break;
            }
            case NODE_VOID: {
                ListCell *lc;
                foreach (lc, list) {
                    dfree(lfirst(lc));
                }
                break;
            }
            case NODE_STRING: {
                ListCell *lc;
                foreach (lc, list) {
                    dfree(lfirst(lc));
                }
                break;
            }
            case NODE_KEY_VALUE: {
                ListCell *lc;
                foreach (lc, list) {
                    free_key_value(lfirst(lc));
                }
                break;
            }
            case NODE_ROW: {
                ListCell *lc;
                foreach (lc, list) {
                    free_row(lfirst(lc));
                }
                break;
            }
            case NODE_REFER: {
                ListCell *lc;
                foreach (lc, list) {
                    free_refer(lfirst(lc));
                }
                break;
            }
            case NODE_COLUMN: {
                ListCell *lc;
                foreach (lc, list) {
                    free_column_node(lfirst(lc));
                }
                break;
            }
            case NODE_PAGE: {
                ListCell *lc;
                foreach (lc, list) {
                    void *page = lfirst(lc);
                    if (page)
                        dfree(page);
                }
                break;
            }
            case NODE_BUFFER_DESC: {
                ListCell *lc;
                foreach (lc, list) {
                    BufferDesc *buff_desc = lfirst(lc);
                    free_buffer_desc(buff_desc);
                }
                break;
            }
            case NODE_META_COLUMN: {
                ListCell *lc;
                foreach (lc, list) {
                    MetaColumn *meta_column = lfirst(lc);
                    free_meta_column(meta_column);
                }
                break;
            }
            case NODE_META_INDEX: {
                ListCell *lc;
                foreach (lc, list) {
                    free_meta_index(lfirst(lc));
                }
                break;
            }
            case NODE_TABLE: {
                ListCell *lc;
                foreach (lc, list) {
                    free_table(lfirst(lc));
                }
                break;
            }
            case NODE_STATEMENT: {
                ListCell *lc;
                foreach (lc, list) {
                    free_statement(lfirst(lc));
                }
                break;
            }
            case NODE_DB_RESULT: {
                ListCell *lc;
                foreach (lc, list) {
                    free_db_result(lfirst(lc));
                }
                break;
            }
            case NODE_COLUMN_DEF: {
                ListCell *lc;
                foreach (lc, list) {
                    free_column_def_node(lfirst(lc));
                }
                break;
            }
            case NODE_COLUMN_DEF_OPT: {
                ListCell *lc;
                foreach (lc, list) {
                    free_column_def_opt_node(lfirst(lc));
                }
                break;
            }
            case NODE_COLUMN_DEF_NAME: {
                ListCell *lc;
                foreach (lc, list) {
                    free_column_def_name(lfirst(lc));
                }
                break;
            }
            case NODE_SCALAR_EXP: {
                ListCell *lc;
                foreach (lc, list) {
                    free_scalar_exp_node(lfirst(lc));
                }
                break;
            }
            case NODE_VALUE_ITEM: {
                ListCell *lc;
                foreach (lc, list) {
                    free_value_item_node(lfirst(lc));
                }
                break;
            }
            case NODE_BASE_TABLE_ELEMENT: {
                ListCell *lc;
                foreach (lc, list) {
                    free_base_table_element_node(lfirst(lc));
                }
                break;
            }
            case NODE_TABLE_REFER: {
                ListCell *lc;
                foreach (lc, list) {
                    free_table_ref_node(lfirst(lc));
                }
                break;
            }
            case NODE_ASSIGNMENT: {
                ListCell *lc;
                foreach (lc, list) {
                    free_assignment_node(lfirst(lc));
                }
                break;
            }
            case NODE_TABLE_BUFFER_ENTRY: {
                ListCell *lc;
                foreach(lc, list) {
                    free_table_buffer_entry(lfirst(lc));
                }
                break;
            }
            case NODE_INTERNAL_NODE_CELL_DESC: {
                /* Fro InternalNodeCellDesc, only free itself, 
                 * not care about the key.*/
                ListCell *lc;
                foreach(lc, list) {
                    dfree(lfirst(lc));
                }
            }
        }

        if (list->elements != list->initial_elements)
            dfree(list->elements);

        dfree(list);
    }
}

