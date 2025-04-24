/*********************************************** Disk Mapping B+TREE Module ***************************************************
 * Auth:        JerryZhou
 * Created:     2023/08/23
 * Modify:      2024/12/04
 * Locataion:   src/store/ltree.c
 * Description: The ltree module is the core mudule for simpledb.
 * - A database file is divided into a whole number pages.
 * - A page includes meta data and cells.
 * - Meta data includes node type, is root leaf node, parent pointer and cell number.
 * - Cell is a Key-Value, key is index and value containes one row data in table.
 * - Simpledb use B-Tree as disk data structre.
 * - Each node correspond to one page.
 * - There are two types nodes, leaf node and internal node.
 * - Leaf node store real row data.
 * - Internal node store keys.
 * - Root node also as leaf node, but does not store data.
 * - Usually, root node is page 0.
 * - The prince: always keep visible row lie at the forefront of same key cells.
 * 
 * DATA STRUCTRURE:
 *
 *   (1) leaf cell structure:
 *   ---------------------------------------------------------------------------------------------------------------------
 *   | null_flag(8 bit)| array cap(32 bit, only exists for array) | col1 value| col2 value| col3 value|...  |  cell key  |
 *   ---------------------------------------------------------------------------------------------------------------------
 *   |                             cell value                                                               |  cell key  |
 *   ---------------------------------------------------------------------------------------------------------------------
 *
 *   (2) leaf node structure:                                                                                            
 *   ---------------------------------------------------------------------------------------------------------------------
 *   |                                                                                                                   |
 *   ---------------------------------------------------------------------------------------------------------------------
 *   |                                                                                                                   |
 *   ---------------------------------------------------------------------------------------------------------------------
 * 
 ****************************************************************************************************************************
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <time.h>
#include "ltree.h"
#include "bufpool.h"
#include "mmgr.h"
#include "copy.h"
#include "free.h"
#include "refer.h"
#include "common.h"
#include "asserts.h"
#include "const.h"
#include "pager.h"
#include "bufmgr.h"
#include "meta.h"
#include "compare.h"
#include "log.h"
#include "index.h"
#include "utils.h"
#include "qsort.h"
#include "table.h"

static void assign_row_value(void *destination, void *value, MetaColumn *meta_column);
static void insert_leaf_node_new_cell(Cursor *cursor, Row *row);
static void append_leaf_node_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos);
static bool check_internal_node_cells_mass(void *internal_node, uint32_t keys_num, uint32_t key_len, uint32_t value_len, DataType data_type);

/* If obsolute node. */
bool is_obsolute_node(void *node) {
    if (node == NULL) return false;
    uint8_t value  = *(uint8_t *)(node + NODE_STATE_SIZE_OFFSET);
    return (NodeState) value == OBSOLETE_STATE;
}

/* Get NodeState. */
NodeState get_node_state(void *node) {
    uint8_t value = *(uint8_t *) (node + NODE_STATE_SIZE_OFFSET);
    return (NodeState) value;
}

/* Set node state. */
void set_node_state(void *node, NodeState state) {
    uint8_t value = (uint8_t) state;
    *(uint8_t *) (node + NODE_STATE_SIZE_OFFSET) = value;
}

/* Get node type */
NodeType get_node_type(void *node) {
    uint8_t value = *(uint8_t *) (node + NODE_TYPE_OFFSET);
    return (NodeType) value;
}

/* Set node type */
void set_node_type(void *node, NodeType node_type) {
    uint8_t value = (uint8_t) node_type;
    *(uint8_t *) (node + NODE_TYPE_OFFSET) = value;
}

/* If root node */
static inline bool is_root_node(void *node) {
    uint8_t value = *(uint8_t *) (node + IS_ROOT_OFFSET);
    return (bool) value;
}

/* Set if root node */
void set_root_node(void *node, bool if_root_node) {
    uint8_t value = (uint8_t) if_root_node;
    *(uint8_t *)(node + IS_ROOT_OFFSET) = value;
}

/* Get parent pointer */
static uint32_t get_parent_pointer(void *node) {
    return *(uint32_t *) (node + PARENT_POINTER_OFFSET);
}

/* Set parent pointer. */
static void set_parent_pointer(void *node, uint32_t value) {
    *(uint32_t *) (node + PARENT_POINTER_OFFSET) = value;
}

/* Get root node column size. */
uint32_t get_column_size(void *root_node) {
    return *(uint32_t *) (root_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET);
}

/* Set root node column size. */
void set_column_size(void *node, uint32_t value) {
    *(uint32_t *) (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET) = (uint32_t) value;
}

/* Get default value cell. */
void *get_default_value_cell(void *node) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size);
    } 
    panic("Try to fetch default value cell in non-root node.");
    return NULL;
}

/* Set default value cell. */
void set_default_value_cell(void *node, void *destination, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        memcpy(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
               ROOT_NODE_META_COLUMN_SIZE * column_size, 
               destination, default_value_len);
    } else 
        panic("Try to fetch default value cell in non-root node.");
}

/* Get leaf node header pointer*/
static void *get_leaf_node_header_pointer(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len);
    } else {
        return (node + CELL_NUM_OFFSET);
    }
}

/* Get leaf node cell number. */
uint32_t get_leaf_node_cell_num(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len);
    } else {
        return *(uint32_t *)(node + CELL_NUM_OFFSET);
    }
}

/* Set leaf node cell number. */
static void set_leaf_node_cell_num(void *node, uint32_t default_value_len, uint32_t cell_num) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len) = cell_num;
    } else {
        *(uint32_t *)(node + CELL_NUM_OFFSET) = cell_num;
    }
}

/* Increase the leaf node coll num. */
static void increase_leaf_node_cell_num(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        (*(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len))++;
    } else {
        (*(uint32_t *)(node + CELL_NUM_OFFSET))++;
    }
}

/* Decrease the leaf node coll num. */
static void decrease_leaf_node_cell_num(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        (*(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len))--;
    } else {
        (*(uint32_t *)(node + CELL_NUM_OFFSET))--;
    }
}

/* Get leaf node next leaf. */
static uint32_t get_leaf_node_next_leaf(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE);
    } else {
        return *(uint32_t *)(node + LEAF_NODE_NEXT_LEAF_OFFSET); 
    }
}

/* Set leaf node next leaf */
static void set_leaf_node_next_leaf(void *node, uint32_t default_value_len, uint32_t value) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + CELL_NUM_SIZE) = value;
    } else {
        *(uint32_t *)(node + LEAF_NODE_NEXT_LEAF_OFFSET) = value;
    }
}

/* Get leaf node cell. */
void *get_leaf_node_cell(void *node, uint32_t key_len, uint32_t value_len, uint32_t index) {
    uint32_t cell_len = key_len + value_len;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + value_len + CELL_NUM_SIZE + 
            LEAF_NODE_NEXT_LEAF_SIZE + cell_len * index); 
    } else {
        return (node + LEAF_NODE_HEAD_SIZE + cell_len * index);
    }
}

/* Get leaf node cell key.  */
void *get_leaf_node_cell_key(void *node, uint32_t index, uint32_t key_len, uint32_t value_len) {
    uint32_t cell_len = key_len + value_len;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + value_len + CELL_NUM_SIZE + 
                LEAF_NODE_NEXT_LEAF_SIZE + cell_len * index + value_len); 
    } else {
        return (node + LEAF_NODE_HEAD_SIZE + cell_len * index + value_len);
    }
}

/* Set leaf node cell key. */
void set_leaf_node_cell_key(void *node, uint32_t index, uint32_t key_len, uint32_t value_len, void *key) {
    uint32_t cell_len = key_len + value_len;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        memcpy(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + value_len + CELL_NUM_SIZE + 
                LEAF_NODE_NEXT_LEAF_SIZE + cell_len * index + value_len, 
                key, key_len);
    } else {
        memcpy(node + LEAF_NODE_HEAD_SIZE + cell_len * index + value_len, key, key_len);
    }
}


/* Get leaf node cell value pointer. */
void *get_leaf_node_cell_value(void *node, uint32_t key_len, uint32_t value_len, uint32_t index) {
    return get_leaf_node_cell(node, key_len, value_len, index);
}

/* Get internal node keys number. */
uint32_t get_internal_node_keys_num(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len);
    } else {
        return *(uint32_t *)(node + KEYS_NUM_OFFSET);
    }
}

/* Set internal node keys number. */
void set_internal_node_keys_num(void *node, uint32_t default_value_len, uint32_t num) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len) = num;
    } else {
        *(uint32_t *)(node + KEYS_NUM_OFFSET) = num;
    }
}

/* Get right child of internal node  */
uint32_t get_internal_node_right_child(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE);
    } else {
        return *(uint32_t *)(node + RIGHT_CHILD_OFFSET);
    }
}

/* Set right child page num of internal node. */
void set_internal_node_right_child(void *node, uint32_t default_value_len, uint32_t right_child_page_num) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE) = right_child_page_num;
    } else {
        *(uint32_t *)(node + RIGHT_CHILD_OFFSET) = right_child_page_num;
    }
}

/* Get internal node body pointer. */
static void *get_internal_node_body_pointer(void *node, uint32_t default_value_len) {
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len); 
    } else {
        return (node + KEYS_NUM_OFFSET);
    }
}

/* Get key in the node by index. */
void *get_internal_node_key(void *node, uint32_t index, uint32_t key_len, uint32_t default_value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + 
                RIGHT_CHILD_SIZE + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE); 
    } else {
        return (node + INTERNAL_NODE_CELL_OFFSET + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE);
    }
}

/* Set internal node key by index */
void set_internal_node_key(void *node, uint32_t index, void *key, uint32_t key_len, uint32_t default_value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        memcpy(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + 
                cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE, key, key_len);
    } else {
        memcpy(node + INTERNAL_NODE_CELL_OFFSET + cell_len * index + INTERNAL_NODE_CELL_CHILD_SIZE, key, key_len);
    }
}

/* Get child value in the node by index. */
uint32_t get_internal_node_child(void *node, uint32_t index, uint32_t key_len, uint32_t default_value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
            ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + 
            cell_len * index); 
    } else {
        return *(uint32_t *)(node + INTERNAL_NODE_CELL_OFFSET + cell_len * index);
    }
}

/* Set child value in the node by index */
void set_internal_node_child(void *node, uint32_t index, uint32_t page_num, uint32_t key_len, uint32_t default_value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        *(uint32_t *)(node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + 
                cell_len * index) = page_num;
    } else {
        *(uint32_t *)(node + INTERNAL_NODE_CELL_OFFSET + cell_len * index) = page_num;
    }
}

/* get internal node cell */
void *get_internal_node_cell(void *node, uint32_t index, uint32_t key_len, uint32_t default_value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (is_root_node(node)) {
        uint32_t column_size = get_column_size(node);
        return (node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + 
                ROOT_NODE_META_COLUMN_SIZE * column_size + default_value_len + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + 
                cell_len * index); 
    } else {
        return (node + INTERNAL_NODE_CELL_OFFSET + cell_len * index);
    }
}

/* Get internal node index of key */
uint32_t get_internal_node_key_index(void *node, void *key, uint32_t keys_num, uint32_t key_len, 
                                     uint32_t default_value_len , DataType key_data_type) {
    /* Binary search */
    uint32_t min_index = 0;
    uint32_t max_index = keys_num;
    while (min_index != max_index) {
        uint32_t index = (max_index + min_index) / 2;
        void* index_key = get_internal_node_key(node, index, key_len, default_value_len);
        if (greater_equal(index_key, key, key_data_type)) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    return min_index;
}

/* Get internal node child page num. */
uint32_t get_internal_node_cell_child_page_num(void *node, void *key, 
                                               uint32_t keys_num, uint32_t key_len, 
                                               uint32_t default_value_len, DataType key_data_type) {
    /* Binary search */
    uint32_t min_index = 0;
    uint32_t max_index = keys_num;
    while(min_index != max_index) {
        uint32_t index = (max_index + min_index) / 2;
        void *index_key = get_internal_node_key(node, index, key_len, default_value_len);
        /* Notice: Greate Equal opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (greater_equal(index_key, key, key_data_type)) 
            max_index = index;
        else 
            min_index = index + 1;
    }
    if (min_index > keys_num) 
        db_log(PANIC, "Tried to access child_num %d > num_keys %d.", 
               min_index, 
               keys_num);
    else if (min_index == keys_num) 
        return get_internal_node_right_child(node, default_value_len);
    else 
        return get_internal_node_child(node, min_index, key_len, default_value_len);
    
    return -1;
}

/* Get leaf node cell index, 
 * maybe the key not exist in the node,then return the bigger one. */
uint32_t get_leaf_node_cell_index(void *node, void *key, 
                                  uint32_t cell_num, uint32_t key_len, 
                                  uint32_t value_len, DataType key_data_type) {
    // binary search
    uint32_t min_index = 0;
    uint32_t max_index = cell_num;
    while (min_index != max_index) {
        uint32_t index = (max_index + min_index) / 2;
        void *key_at_index = get_leaf_node_cell_key(node, index, key_len, value_len);
        /* Notice: Greate Equal opreator is really import for store data, 
         * when keep the prince: always keep visible row lie at the forefront of same key cells. */
        if (greater_equal(key_at_index, key, key_data_type)) {
            max_index = index;
        } else {
            min_index = index + 1; 
        }
    }
    return min_index;
}

/* Get index meta column pointer */
void *get_meta_column_pointer(void *root_node, uint32_t index) {
    return root_node + ROOT_NODE_META_COLUMN_OFFSET + ROOT_NODE_META_COLUMN_SIZE * index;
}

/* Get meta column size. */
uint32_t get_root_node_meta_column_size() {
    return ROOT_NODE_META_COLUMN_SIZE;
}

/* Set index meta column */
void set_meta_column(void *root_node, void *destination, uint32_t index) {
   memcpy(root_node + ROOT_NODE_META_COLUMN_OFFSET + ROOT_NODE_META_COLUMN_SIZE * index, 
          destination, 
          ROOT_NODE_META_COLUMN_SIZE);
}

/* Get absolute max key.
 * --------------------
 * Absolute max key is the max child key. 
 * For leaf node, the max key child is the last child; 
 * For internal node, the max child exists in the right child.
 * */
static void *get_max_key(Table *table, void *node, 
                         uint32_t key_len, uint32_t value_len) {
    switch (get_node_type(node)) {
        case INTERNAL_NODE: {
            uint32_t right_child_page_num = get_internal_node_right_child(node, value_len);
            Buffer buffer = ReadBuffer(GET_TABLE_OID(table), right_child_page_num);
            void *right_child = GetBufferPage(buffer);
            void *right_child_max_value = get_max_key(table, right_child, key_len, value_len);
            ReleaseBuffer(buffer);
            return right_child_max_value;
        }
        case LEAF_NODE: {
            uint32_t cell_num = get_leaf_node_cell_num(node, value_len);
            return get_leaf_node_cell_key(node, cell_num - 1, key_len, value_len);
        }
        default:
            UNEXPECTED_VALUE(get_node_type(node));
            return NULL;
    }
}

/* Initialize leaf node. */
void initial_leaf_node(void *leaf_node, uint32_t default_value_len, bool is_root) {
    set_node_type(leaf_node, LEAF_NODE);
    set_root_node(leaf_node, is_root);
    set_leaf_node_cell_num(leaf_node, default_value_len, 0);
    /* 'next leaf = 0' means no subling leaf node. */
    set_leaf_node_next_leaf(leaf_node, default_value_len, 0); 
}

/* Initialize internal node. */
void initial_internal_node(void *internal_node, bool is_root) {
    set_node_type(internal_node, INTERNAL_NODE);
    set_root_node(internal_node, is_root);
}

/* Update internal node node key. */
static void update_internal_node_key(Table *table, uint32_t page_num, void *old_key, void *new_key, 
                                     uint32_t key_len, uint32_t value_len, DataType key_data_type) {
    Buffer buffer;
    uint32_t keys_num;
    void *internal_node;
    void *max_key, *absolute_max_key;

    buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    LockBuffer(buffer, RW_READERS);
    internal_node = GetBufferPage(buffer);
    keys_num = get_internal_node_keys_num(internal_node, value_len);  
    Assert(keys_num > 0);

    /* Get max key and absolute max key in internal node cell. */
    max_key = get_internal_node_key(internal_node, keys_num - 1, key_len, value_len);
    absolute_max_key = get_max_key(table, internal_node, key_len, value_len);
    
    /* If old key greater than the max key, it means it exist in the right child node. 
     * No need to change cells key. Otherwise, it means the key in the cells, 
     * need to be replaced with new one. */
    if (!greater(old_key, max_key, key_data_type)) {
        UpgradeLockBuffer(buffer);
        uint32_t key_index = get_internal_node_key_index(internal_node, old_key, keys_num, key_len, value_len, key_data_type);
        void *key = get_internal_node_key(internal_node, key_index, key_len, value_len);

        /* Theoretically equal, just for check.*/
        if (!equal(old_key, key, key_data_type))
            Assert(equal(old_key, key, key_data_type));
        set_internal_node_key(internal_node, key_index, new_key, key_len, value_len);
        MakeBufferDirty(buffer);
        DowngradeLockBuffer(buffer);
    }
    
    /* If internal has parent node, and change its absolute max key, 
     * also need to change its parent key. */
    if (!is_root_node(internal_node) && 
            (equal(old_key, absolute_max_key, key_data_type) || 
                equal(new_key, absolute_max_key, key_data_type))) { 
        /* Get parent node buffer and lock it. */
        uint32_t parent_page_num = get_parent_pointer(internal_node);
        update_internal_node_key(table, parent_page_num, old_key, new_key, key_len, value_len, key_data_type);
    }
        
    /* Unlock Buffer. */
    UnlockBuffer(buffer);
    /* Relase Buffer. */
    ReleaseBuffer(buffer);
}

/* Check if leaf node page overflow. */
static bool overflow_leaf_node(void *leaf_node, uint32_t key_len, 
                               uint32_t value_len, uint32_t cell_num) {
    uint32_t cell_len = key_len + value_len;
    if (is_root_node(leaf_node)) {
        uint32_t column_size = get_column_size(leaf_node);
        return COMMON_NODE_HEADER_SIZE + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size + 
               value_len + CELL_NUM_SIZE + LEAF_NODE_NEXT_LEAF_SIZE + cell_len * (cell_num + 1) > PAGE_SIZE;
    } else {
        return LEAF_NODE_HEAD_SIZE + cell_len *(cell_num + 1) > PAGE_SIZE;
    }
}

/* Check if internal node page overflow */
static bool overflow_internal_node(void *internal_node, uint32_t keys_num, 
                                   uint32_t key_len, uint32_t value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    if (is_root_node(internal_node)) {
        uint32_t column_size = get_column_size(internal_node);
        return COMMON_NODE_HEADER_SIZE + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size + 
               value_len + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + cell_len * (keys_num + 1) > PAGE_SIZE;
    } else {
        return INTERNAL_NODE_HEAD_SIZE + cell_len * (keys_num + 1) > PAGE_SIZE;
    }
}

/* Redefine parent page number.
 * When a internal node page number changed, if it has children nodes, 
 * we also need to redefine parent page number in the children nodes. */
static void redefine_parent(Table *table, uint32_t page_num) {
    /* Read buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *internal_node = GetBufferPage(buffer);

    uint32_t key_len, value_len, keys_num, right_child_page_num;
    key_len = calc_primary_key_length(table);
    value_len = calc_table_row_length(table);
    keys_num = get_internal_node_keys_num(internal_node, value_len);

    /* Redefine each child node parent page. */
    for (int i = 0; i < keys_num; i++) {
        uint32_t child_page_num = get_internal_node_child(internal_node, i, key_len, value_len);
        Buffer child_buffer = ReadBuffer(GET_TABLE_OID(table), child_page_num);
        void *child_node = GetBufferPage(child_buffer);
        set_parent_pointer(child_node, page_num); 
        MakeBufferDirty(child_buffer);
        ReleaseBuffer(child_buffer);
    }

    /* Don`t forget the right child node. */
    right_child_page_num = get_internal_node_right_child(internal_node, value_len);

    /* Reader right buffer. */
    Buffer right_child_buffer = ReadBuffer(GET_TABLE_OID(table), right_child_page_num);
    void *right_node = GetBufferPage(right_child_buffer);
    set_parent_pointer(right_node, page_num);
    MakeBufferDirty(right_child_buffer);

    /* Release buffer. */
    ReleaseBuffer(right_child_buffer);
    ReleaseBuffer(buffer);
}

/* Check internal node cells mass. */
static bool check_internal_node_cells_mass(void *internal_node, uint32_t keys_num, uint32_t key_len, uint32_t value_len, DataType data_type) {
    void *before = NULL;
    for (uint32_t i = 0; i < keys_num; i++) {
        void *key = get_internal_node_key(internal_node, i, key_len, value_len);
        if (before) {
            if (greater(before, key, data_type))
                return true;
        }
        before = key;
    }
    return false;
}

/* The comparator of InternalNodeCellEntry. */
static inline int internal_node_cell_entry_comparator(const ListCell *lc1, const ListCell *lc2) {
    InternalNodeCellEntry *entry1 = (InternalNodeCellEntry *)lfirst(lc1);
    InternalNodeCellEntry *entry2 = (InternalNodeCellEntry *)lfirst(lc2);
    return compare(entry1->key, entry2->key, entry1->key_type);
}

/* Resort the internal node cells. 
 * Sometimes, mass of internal node cells may happer when children splitting.
 * It's necessary to resort internal node cells.
 * */
void resort_internal_node_cells(Table *table, uint32_t page_num, uint32_t key_len, uint32_t value_len, DataType data_type) {
    uint32_t keys_num, index;
    Buffer buffer;
    void *internal_node;
    List *list;
    
    buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    internal_node = GetBufferPage(buffer);
    keys_num = get_internal_node_keys_num(internal_node, value_len);

    if (!check_internal_node_cells_mass(internal_node, keys_num, key_len, value_len, data_type)) {
        ReleaseBuffer(buffer);
        return;
    }

    list = create_list(NODE_VOID);

    /* Append internal node cells. */
    for (index = 0; index < keys_num; index++) {
        void *key = get_internal_node_key(internal_node, index, key_len, value_len);
        uint32_t value = get_internal_node_child(internal_node, index, key_len, value_len);
        InternalNodeCellEntry *entry = instance(InternalNodeCellEntry);
        entry->key = key;
        entry->value = value;
        entry->key_type = data_type;
        append_list(list, entry);
    }
    
    /* Append internal node right cell. */
    {
        void *right_key = get_max_key(table, internal_node,  key_len, value_len);
        uint32_t right_value = get_internal_node_right_child(internal_node, value_len);
        InternalNodeCellEntry *entry = instance(InternalNodeCellEntry);
        entry->key = right_key;
        entry->value = right_value;
        entry->key_type = data_type;
        append_list(list, entry);
    }

    /* Resort*/
    list_qsort(list, internal_node_cell_entry_comparator);

    /* Reset into internal node cells. */
    for (index = 0; index < keys_num; index++) {
        ListCell *cell = list_nth_cell(list, index);
        InternalNodeCellEntry *entry = (InternalNodeCellEntry *) lfirst(cell);
        set_internal_node_key(internal_node, index, entry->key, key_len, value_len);
        set_internal_node_child(internal_node, index, entry->value, key_len, value_len);
    }

    /* Reset into internal node right cell. */
    {
        ListCell *cell = last_cell(list);
        InternalNodeCellEntry *entry = (InternalNodeCellEntry *) lfirst(cell);
        set_internal_node_right_child(internal_node, value_len, entry->value);
    }
    
    /* Free list. */
    free_list(list);

    /* Make page dirty. */
    MakeBufferDirty(buffer);
    ReleaseBuffer(buffer);
}


/* Covnert root node to leaf node */
static void copy_root_to_leaf_node(Table *table, uint32_t new_page_num, 
                                   uint32_t key_len, uint32_t value_len) {
    /* Read the root buffer. */
    Buffer rbuffer = ReadBuffer(GET_TABLE_OID(table), table->root_page_num);
    void *root = GetBufferPage(rbuffer);
    Oid oid = GET_TABLE_OID(table);

    /* Read the new buffer. */
    Buffer nbuffer = ReadBuffer(oid, new_page_num);
    void *leaf_node = GetBufferPage(nbuffer);

    initial_leaf_node(leaf_node, value_len, false);
    set_leaf_node_cell_num(leaf_node, value_len, get_leaf_node_cell_num(root, value_len));
    set_leaf_node_next_leaf(leaf_node, value_len, get_leaf_node_next_leaf(root, value_len));
    uint32_t cell_num = get_leaf_node_cell_num(root, value_len); 

    uint32_t i;
    for (i = 0; i < cell_num; i++) {
        set_leaf_node_cell_key(
            leaf_node, i, 
            key_len, value_len, 
            get_leaf_node_cell_key(root, i, key_len, value_len)
        ); 
        memcpy(
            get_leaf_node_cell_value(leaf_node, key_len, value_len, i), 
            get_leaf_node_cell_value(root, key_len, value_len, i), 
            value_len
        );
        /* Update refer. */
        update_refer(oid, table->root_page_num, i, new_page_num, i);
    }

    /* Make clear outsides header. */
    uint32_t column_size = get_column_size(root);
    uint32_t ROOT_LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + ROOT_NODE_META_COLUMN_SIZE_SIZE 
                                        + ROOT_NODE_META_COLUMN_SIZE * column_size;
    memset(root + ROOT_LEAF_NODE_HEADER_SIZE, 0, PAGE_SIZE - ROOT_LEAF_NODE_HEADER_SIZE);

    /* Release buffer. */
    ReleaseBuffer(nbuffer);
    ReleaseBuffer(rbuffer);
}

/* Copy root to an internal node. */
static void copy_root_to_internal_node(void *root, void *internal_node, 
                                       uint32_t key_len, uint32_t value_len) {
    /* Copy header. */
    initial_internal_node(internal_node, false);
    set_internal_node_keys_num(internal_node, value_len, get_internal_node_keys_num(root, value_len));
    set_internal_node_right_child(internal_node, value_len, get_internal_node_right_child(root, value_len));
    uint32_t keys_num = get_internal_node_keys_num(root, value_len);
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;

    /* Copy Body. */
    for (int i = 0; i < keys_num; i++) {
        memcpy(
            get_internal_node_cell(internal_node, i, key_len, value_len), 
            get_internal_node_cell(root, i, key_len, value_len), 
            cell_len
        );
    }
}

/* Create new root node. */
static void create_new_root_node(Table *table, uint32_t right_child_page_num, 
                                 uint32_t key_len, uint32_t value_len) {
    /* Get buffers. */
    Buffer root_buffer = ReadBuffer(GET_TABLE_OID(table), table->root_page_num);
    Buffer right_buffer = ReadBuffer(GET_TABLE_OID(table), right_child_page_num);
    void *root = GetBufferPage(root_buffer);
    void *right_child = GetBufferPage(right_buffer);

    /* Notice that, current next unused page num is not right child page num. 
     * The pager size has increased. */
    uint32_t next_unused_page_num = GetNextUnusedPageNum(table);
    Buffer left_buffer = ReadBuffer(GET_TABLE_OID(table), next_unused_page_num);

    /* Keep old root, generate a new leaf (or internal) node, 
     * and copy old root data to the new one. */
    void *left_child = GetBufferPage(left_buffer);

    NodeType node_type = get_node_type(root);
    switch(node_type) {
        case LEAF_NODE:
            copy_root_to_leaf_node(table, next_unused_page_num, key_len, value_len);
            break;
        case INTERNAL_NODE:
            copy_root_to_internal_node(root, left_child, key_len, value_len);
            redefine_parent(table, next_unused_page_num);
            break;
        default:
            UNEXPECTED_VALUE(node_type);
            break;
    }

    set_parent_pointer(left_child, table->root_page_num);
    set_parent_pointer(right_child, table->root_page_num);

    /* Root node now is internal node. */
    set_node_type(root, INTERNAL_NODE);
    set_internal_node_keys_num(root, value_len, 1);

    /* Register the new (leaf) leaf node to root. */
    void *max_key = get_max_key(table, left_child, key_len, value_len);
    set_internal_node_key(root, 0, max_key, key_len, value_len);
    set_internal_node_child(root, 0, next_unused_page_num, key_len, value_len);
    set_internal_node_right_child(root, value_len, right_child_page_num);

    MakeBufferDirty(left_buffer);
    MakeBufferDirty(right_buffer);
    MakeBufferDirty(root_buffer);

    /* Release buffer. */
    ReleaseBuffer(left_buffer);
    ReleaseBuffer(right_buffer);
    ReleaseBuffer(root_buffer);
}

/* When internal node is full, there is a need to generate a new internal node. 
 * And half hight cells in the old internal node will be moved into the new one. */
static void insert_and_split_internal_node(Table *table, uint32_t old_internal_page_num, 
                                           uint32_t new_child_page_num) {
    Buffer old_buffer, new_buffer;
    void *old_internal_node, *new_internal_node;
    uint32_t keys_num, next_unused_page_num, key_len, value_len, cell_len;

    /* Get old internal node. */
    old_buffer = ReadBuffer(GET_TABLE_OID(table), old_internal_page_num);
    old_internal_node = GetBufferPage(old_buffer);

    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    keys_num = get_internal_node_keys_num(old_internal_node, value_len);
    cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;

    MetaColumn *primary_key_meta_column = get_primary_key_meta_column(table->meta_table);
    
    /* Get new internal node. */
    next_unused_page_num = GetNextUnusedPageNum(table);
    new_buffer = ReadBuffer(GET_TABLE_OID(table), next_unused_page_num);
    new_internal_node = GetBufferPage(new_buffer);

    initial_internal_node(new_internal_node, false);
    set_parent_pointer(new_internal_node, get_parent_pointer(old_internal_node));
    set_internal_node_keys_num(new_internal_node, value_len, 0);
    set_internal_node_right_child(new_internal_node, value_len, 0);
    
    /* Get the new child node. */
    Buffer new_child_buffer = ReadBuffer(GET_TABLE_OID(table), new_child_page_num);
    void *new_child_node = GetBufferPage(new_child_buffer);

    /* Get the new child node max key and position in parent node. */
    void *new_child_max_key = get_max_key(table, new_child_node, key_len, value_len);
    uint32_t new_child_max_key_index = get_internal_node_key_index(
        old_internal_node, new_child_max_key, 
        keys_num, key_len, value_len, 
        primary_key_meta_column->column_type
    );

    uint32_t old_right_page_num = get_internal_node_right_child(old_internal_node, value_len);
    Buffer right_buffer = ReadBuffer(GET_TABLE_OID(table), old_right_page_num);
    void *right_node = GetBufferPage(right_buffer);
    void *right_node_max_key = get_max_key(table, right_node, key_len, value_len);

    /* Maybe the new child is greater than max key, need to compare. */
    if (greater(new_child_max_key, right_node_max_key, primary_key_meta_column->column_type)) {
        /* If yes, replace the new child with the origin old right child. */ 
        set_internal_node_right_child(old_internal_node, value_len, new_child_page_num);
        if (!is_root_node(old_internal_node)) {
            uint32_t parent_page = get_parent_pointer(old_internal_node);
            update_internal_node_key(
                table, parent_page, 
                right_node_max_key, new_child_max_key, 
                key_len, value_len, primary_key_meta_column->column_type
            );
        }
        new_child_max_key = right_node_max_key;
        new_child_max_key_index = keys_num; 
        new_child_page_num = old_right_page_num;
    }

    /* Get internal node absolute max key.*/
    void *old_max_key = get_max_key(table, old_internal_node, key_len, value_len);

    uint32_t RIGHT_SPLIT_COUNT = (keys_num + 1) / 2;
    uint32_t LEFT_SPLIT_COUNT = (keys_num + 1) - RIGHT_SPLIT_COUNT;

    int i;
    for (i = keys_num; i >= 0; i--) {
        /* Define which node. */ 
        void *destination_node = (i >= LEFT_SPLIT_COUNT)
                    ? new_internal_node 
                    : old_internal_node;
        
        /* New position. */
        uint32_t index = i % LEFT_SPLIT_COUNT;

        if (i == new_child_max_key_index) {
            set_internal_node_key(destination_node, index, new_child_max_key, key_len, value_len); 
            set_internal_node_child(destination_node, index, new_child_page_num, key_len, value_len);
        } else if (i > new_child_max_key_index) {
            /* Right cells make cell space. */
            memcpy(get_internal_node_cell(destination_node, index, key_len, value_len), 
                   get_internal_node_cell(old_internal_node, i - 1, key_len, value_len), 
                   cell_len);
        } else {
            memcpy(get_internal_node_cell(destination_node, index, key_len, value_len), 
                   get_internal_node_cell(old_internal_node, i, key_len, value_len), 
                   cell_len);
        }
    }

    /* For new internal node, it uses old internall node right node as right node. */
    set_internal_node_keys_num(new_internal_node, value_len, RIGHT_SPLIT_COUNT);
    set_internal_node_right_child(
        new_internal_node, 
        value_len, 
        get_internal_node_right_child(old_internal_node, value_len)
    );

    /* Rest parent for new internal node.*/
    redefine_parent(table, next_unused_page_num);

    /* For old internal node, it uses its last cell as it right child. */
    set_internal_node_keys_num(old_internal_node, value_len, LEFT_SPLIT_COUNT);
    set_internal_node_right_child(
        old_internal_node, 
        value_len, 
        get_internal_node_child(old_internal_node, LEFT_SPLIT_COUNT - 1, key_len, value_len)
    );
    set_internal_node_keys_num(old_internal_node, value_len, LEFT_SPLIT_COUNT - 1);

    redefine_parent(table, old_internal_page_num);

    /* If old internal is root, need to create new root. */
    if (is_root_node(old_internal_node)) 
        create_new_root_node(table, next_unused_page_num, key_len, value_len);
    else {
        /* Otherwise, it`s a normal internal node. 
         * -----------------------------------
         * Maybe the max key change, need update max key in parent internal node. 
         * Note that: because of new_max_key more likely less than the old_max_key,
         * so mass of parent internal node cells may be happer.
         * We'll resort parent internal node cells lately.
         * */
        uint32_t parent_page_num = get_parent_pointer(old_internal_node);
        void *new_max_key = get_max_key(table, old_internal_node, key_len, value_len);
        update_internal_node_key(
            table, parent_page_num, 
            old_max_key, new_max_key, 
            key_len, value_len, primary_key_meta_column->column_type
        );

        /* And insert a new cell about the new leaf node to the parent internal node. */
        insert_internal_node_cell(table, parent_page_num, next_unused_page_num, key_len, value_len);

        /* Parent internla node cells may mass, so resort the cells. */
        resort_internal_node_cells(table, parent_page_num, key_len, value_len, primary_key_meta_column->column_type);

        MakeBufferDirty(old_buffer);
        MakeBufferDirty(new_buffer);
    }

    /* Release the old right page buffer.*/
    ReleaseBuffer(old_buffer);
    ReleaseBuffer(new_buffer);
    ReleaseBuffer(new_child_buffer);
    ReleaseBuffer(right_buffer);
}

/* Insert new internal node cell. */
void insert_internal_node_cell(Table *table, uint32_t page_num, uint32_t new_child_page_num, 
                               uint32_t key_len, uint32_t value_len) {
    /* Get buffer. */
    Oid oid = GET_TABLE_OID(table);
    Buffer buffer = ReadBuffer(oid, page_num);
    Buffer new_child_buffer = ReadBuffer(oid, new_child_page_num);
    LockBuffer(buffer, RW_READERS);
    LockBuffer(new_child_buffer, RW_READERS);
    void *internal_node = GetBufferPage(buffer);
    void *new_child_node = GetBufferPage(new_child_buffer);

    /* Get primary key column meta info. */
    MetaColumn *primary_key_meta_column = get_primary_key_meta_column(table->meta_table);
    uint32_t keys_num = get_internal_node_keys_num(internal_node, value_len);

    /* Upgrade buffer lock to RW_WRITE. */
    UpgradeLockBuffer(buffer);

    /* Check if overflow after inserting.*/
    if (overflow_internal_node(internal_node, keys_num, key_len, value_len)) 
        insert_and_split_internal_node(table, page_num, new_child_page_num);
    else 
    {
        /* Get new child node max key and position in parent node. */
        void *new_child_max_key = get_max_key(table, new_child_node, key_len, value_len);

        /* Get right child node and right child node max key. */
        uint32_t right_child_page_num = get_internal_node_right_child(internal_node, value_len);
        Buffer right_child_buffer = ReadBuffer(oid, right_child_page_num);
        void *right_child = GetBufferPage(right_child_buffer);
        void *right_child_max_key = get_max_key(table, right_child, key_len, value_len);

        /* Right child always is the node which has the maximum key. */
        if (greater_equal(new_child_max_key, right_child_max_key, primary_key_meta_column->column_type)) {
            /* Replace old right child */
            set_internal_node_child(internal_node, keys_num, right_child_page_num, key_len, value_len);
            set_internal_node_key(internal_node, keys_num, right_child_max_key, key_len, value_len);
            if (!is_root_node(internal_node)) {
                uint32_t parent_page_num = get_parent_pointer(internal_node);
                update_internal_node_key(
                    table, parent_page_num, 
                    right_child_max_key, new_child_max_key, 
                    key_len, value_len, primary_key_meta_column->column_type
                );
            }
            set_internal_node_right_child(internal_node, value_len, new_child_page_num);
        } else {
            /* If not exist in the right child node, then exist in the cells. */ 
            uint32_t new_child_max_key_index = get_internal_node_key_index(
                internal_node, new_child_max_key, keys_num, 
                key_len, value_len, primary_key_meta_column->column_type
            );

            /* Check the default key if equals the inserting one. */
            void *default_key = get_internal_node_key(internal_node, new_child_max_key_index, key_len, value_len);
            if (equal(default_key, new_child_max_key, primary_key_meta_column->column_type)) {
                /* Move the right cells and make space for the new one. */
                int i;
                for (i = keys_num; i > new_child_max_key_index + 1; i--) {
                    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
                    memcpy(get_internal_node_cell(internal_node, i, key_len, value_len), 
                           get_internal_node_cell(internal_node, i - 1, key_len, value_len), 
                           cell_len); 
                } 
                set_internal_node_key(internal_node, new_child_max_key_index + 1, new_child_max_key, key_len, value_len);
                set_internal_node_child(internal_node, new_child_max_key_index + 1, new_child_page_num, key_len, value_len);
            } else {
                /* Move the right cells and make space for the new one. */
                int i;
                for (i = keys_num; i > new_child_max_key_index; i--) {
                    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
                    memcpy(get_internal_node_cell(internal_node, i, key_len, value_len), 
                           get_internal_node_cell(internal_node, i - 1, key_len, value_len), 
                           cell_len); 
                } 
                set_internal_node_key(internal_node, new_child_max_key_index, new_child_max_key, key_len, value_len);
                set_internal_node_child(internal_node,  new_child_max_key_index, new_child_page_num, key_len, value_len);
            }
        }

        /* Increase keys number. */
        set_internal_node_keys_num(internal_node, value_len, keys_num + 1);

        /* Flush disk. */
        MakeBufferDirty(buffer);

        /* Release right child buffer. */
        ReleaseBuffer(right_child_buffer);
    }

    /* Unlock buffer. */
    UnlockBuffer(new_child_buffer);
    UnlockBuffer(buffer);

    /* Release buffer. */
    ReleaseBuffer(new_child_buffer);
    ReleaseBuffer(buffer);
}

/* When page full, it will generate a new leaf node. 
 * And half high cell in the old leaf will be moved to new leaf node. */
static void insert_and_split_leaf_node(Cursor *cursor, Row *row) {
    /* Get cell key, value and cell lenght. */
    uint32_t key_len, value_len, cell_length;

    key_len = calc_primary_key_length(cursor->table);
    value_len = calc_table_row_length(cursor->table);
    cell_length = key_len + value_len;
    
    Table *table = cursor->table;
    Oid oid = GET_TABLE_OID(table);

    /* Get table primary key meta info. */
    MetaColumn *primary_key_meta_column = get_primary_key_meta_column(cursor->table->meta_table);

    /* Get the old leaf node and lock. */
    Buffer old_buffer = ReadBuffer(oid, cursor->page_num);
    void *old_node = GetBufferPage(old_buffer);
    uint32_t parent_page_num = get_parent_pointer(old_node);
    /* Get the old leaf node cell count.*/
    uint32_t cell_num = get_leaf_node_cell_num(old_node, value_len);

    /* Double check for concurrency. */
    if (!overflow_leaf_node(old_node, key_len, value_len, cell_num)) {
        ReleaseBuffer(old_buffer);
        insert_leaf_node_new_cell(cursor, row);
        return;
    }

    /* Upgrade old buffer lock to RW_WRITE. */
    UpgradeLockBuffer(old_buffer);

    /* Get the old leaf node max key. */
    void *old_max_key = get_max_key(table, old_node, key_len, value_len);

    /* Page num start with zero, so the page size is the next page num. */
    uint32_t next_unused_page_num = GetNextUnusedPageNum(cursor->table);

    /* Get new leaf node, if not exists, pager will generate a new one. */
    Buffer new_buffer = ReadBuffer(oid, next_unused_page_num);
    void *new_node = GetBufferPage(new_buffer);
    initial_leaf_node(new_node, value_len, false);

    /* Both of old leaf node and new leaf node have same parent internal node. */
    set_parent_pointer(new_node, parent_page_num);
    set_leaf_node_next_leaf(new_node, value_len, get_leaf_node_next_leaf(old_node, value_len));
    set_leaf_node_next_leaf(old_node, value_len, next_unused_page_num);

    /* All existing keys plus new key should should be divided 
     * evenly between old (left) and new (right) nodes.
     * Starting from the right, move each key to correct position. */
    uint32_t RIGHT_SPLIT_COUNT = (cell_num + 1) / 2;
    uint32_t LEFT_SPLIT_COUNT = (cell_num + 1) - RIGHT_SPLIT_COUNT;

    /* Notice, cant make i uint32_t when i decrease, 
     * when i = 0 and decrease, it still satisfy i >= 0. */
    int i; 
    for (i = cell_num; i >= 0; i--) {
        /* If index greater than LEAF_SPLIT_COUNT, destination is new old, 
         * othersize, stay in the old node. */
        void *destination_node = (i >= LEFT_SPLIT_COUNT) 
                            ? new_node 
                            : old_node;
        uint32_t destination_page = (i >= LEFT_SPLIT_COUNT) 
                            ? next_unused_page_num 
                            : cursor->page_num;

        /* New position. */
        uint32_t index_at_node = i % LEFT_SPLIT_COUNT;
        void *destination = get_leaf_node_cell(destination_node, key_len, value_len, index_at_node);

        /* The cursor rigth cells should move one cell to the right to make space for the cursor, 
         * include the cell having the old same num as cursor. The cursor leaf cells don`t need to make space.
         * Because i start with cell number and decrease, right cells firstly move and make space. */
        if (i == cursor->cell_num) {
            /* Deposit cursor. */
            void *serial_data = serialize_row_data(row, cursor->table);
            memcpy(destination, serial_data, value_len);
            set_leaf_node_cell_key(destination_node, index_at_node, key_len, value_len, row->key);
            dfree(serial_data);
        } else if (i > cursor->cell_num) {
            /* Define new position, and right cells make cell space. */
            memcpy(destination, get_leaf_node_cell(old_node, key_len, value_len, i - 1), cell_length);
            /* Update refer. */
            update_refer(oid, cursor->page_num, i - 1, destination_page, index_at_node);
        } else {
            /* Define new position. */
            memcpy(destination, get_leaf_node_cell(old_node, key_len, value_len, i), cell_length);
            /* Update refer. */
            update_refer(oid, cursor->page_num, i, destination_page, index_at_node);
        }
    }

    /* Set cell num */
    set_leaf_node_cell_num(old_node, value_len, LEFT_SPLIT_COUNT);
    set_leaf_node_cell_num(new_node, value_len, RIGHT_SPLIT_COUNT);
    set_leaf_node_next_leaf(old_node, value_len, next_unused_page_num);

    /* If the old node is root, need to creat new root node. */
    if (is_root_node(old_node)) 
        create_new_root_node(cursor->table, next_unused_page_num, key_len, value_len); 
    else {
        /* Otherwise, it`s a normal leaf node. 
         * -----------------------------------
         * Maybe the max key change, need update max key in parent internal node. 
         * Note that: because of new_max_key more likely less than the old_max_key,
         * so mass of parent internal node cells may be happen.
         * We'll resort parent internal node cells lately.
         * */
        void *new_max_key = get_max_key(cursor->table, old_node, key_len, value_len);
        update_internal_node_key(
            cursor->table, parent_page_num, 
            old_max_key, new_max_key, 
            key_len, value_len, primary_key_meta_column->column_type
        );
        
        /* And insert a new cell about the new 
         * leaf node to the parent internal node. */
        insert_internal_node_cell(cursor->table, parent_page_num, next_unused_page_num, key_len, value_len);

        /* Parent internla node cells may mass, so resort the cells. */
        resort_internal_node_cells(cursor->table, parent_page_num, key_len, value_len, primary_key_meta_column->column_type);

        /* Dirty pages. */
        MakeBufferDirty(old_buffer);
        MakeBufferDirty(new_buffer);
    }

    /* Release new page buffer. */
    ReleaseBuffer(new_buffer);
    ReleaseBuffer(old_buffer);
}


/* Insert leaf node a new cell. */
static void insert_leaf_node_new_cell(Cursor *cursor, Row *row) {
    uint32_t cell_num, value_len, key_len, cell_length;
    Table *table = cursor->table;
    Oid oid = GET_TABLE_OID(table);

    /* Get the node buffer. */
    Buffer buffer = ReadBuffer(oid, cursor->page_num);  
    void *node = GetBufferPage(buffer);
    
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    cell_num = get_leaf_node_cell_num(node, value_len);
    cell_length = value_len + key_len;

    if (cursor->cell_num < cell_num) {
        /* Upgrade lock buffer to RW_WRITE. */
        UpgradeLockBuffer(buffer);

        /* Make room for new cell. */
        int i;
        for (i = cell_num; i > cursor->cell_num; i--) {
            /* Movement. */
            memcpy(
                get_leaf_node_cell(node, key_len, value_len, i), 
                get_leaf_node_cell(node, key_len, value_len, i - 1), 
                cell_length
            );
            /* Update refer. */
            update_refer(oid, cursor->page_num, i - 1, cursor->page_num, i);
        }
        
        /* Downgrade lock to RW_READERS. */
        DowngradeLockBuffer(buffer);
    }
    
    /* Insert the new row. */
    set_leaf_node_cell_key(node, cursor->cell_num, key_len, value_len, row->key);
    void *destination = serialize_row_data(row, cursor->table);
    memcpy(get_leaf_node_cell_value(node, key_len, value_len, cursor->cell_num), destination, value_len);
    
    /* Check if the max key in leaf node has changed, that may impact the parent internal node. */
    if (!is_root_node(node) && cursor->cell_num == cell_num) {
        uint32_t parent_page_num = get_parent_pointer(node);
        void *old_max_key = get_leaf_node_cell_key(node, cell_num - 1, key_len, value_len);
        MetaColumn *primary_key_meta_column = get_primary_key_meta_column(cursor->table->meta_table);
        /* Logic check.*/
        Assert(greater_equal(row->key, old_max_key, primary_key_meta_column->column_type));

        /* Update internal node key. */
        update_internal_node_key(
            cursor->table, parent_page_num, 
            old_max_key, row->key, 
            key_len, value_len, primary_key_meta_column->column_type
        );
    }
    
    /* Cell number increases. */
    increase_leaf_node_cell_num(node, value_len);
    
    /* Flush into disk. */
    MakeBufferDirty(buffer);

    dfree(destination);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Insert a new cell in leaf node */
void insert_leaf_node_cell(Cursor *cursor, Row *row) {
    Buffer buffer;
    void *node;
    uint32_t cell_num, value_len, key_len;
    Table *table = cursor->table;

    /* Get the node buffer. */
    buffer = ReadBuffer(GET_TABLE_OID(table), cursor->page_num); 
    LockBuffer(buffer, RW_READERS);
    node = GetBufferPage(buffer); 
    
    /* Get data. */ 
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    cell_num = get_leaf_node_cell_num(node, value_len);

    /* Check if the leaf node overflow after inserting, 
     * If overflow, split the leaf node fist.*/
    if (overflow_leaf_node(node, key_len, value_len, cell_num)) 
        insert_and_split_leaf_node(cursor, row);
    else 
        insert_leaf_node_new_cell(cursor, row);

    /* Unlock the buffer. */
    UnlockBuffer(buffer);
    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Check if overflow after appending new column for leaf node. */
static bool overflow_leaf_node_new_column(void *leaf_node, MetaColumn *new_column, uint32_t key_len, uint32_t value_len) {
    uint32_t after_len;
    uint32_t cell_num, row_len;

    cell_num = get_leaf_node_cell_num(leaf_node, value_len);
    /* Row value lenght after adding new column. */
    value_len += new_column->column_length;
    row_len = key_len + value_len;
    if (is_root_node(leaf_node)) {
        /* Column size increases one. */
        uint32_t column_size = get_column_size(leaf_node) + 1;
        after_len = ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size + 
                    value_len + CELL_NUM_SIZE + LEAF_NODE_NEXT_LEAF_SIZE + row_len * cell_num;
    } else {
        after_len = LEAF_NODE_HEAD_SIZE + row_len * cell_num;
    }
    return after_len > PAGE_SIZE;
}

/* Check Root internal node if overflow. */
static bool overflow_root_internal_node_new_column(void *root_node, MetaColumn *new_column, uint32_t key_len, uint32_t value_len) {
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    uint32_t column_size = get_column_size(root_node);
    uint32_t keys_num = get_internal_node_keys_num(root_node, value_len);
    value_len += new_column->column_length;
    uint32_t after_len = COMMON_NODE_HEADER_SIZE + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * (column_size + 1) + 
                         value_len + KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + cell_len * keys_num;
    return after_len > PAGE_SIZE;
}

/* Get new cell pointer After appending column. */
static void *cell_pointer_after_append(void *leaf_node, MetaColumn *new_column, int index, uint32_t key_len, uint32_t value_len) {
    /* Row value lenght after adding new column. */
    value_len += new_column->column_length;
    uint32_t row_len = key_len + value_len;
    /* Column size increases one. */
    uint32_t column_size = get_column_size(leaf_node) + 1;
    if (is_root_node(leaf_node))
        return (leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size + 
                value_len + CELL_NUM_SIZE + LEAF_NODE_NEXT_LEAF_SIZE + row_len * index); 
    else
        return (leaf_node + LEAF_NODE_CELL_OFFSET + row_len * index);

}

/* Get default value pointer after appending column. */
static void *default_value_pointer_after_append(void *root_node) {
    /* Column size increases one. */
    uint32_t column_size = get_column_size(root_node) + 1;
    return (root_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size); 
}

/* Calcualte the offset where new column appending at. */
static uint32_t calc_offset_new_column(MetaTable *meta_table, int pos) {
    /*  Calcualte offset. */
    uint32_t offset = 0;
    for (int i = 0; i < pos; i++) {
        MetaColumn *current = meta_table->meta_column[i];
        offset += current->column_length;
    }
    return offset;
}

/* Generate new cell after appending column. */
static void *gen_new_cell_after_append_column(void *old_cell, MetaTable *meta_table, MetaColumn *new_meta_column, int pos) {

    Assert(pos > -1);

    uint32_t value_len = calc_table_row_length2(meta_table);
    uint32_t key_len = calc_primary_key_length2(meta_table);
    uint32_t cell_len = key_len + value_len;
    
    /* Get new column offset. */
    uint32_t offset = calc_offset_new_column(meta_table, pos);
    Assert(cell_len > offset);
    
    /* Move after offset memory. */
    memcpy(old_cell + offset + new_meta_column->column_length, 
           old_cell + offset, 
           cell_len - offset);

    /* Assign new column default value. */
    assign_row_value(old_cell + offset, 
                     new_meta_column->default_value, 
                     new_meta_column);

    return old_cell;
}

/* Genrate new default value. */
static void *gen_new_default_value_at_append_column(void *default_value, MetaTable *meta_table, MetaColumn *new_meta_column, int pos) {

    /* Make sure. */
    Assert(pos > -1);

    /* Get value len, offset. */
    uint32_t value_len, offset;
    value_len = calc_table_row_length2(meta_table);
    offset = calc_offset_new_column(meta_table, pos);

    /* Move after offset memory. */
    memmove(default_value + offset + new_meta_column->column_length,
           default_value + offset,
           value_len - offset);

    /* Assign new column default value. */
    switch (new_meta_column->default_value_type) {
        case DEFAULT_VALUE: {
            /* Maybe default value is null, when refer value not found any match row. */
            if (!is_null(new_meta_column->default_value))
                memcpy(default_value + offset, 
                       new_meta_column->default_value, 
                       new_meta_column->column_length);
            else
                memset(default_value + offset, 0, new_meta_column->column_length);
            break;
        }
        case DEFAULT_VALUE_NONE:
        case DEFAULT_VALUE_NULL:
            memset(default_value + offset, 0, new_meta_column->column_length);
            break;
    }

    return default_value;
}

/* Split leaf node. */
static void split_leaf_node_append_column(uint32_t page_num, Table *table, MetaColumn *new_column, uint32_t pos) {
    /* Get leaf node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *leaf_node = GetBufferPage(buffer);

    /* Get cell key, value and cell lenght. */
    uint32_t key_len, value_len, cell_length;
    key_len = calc_primary_key_length(table);
    value_len = calc_table_row_length(table);
    cell_length = key_len + value_len;
    
    MetaTable *meta_table = table->meta_table;

    /* Get table primary key meta info. */
    MetaColumn *primary_key_meta_column = get_primary_key_meta_column(meta_table);

    /* Get the old leaf node cell count.*/
    uint32_t cell_num = get_leaf_node_cell_num(leaf_node, value_len);

    /* Get the old leaf node max key. */
    void *old_max_key = get_max_key(table, leaf_node, key_len, value_len);

    /* Page num start with zero, so the page size is the next page num. */
    uint32_t next_unused_page_num = GetNextUnusedPageNum(table);

    /* Get new leaf node, if not exists, pager will generate a new one. */
    Buffer new_buffer = ReadBuffer(GET_TABLE_OID(table), next_unused_page_num);
    void *new_node = GetBufferPage(new_buffer);
    initial_leaf_node(new_node, value_len, false);

    /* Both of old leaf node and new leaf node have same parent internal node. */
    set_parent_pointer(new_node, get_parent_pointer(leaf_node));
    set_leaf_node_next_leaf(new_node, value_len, get_leaf_node_next_leaf(leaf_node, value_len));
    set_leaf_node_next_leaf(leaf_node, value_len, next_unused_page_num);

    /* All existing keys plus new key should should be divided 
     * evenly between old (left) and new (right) nodes.
     * Starting from the tail, move each key to correct position. */
    uint32_t RIGHT_SPLIT_COUNT = cell_num / 2;
    uint32_t LEFT_SPLIT_COUNT = cell_num  - RIGHT_SPLIT_COUNT;

    int i; 
    for (i = cell_num; i >= 0; i--) {
        /* If index greater than LEAF_SPLIT_COUNT, destination is new old, othersize, stay in the old node. */
        void *destination_node = i >= LEFT_SPLIT_COUNT ? new_node : leaf_node;
        uint32_t destination_page = i  >= LEFT_SPLIT_COUNT ? next_unused_page_num : page_num;

        /* New position. */
        uint32_t index_at_node = i % LEFT_SPLIT_COUNT;
        void *destination = get_leaf_node_cell(destination_node, key_len, value_len, index_at_node);

        /* Define new position. */
        memcpy(destination, get_leaf_node_cell(leaf_node, key_len, value_len, i), cell_length);
        /* Update refer. */
        update_refer(GET_TABLE_OID(table), page_num, i, destination_page, index_at_node);
    }

    /* Set cell num */
    set_leaf_node_cell_num(leaf_node, value_len, LEFT_SPLIT_COUNT);
    set_leaf_node_cell_num(new_node, value_len, RIGHT_SPLIT_COUNT);
    set_leaf_node_next_leaf(leaf_node, value_len, next_unused_page_num);

    /* If the old node is root, need to creat new root node. */
    if (is_root_node(leaf_node)) {
        create_new_root_node(table, next_unused_page_num, key_len, value_len); 
        append_new_column(table->root_page_num, table, new_column, pos);
    }
    else {
        /* append leaf node column. */
        append_leaf_node_column(page_num, table, new_column, pos);
        append_leaf_node_column(next_unused_page_num, table, new_column, pos);

        value_len += new_column->column_length;

        /* Otherwise, it`s a normal leaf node. 
         * Maybe the max key change, need update max key in parent internal node. */
        uint32_t parent_page_num = get_parent_pointer(leaf_node);
        void *new_max_key = get_max_key(table, leaf_node, key_len, value_len);

        /* Update internal node key. 
         * ------------------------
         * Note that: because of new_max_key more likely less than the old_max_key,
         * so mass of parent internal node cells may be happer.
         * We'll resort parent internal node cells lately.
         * */
        update_internal_node_key(
            table, parent_page_num, 
            old_max_key, 
            new_max_key, key_len, 
            value_len, primary_key_meta_column->column_type
        );
        
        /* And insert a new cell about the new leaf node to the parent internal node. */
        insert_internal_node_cell(table, parent_page_num, next_unused_page_num, key_len, value_len);

        /* Parent internla node cells may mass, so resort the cells. */
        resort_internal_node_cells(table, parent_page_num, key_len, value_len, primary_key_meta_column->column_type);

        MakeBufferDirty(buffer);
        MakeBufferDirty(new_buffer);
    }
    
    /* Release buffer. */
    ReleaseBuffer(new_buffer);
    ReleaseBuffer(buffer);
}

/* Split root internla node when appending column. */
static void split_root_internal_node_append_column(uint32_t page_num, Table *table, 
                                                   uint32_t key_len, uint32_t value_len) {

    uint32_t keys_num, next_unused_page_num, cell_len;
    Oid oid = GET_TABLE_OID(table);
    
    /* Get old internal node. */
    Buffer buffer = ReadBuffer(oid, page_num);
    void *old_internal_node = GetBufferPage(buffer);

    /* Get keys number, key length, value length. cell_len */
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    keys_num = get_internal_node_keys_num(old_internal_node, value_len);
    cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;

    /* Create new internal node and initialize. */
    next_unused_page_num = GetNextUnusedPageNum(table);
    Buffer new_buffer = ReadBuffer(oid, next_unused_page_num);
    void *new_internal_node = GetBufferPage(new_buffer);

    initial_internal_node(new_internal_node, false);
    set_parent_pointer(new_internal_node, get_parent_pointer(old_internal_node));
    set_internal_node_keys_num(new_internal_node, value_len, 0);
    set_internal_node_right_child(new_internal_node, value_len, 0);
    
    uint32_t RIGHT_SPLIT_COUNT = keys_num / 2;
    uint32_t LEFT_SPLIT_COUNT = keys_num - RIGHT_SPLIT_COUNT;

    int i;
    for (i = keys_num - 1; i >= 0; i--) {
        /* Define which node. */ 
        void *destination_node = (i >= LEFT_SPLIT_COUNT)
                ? new_internal_node 
                : old_internal_node;
        
        /* New position. */
        uint32_t index = i % LEFT_SPLIT_COUNT;

        /* Copy memory. */
        memcpy(
            get_internal_node_cell(destination_node, index, key_len, value_len), 
            get_internal_node_cell(old_internal_node, i, key_len, value_len), 
            cell_len
        );
    }

    /* For new internal node, it uses old internall node right node as right node. */
    set_internal_node_keys_num(new_internal_node, value_len, RIGHT_SPLIT_COUNT);
    set_internal_node_right_child(
        new_internal_node, 
        value_len, 
        get_internal_node_right_child(old_internal_node, value_len)
    );

    /* Rest parent for new internal node.*/
    redefine_parent(table, next_unused_page_num);

    /* For old internal node, it uses its last cell as it right child. */
    set_internal_node_keys_num(old_internal_node, value_len, LEFT_SPLIT_COUNT);
    set_internal_node_right_child(
        old_internal_node, 
        value_len, 
        get_internal_node_child(old_internal_node, LEFT_SPLIT_COUNT - 1, key_len, value_len)
    );
    set_internal_node_keys_num(old_internal_node, value_len, LEFT_SPLIT_COUNT - 1);

    redefine_parent(table, page_num);

    /* Flush page.*/
    MakeBufferDirty(buffer);

    /* Create new root. */
    create_new_root_node(table, next_unused_page_num, key_len, value_len);

    /* Release buffer. */
    ReleaseBuffer(new_buffer);
    ReleaseBuffer(buffer);
}

/* Append root leaf node new column. */
static void append_root_leaf_node_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos) {
    /* Get leaf node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *leaf_node = GetBufferPage(buffer);

    /* Just for check. */
    Assert(get_node_type(leaf_node) == LEAF_NODE);
    Assert(is_root_node(leaf_node));

    uint32_t value_len = calc_table_row_length(table);
    uint32_t key_len = calc_primary_key_length(table);
    uint32_t cell_len = key_len + value_len;
    
    MetaTable *meta_table = table->meta_table;
    void *destination = serialize_meta_column(new_column);

    /********************************************************************************************
     * When we appending new column, we need to adjust the Page cell from bottom to top.
     * Otherwise, the front cell will override the back cell.
     * So we first add default or NULL value in reserved order, then append new meta column info.
     ********************************************************************************************/

    /* Move leaf node body. */
    uint32_t cell_num = get_leaf_node_cell_num(leaf_node, value_len);
    for (int i = cell_num - 1; i >= 0; i--) {
        void *old_cell = get_leaf_node_cell(leaf_node, key_len, value_len, i);
        void *new_cell = gen_new_cell_after_append_column(old_cell, meta_table, new_column, pos);
        memmove(
            cell_pointer_after_append(leaf_node, new_column, i, key_len, value_len), 
            new_cell,
            cell_len + new_column->column_length
        );
    }

    /* Move leaf node header. */
    memmove(
        get_leaf_node_header_pointer(leaf_node, value_len) + ROOT_NODE_META_COLUMN_SIZE + new_column->column_length, 
        get_leaf_node_header_pointer(leaf_node, value_len), 
        CELL_NUM_SIZE + LEAF_NODE_NEXT_LEAF_SIZE
    );

    /* Move default value. */
    void *old_default_value = get_default_value_cell(leaf_node);
    void *new_default_value = gen_new_default_value_at_append_column(old_default_value, meta_table, new_column, pos);
    memmove(
        default_value_pointer_after_append(leaf_node), 
        new_default_value,
        value_len + new_column->column_length
    );

    /* Move meta column info. */
    uint32_t column_size = get_column_size(leaf_node);
    for (int i = column_size; i >= pos; i--) {
        if (i == pos) 
            memcpy(
                get_meta_column_pointer(leaf_node, i), 
                destination, 
                ROOT_NODE_META_COLUMN_SIZE
            );
        else if (i > pos) 
            memcpy(
                get_meta_column_pointer(leaf_node, i), 
                get_meta_column_pointer(leaf_node, i - 1), 
                ROOT_NODE_META_COLUMN_SIZE
            );
    }
    
    /* Increase column size. */
    set_column_size(leaf_node, column_size + 1);

    /* Make page dirty. */
    MakeBufferDirty(buffer);

    /* Release buffer. */
    ReleaseBuffer(buffer);
}


/* Append normal leaf node new column. */
static void append_normal_leaf_node_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos) {

    /* Get leaf page buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *leaf_node = GetBufferPage(buffer);

    /* Just for check. */
    Assert(get_node_type(leaf_node) == LEAF_NODE);
    Assert(!is_root_node(leaf_node));

    uint32_t value_len = calc_table_row_length(table);
    uint32_t key_len = calc_primary_key_length(table);
    uint32_t cell_len = key_len + value_len;
    
    MetaTable *meta_table = table->meta_table;

    /********************************************************************************************
     * When we appending new column, we need to adjust the Page cell from bottom to top.
     * Otherwise, the front cell will override the back cell.
     * So we first add default or NULL value in reserved order, then append new meta column info.
     ********************************************************************************************/

    /* Move leaf node body. */
    uint32_t cell_num = get_leaf_node_cell_num(leaf_node, value_len);
    int i;
    for (i = cell_num - 1; i >= 0; i--) {
        void *old_cell = get_leaf_node_cell(leaf_node, key_len, value_len, i);
        void *new_cell = gen_new_cell_after_append_column(old_cell, meta_table, new_column, pos);
        memmove(
            cell_pointer_after_append(leaf_node, new_column, i, key_len, value_len), 
            new_cell,
            cell_len + new_column->column_length
        );
    }

    /* Move leaf node header. */
    memmove(
        get_leaf_node_header_pointer(leaf_node, value_len) + ROOT_NODE_META_COLUMN_SIZE + new_column->column_length, 
        get_leaf_node_header_pointer(leaf_node, value_len), 
        CELL_NUM_SIZE + LEAF_NODE_NEXT_LEAF_SIZE
    );

    /* flush page. */
    MakeBufferDirty(buffer);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Append leaf node new column. */
static void append_leaf_node_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos) {

    /* Get leaf page buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *leaf_node = GetBufferPage(buffer);
    Assert(get_node_type(leaf_node) == LEAF_NODE);

    uint32_t value_len = calc_table_row_length(table);
    uint32_t key_len = calc_primary_key_length(table);
    
    if (overflow_leaf_node_new_column(leaf_node, new_column, key_len, value_len)) 
        split_leaf_node_append_column(page_num, table, new_column, pos);
    else if (is_root_node(leaf_node))
        append_root_leaf_node_column(page_num, table, new_column, pos);
    else 
        append_normal_leaf_node_column(page_num, table, new_column, pos);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Append new column for root internal node. */
static void append_root_internal_node_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos) {

    /* Get root node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *root_node = GetBufferPage(buffer);
    Assert(is_root_node(root_node));

    uint32_t key_len = calc_primary_key_length(table);
    uint32_t value_len = calc_table_row_length(table);
    
    void *destination = serialize_meta_column(new_column);
    if (overflow_root_internal_node_new_column(root_node, new_column, key_len, value_len)) {
        split_root_internal_node_append_column(page_num, table, key_len, value_len);
        append_new_column(table->root_page_num, table, new_column, pos);
    } else {
        /* Move body */
        uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
        uint32_t keys_num = get_internal_node_keys_num(root_node, value_len);

        void *body = get_internal_node_body_pointer(root_node, value_len);
        memmove(
            body + ROOT_NODE_META_COLUMN_SIZE + new_column->column_length,
            body,
            KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + cell_len * keys_num
        );

        /* Move default value. */
        void *old_default_value = get_default_value_cell(root_node);
        void *new_default_value = gen_new_default_value_at_append_column(old_default_value, table->meta_table, new_column, pos);
        memmove(
            default_value_pointer_after_append(root_node),
            new_default_value,
            value_len + new_column->column_length
        );

        /* Move meta column info. */
        uint32_t column_size = get_column_size(root_node);
        for (int i = column_size; i >= pos; i--) {
            if (i == pos) 
                memcpy(
                    get_meta_column_pointer(root_node, i), 
                    destination, 
                    ROOT_NODE_META_COLUMN_SIZE
                );
            else if (i > pos) 
                memcpy(
                    get_meta_column_pointer(root_node, i), 
                    get_meta_column_pointer(root_node, i - 1), 
                    ROOT_NODE_META_COLUMN_SIZE
                );
        }
        
        /* Increase column size. */
        set_column_size(root_node, column_size + 1);
    
        /* Flush page. */
        MakeBufferDirty(buffer);

        /* Append new column for children. */
        value_len += new_column->column_length;

        /* Notice: Keep the FRTL (From Right to Left) princile.
         * we then maybe need the get_max_key function work, 
         * it will try to fetch the rightmost cell in internal node, 
         * so make sure right cells has been appended new column. */
        uint32_t right_child_page_num = get_internal_node_right_child(root_node, value_len);
        append_new_column(right_child_page_num, table, new_column, pos);

        int i;
        for (i = keys_num - 1; i >= 0; i--) {
            uint32_t child_page_num = get_internal_node_child(root_node, i, key_len, value_len);
            append_new_column(child_page_num, table, new_column, pos);
        }
    }

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Append new column for internal node. */
static void append_internal_node_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos) {

    /* Get internal node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *internal_node = GetBufferPage(buffer);
    Assert(get_node_type(internal_node) == INTERNAL_NODE);

    if (is_root_node(internal_node)) 
        append_root_internal_node_column(page_num, table, new_column, pos);
    else {
        uint32_t key_len = calc_primary_key_length(table);
        uint32_t value_len = calc_table_row_length(table);
        uint32_t keys_num = get_internal_node_keys_num(internal_node, value_len);
        
        uint32_t i;
        for (i = 0; i < keys_num; i++) {
            uint32_t child_page_num = get_internal_node_child(internal_node, i, key_len, value_len);
            append_new_column(child_page_num, table, new_column, pos);
        }

        uint32_t right_child_page_num = get_internal_node_right_child(internal_node, value_len);
        append_new_column(right_child_page_num, table, new_column, pos);
    }

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Append new column. 
 * This function supports page-level alter-table-add-column routine.
 * And it will do the following:
 * (1) Add new meta column info to the root node.
 * (2) Add default value or NULL value (if missing default value) to the whole cells. */
void append_new_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos) {

    /* Get page buffer.*/
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num); 
    void *node = GetBufferPage(buffer);
    NodeType node_type = get_node_type(node);
    switch (node_type) {
        case LEAF_NODE:
            append_leaf_node_column(page_num, table, new_column, pos);
            break;
        case INTERNAL_NODE:
            append_internal_node_column(page_num, table, new_column, pos);
            break;
        case UNKNOWN_NODE:
            UNEXPECTED_VALUE(node_type);
            break;
    }
    /* Release page buffer. */
    ReleaseBuffer(buffer);
}

/* Genrate new default value at drop colum*/
static void *gen_new_default_value_after_drop_column(void *default_value, MetaTable *meta_table, MetaColumn *meta_column, int pos) {
    Assert(pos > -1);

    uint32_t value_len, offset;
    value_len = calc_table_row_length2(meta_table);
    /* Get new column offset. */
    offset = calc_offset_new_column(meta_table, pos);

    /* Move after offset memory. */
    memmove(
        default_value + offset, 
        default_value + offset + meta_column->column_length, 
        value_len - offset - meta_column->column_length
    );
    
    /* Set 0 for tail of value. */
    memset(
        default_value + value_len - meta_column->column_length, 
        0, 
        meta_column->column_length
    );

    return default_value;
}


/* Generate new cell after droping column. */
static void *gen_new_cell_after_drop_column(void *old_cell, MetaTable *meta_table, MetaColumn *meta_column, int pos) {

    Assert(pos > -1);

    uint32_t value_len = calc_table_row_length2(meta_table);
    uint32_t key_len = calc_primary_key_length2(meta_table);
    uint32_t cell_len = key_len + value_len;
    
    /* Get new column offset. */
    uint32_t offset = calc_offset_new_column(meta_table, pos);
    Assert(cell_len > offset);
    
    /* Move after offset memory. */
    memcpy(old_cell + offset, 
           old_cell + offset + meta_column->column_length, 
           cell_len - offset - meta_column->column_length);

    return old_cell;

}


/* Get new cell pointer After droping column. */
static void *cell_pointer_after_drop_column(void *leaf_node, MetaColumn *old_column, int index, uint32_t key_len, uint32_t value_len) {
    /* Row value lenght after substract the old column. */
    value_len -= old_column->column_length;
    uint32_t row_len = key_len + value_len;
    /* Column size increases one. */
    uint32_t column_size = get_column_size(leaf_node) - 1;
    if (is_root_node(leaf_node))
        return (leaf_node + ROOT_NODE_META_COLUMN_SIZE_OFFSET + ROOT_NODE_META_COLUMN_SIZE_SIZE + ROOT_NODE_META_COLUMN_SIZE * column_size + value_len + CELL_NUM_SIZE + LEAF_NODE_NEXT_LEAF_SIZE + row_len * index); 
    else
        return (leaf_node + LEAF_NODE_CELL_OFFSET + row_len * index);

}

/* Drop column for root leaf node.  */
static void drop_root_leaf_node_column(uint32_t page_num, Table *table, int pos) {

    /* Get root node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *root_node = GetBufferPage(buffer);
    Assert(get_node_type(root_node) == LEAF_NODE);
    Assert(is_root_node(root_node));

    uint32_t key_len, value_len, cell_len;
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    cell_len = key_len + value_len;
    
    MetaTable *meta_table = table->meta_table;
    MetaColumn *meta_column = meta_table->meta_column[pos];

    /* Move meta column info. */
    uint32_t column_size = get_column_size(root_node);
    for (int i = pos; i < column_size; i++) {
        if (i < column_size - 1)
            mempcpy(
                get_meta_column_pointer(root_node, i),
                get_meta_column_pointer(root_node, i + 1),
                ROOT_NODE_META_COLUMN_SIZE
            );
        else
            memset(
                get_meta_column_pointer(root_node, i), 
                0, 
                ROOT_NODE_META_COLUMN_SIZE
            );
    }

    /* Move default value. */
    void *default_value = get_default_value_cell(root_node);
    memmove(
        default_value - ROOT_NODE_META_COLUMN_SIZE, 
        gen_new_default_value_after_drop_column(default_value, meta_table, meta_column, pos), 
        value_len - meta_column->column_length
    );

    /* Move leaf node header. */
    memmove(
        get_leaf_node_header_pointer(root_node, value_len) - ROOT_NODE_META_COLUMN_SIZE - meta_column->column_length, 
        get_leaf_node_header_pointer(root_node, value_len), 
        CELL_NUM_SIZE + LEAF_NODE_NEXT_LEAF_SIZE
    );

    /* Move leaf node body cells. */ 
    uint32_t cell_num = get_leaf_node_cell_num(root_node, value_len);
    for (int i = 0; i < cell_num; i++) {
        void *old_cell = get_leaf_node_cell(root_node, key_len, value_len, i);
        void *new_cell = gen_new_cell_after_drop_column(old_cell, meta_table, meta_column, pos);
        memmove(
            cell_pointer_after_drop_column(root_node, meta_column, i, key_len, value_len), 
            new_cell,
            cell_len - meta_column->column_length
        );
    }
    
    /* Decrease column size. */
    set_column_size(root_node, column_size - 1);

    /* flush page. */
    MakeBufferDirty(buffer);

    /* Release pager buffer. */
    ReleaseBuffer(buffer);
}

/* Drop column for normal leaf node. */
static void drop_normal_leaf_node_column(uint32_t page_num, Table *table, int pos) {
    
    /* Get leaf node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *leaf_node = GetBufferPage(buffer);
    Assert(get_node_type(leaf_node) == LEAF_NODE);
    Assert(!is_root_node(leaf_node));

    uint32_t key_len, value_len, cell_len;
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    cell_len = key_len + value_len;
    
    MetaTable *meta_table = table->meta_table;
    MetaColumn *meta_column = meta_table->meta_column[pos];
    
    /*************************************************************
     * For normal leaf node, only need to move body cells, 
     * the header keeps still. 
     ************************************************************/

    /* Move leaf node body cells. */ 
    uint32_t cell_num = get_leaf_node_cell_num(leaf_node, value_len);
    for (int i = 0; i < cell_num; i++) {
        void *old_cell = get_leaf_node_cell(leaf_node, key_len, value_len, i);
        void *new_cell = gen_new_cell_after_drop_column(old_cell, meta_table, meta_column, pos);
        memmove(
            cell_pointer_after_drop_column(leaf_node, meta_column, i, key_len, value_len), 
            new_cell,
            cell_len - meta_column->column_length
        );
    }

    /* Flush page. */
    MakeBufferDirty(buffer);

    /* Release buffer. */
    ReleaseBuffer(buffer);
}

/* Drop column for leaf node. */
static void drop_leaf_node_column(uint32_t page_num, Table *table, int pos) {
    
    /* Get leaf node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *leaf_node = GetBufferPage(buffer);
    Assert(get_node_type(leaf_node) == LEAF_NODE);

    if (is_root_node(leaf_node)) 
        drop_root_leaf_node_column(page_num, table, pos);
    else 
        drop_normal_leaf_node_column(page_num, table, pos);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}


/* Drop column for root internal node. */
static void drop_root_internal_node_column(uint32_t page_num, Table *table, int pos) {

    /* Get root node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *root_node = GetBufferPage(buffer);
    Assert(get_node_type(root_node) == INTERNAL_NODE);
    Assert(is_root_node(root_node));

    uint32_t key_len, value_len;
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    
    MetaTable *meta_table = table->meta_table;
    MetaColumn *meta_column = meta_table->meta_column[pos];

    /* Move meta column info. */
    uint32_t column_size = get_column_size(root_node);
    for (int i = pos; i < column_size; i++) {
        if (i < column_size - 1)
            mempcpy(
                get_meta_column_pointer(root_node, i),
                get_meta_column_pointer(root_node, i + 1),
                ROOT_NODE_META_COLUMN_SIZE
            );
        else
            memset(
                get_meta_column_pointer(root_node, i), 
                0, 
                ROOT_NODE_META_COLUMN_SIZE
            );
    }

    /* Move default value. */
    void *default_value = get_default_value_cell(root_node);
    memmove(
        default_value - ROOT_NODE_META_COLUMN_SIZE, 
        gen_new_default_value_after_drop_column(default_value, meta_table, meta_column, pos), 
        value_len - meta_column->column_length
    );


    /* Move body */
    uint32_t cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    uint32_t keys_num = get_internal_node_keys_num(root_node, value_len);

    void *body = get_internal_node_body_pointer(root_node, value_len);
    memmove(
        body - ROOT_NODE_META_COLUMN_SIZE - meta_column->column_length,
        body,
        KEYS_NUM_SIZE + RIGHT_CHILD_SIZE + cell_len * keys_num
    );


    /* Decrease column size. */
    set_column_size(root_node, column_size - 1);

    /* Flush page. */
    MakeBufferDirty(buffer);

    /* Loop for children. */
    value_len -= meta_column->column_length;
    uint32_t i;
    for (i = 0; i < keys_num; i++) {
        uint32_t child_page_num = get_internal_node_child(root_node, i, key_len, value_len);
        drop_column(child_page_num, table, pos);
    }

    uint32_t right_child_page_num = get_internal_node_right_child(root_node, value_len);
    drop_column(right_child_page_num, table, pos);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}


/* Drop column for normal internal node. */
static void drop_normal_internal_node_column(uint32_t page_num, Table *table, int pos) {

    /* Get internal_node node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *internal_node = GetBufferPage(buffer);
    Assert(get_node_type(internal_node) == INTERNAL_NODE);
    Assert(!is_root_node(internal_node));

    uint32_t key_len, value_len, keys_num;
    value_len = calc_table_row_length(table);
    key_len = calc_primary_key_length(table);
    keys_num = get_internal_node_keys_num(internal_node, value_len);

    /* Loop for children. */
    uint32_t i;
    for (i = 0; i < keys_num; i++) {
        uint32_t child_page_num = get_internal_node_child(internal_node, i, key_len, value_len);
        drop_column(child_page_num, table, pos);
    }

    uint32_t right_child_page_num = get_internal_node_right_child(internal_node, value_len);
    drop_column(right_child_page_num, table, pos);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Drop column for internal node. */
static void drop_internal_node_column(uint32_t page_num, Table *table, int pos) {

    /* Get internal node. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    void *internal_node = GetBufferPage(buffer);
    Assert(get_node_type(internal_node) == INTERNAL_NODE);

    if (is_root_node(internal_node))
        drop_root_internal_node_column(page_num, table, pos);
    else 
        drop_normal_internal_node_column(page_num, table, pos);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Drop the column. */
void drop_column(uint32_t page_num, Table *table, int pos) {

    /* Get page buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), page_num); 
    void *node = GetBufferPage(buffer);

    NodeType node_type = get_node_type(node);
    switch (node_type) {
        case LEAF_NODE:
            drop_leaf_node_column(page_num, table, pos);
            break;
        case INTERNAL_NODE:
            drop_internal_node_column(page_num, table, pos);
            break;
        case UNKNOWN_NODE:
            UNEXPECTED_VALUE(node_type);
            break;
    }

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* If row is deleted*/
bool cursor_is_deleted(Cursor *cursor) {
    uint32_t key_len, value_len;

    Table *table = cursor->table;
    key_len = calc_primary_key_length(table); 
    value_len = calc_table_row_length(table); 

    /* Get the leaf node buffer. */
    Buffer buffer = ReadBuffer(GET_TABLE_OID(table), cursor->page_num);
    void *leaf_node = GetBufferPage(buffer);
    void *destination = get_leaf_node_cell_value(leaf_node, key_len, value_len, cursor->cell_num);
 
    int i, offset = 0;
    for (i = 0; i < table->meta_table->all_column_size; i++) {
        MetaColumn *meta_column = table->meta_table->meta_column[i];
        if (meta_column->sys_reserved && 
                strcmp(meta_column->column_name, EXPIRED_XID_COLUMN_NAME) == 0) {
            int64_t expired_xid = *(int64_t *)(destination + offset);
            ReleaseBuffer(buffer);
            return expired_xid != 0;
        }
        offset += meta_column->column_length;
    }

    /* Release the leaf node buffer. */
    ReleaseBuffer(buffer);

    return false;
}

/* Update row system reserved columns. */
void update_row_data(Row *row, Cursor *cursor) {

    Table *table;
    Buffer buffer;
    void *leaf_node, *destination;
    uint32_t key_len, value_len;

    table = cursor->table;
    key_len = calc_primary_key_length(table); 
    value_len = calc_table_row_length(table); 
    
    /* Get leaf node. */
    buffer = ReadBuffer(GET_TABLE_OID(table), cursor->page_num);
    leaf_node = GetBufferPage(buffer);

    /* Serialize row. */
    destination = serialize_row_data(row, cursor->table);

    /* Overcover leaf node. */
    memcpy(get_leaf_node_cell_value(leaf_node, key_len, value_len, cursor->cell_num), destination, value_len);

    /* Flush page. */
    MakeBufferDirty(buffer);

    dfree(destination);

    /* Release page buffer. */
    ReleaseBuffer(buffer);
}

/* When root is empty to do. */
static void make_empty_root_node(Table *table) {
    Buffer buffer;
    void *root;
    uint32_t value_len;

    buffer = ReadBuffer(GET_TABLE_OID(table), table->root_page_num); 
    root = GetBufferPage(buffer);
    Assert(get_node_type(root) == INTERNAL_NODE);
    value_len = calc_table_row_length(table);

    set_node_type(root, LEAF_NODE);
    set_leaf_node_cell_num(root, value_len, 0);
    set_leaf_node_next_leaf(root, value_len, 0);

    /* Release root buffer. */
    ReleaseBuffer(buffer);
}

/* Delete internal node. */
void delete_internal_node_cell(Table *table, uint32_t page_num, void *key, DataType key_data_type) {
    /* Get internal node. */
    Buffer buffer;
    void *internal_node; 
    uint32_t key_num, key_len, cell_len, value_len, key_index;

    buffer = ReadBuffer(GET_TABLE_OID(table), page_num);
    internal_node = GetBufferPage(buffer);
    key_len = calc_primary_key_length(table);
    value_len = calc_table_row_length(table);
    key_num = get_internal_node_keys_num(internal_node, value_len);
    cell_len = key_len + INTERNAL_NODE_CELL_CHILD_SIZE;
    value_len = calc_table_row_length(table);
    key_index = get_internal_node_key_index(internal_node, key, key_num, key_len, value_len, key_data_type);

    /* Get max key in internal node cell. */
    void *max_key = (key_num == 0) 
                ? NULL 
                : get_internal_node_key(internal_node, key_num - 1, key_len, value_len);

    /* If the key greater than max key, it means it is in right child node. */
    if (greater(key, max_key, key_data_type)) {
        /* If key number is one, no need to keep right node, delete it. 
         * If key number is zero, only left right node. After deleting it, check if root, empty root node or delete parent node cell.
         * If key number more than one, replace max key cell as right child node. */
        if (key_num == 1) 
            set_internal_node_right_child(internal_node, value_len, 0);
        else if (key_num == 0) {
            set_internal_node_right_child(internal_node, value_len, 0);
            if (is_root_node(internal_node))
                make_empty_root_node(table);
            else {
                uint32_t parent_page_num = get_parent_pointer(internal_node);
                delete_internal_node_cell(table, parent_page_num, key, key_data_type);
            }
        } else {
            void *old_max_key = get_max_key(table, internal_node, key_len, value_len);
            void *new_max_key = get_internal_node_key(internal_node, key_num - 1, key_len, value_len);
            uint32_t max_key_page_num = get_internal_node_child(internal_node, key_num - 1, key_len, value_len);
            set_internal_node_right_child(internal_node, value_len, max_key_page_num);
            memset(get_internal_node_cell(internal_node, key_num - 1, key_len, value_len), 0, cell_len);
            /* Decrease cell number. */
            set_internal_node_keys_num(internal_node, value_len, --key_num);
            /* Update key.*/
            update_internal_node_key(
                table, page_num, old_max_key, new_max_key,
                key_len, value_len, key_data_type
            );
        }
    } else {
        /* Check if deleting the last cell.
         * If last internal cell, it will impact its parent internal node (if exists). */
        if (key_index == key_num - 1) {
            /* Check if current internal node is root, absolutely, root node has no parent node. */
            if (!is_root_node(internal_node)) {
                uint32_t parent_page = get_parent_pointer(internal_node);
                /* Check if the last one cell in ther internal node. 
                 * If no, we need to replace key with previous key.
                 * If yes, more complex, we need to check if right child node exists, 
                 * If not, need to delete the cell in parent internal node. */
                if (key_num != 1) {
                    /*void *previous_key = get_internal_node_key(internal_node, key_index - 1, key_len);*/
                    /*update_internal_node_key(table, parent_internal_node, key, previous_key, key_len, value_len, key_data_type);*/
                    /*make_page_dirty(table->pager, parent_page);*/
                } else {
                    /* It means there is no right child node. We need to delete the cell in parent internal node. */
                    uint32_t right_child_page_num = get_internal_node_right_child(internal_node, value_len);
                    if (right_child_page_num == 0) 
                        delete_internal_node_cell(table, parent_page, key, key_data_type);
                }
            } else {
                if (key_num == 1) {
                    /* If root node, firstly check if exist right child node. 
                     * If not is was empty root, deal with empty root. */
                    uint32_t right_child_page_num = get_internal_node_right_child(internal_node, value_len);
                    if (right_child_page_num == 0) 
                        make_empty_root_node(table);
                }
            }
            
            /* Make last one NULL. */
            memset(get_internal_node_cell(internal_node, key_index, key_len, value_len), 0, cell_len);
            /* Decrease cell number. */
            set_internal_node_keys_num(internal_node, value_len, --key_num);
        } else {
            /* Move right cell forward to cover the obsolute cell sapce. 
             * If the last cell, set NULL. */
            for (uint32_t i = key_index; i < key_num; i++) {
                if (i != key_num - 1) 
                    memcpy(
                        get_internal_node_cell(internal_node, i, key_len, value_len), 
                        get_internal_node_cell(internal_node, i + 1, key_len, value_len), 
                        cell_len
                    );
                else 
                    memset(
                        get_internal_node_cell(internal_node, i, key_len, value_len), 
                        0, 
                        cell_len
                    );
            }
            /* Decrease cell number. */
            set_internal_node_keys_num(internal_node, value_len, --key_num);
        }
    }
    
    /* Flush to page. */
    MakeBufferDirty(buffer);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Delete leaf node. */
void delete_leaf_node_cell(Cursor *cursor, void *key) {
    Buffer buffer;
    void *leaf_node, *obs_key;
    uint32_t key_len, value_len, cell_length, cell_num;
    Table *table;
    MetaColumn *primary_key_meta_column;
    Oid oid;

    key_len = calc_primary_key_length(cursor->table);
    value_len = calc_table_row_length(cursor->table);
    cell_length = value_len + key_len;
    table = cursor->table;

    /* Get leaf node and cell number. */
    oid = GET_TABLE_OID(table);
    buffer = ReadBuffer(oid, cursor->page_num);
    leaf_node = GetBufferPage(buffer);
    cell_num = get_leaf_node_cell_num(leaf_node, value_len);
    obs_key = get_leaf_node_cell_key(leaf_node, cursor->cell_num, key_len, value_len);
    primary_key_meta_column = get_primary_key_meta_column(table->meta_table);

    /* Theoretically, key and obs_key should be equal. */
    Assert(equal(obs_key, key, primary_key_meta_column->column_type));

    /* Need to check if the last cell in the leaf node. */
    if (cursor->cell_num == cell_num - 1) {
        /* The last cell has the max key, so if it`s parent node is internal node, need to repalce key. */
        if (!is_root_node(leaf_node)) {
            /* Get parent node. */
            uint32_t parent_page = get_parent_pointer(leaf_node);

            /* If cell num is zero, it means it`s the only one in the leaf node, 
             * after delete it we also need to delete the cell in internal node. 
             * If not zero, it means these are sibling cells, just replace it with its previous cell. */
            if (cursor->cell_num == 0)
                delete_internal_node_cell(cursor->table, parent_page, obs_key, primary_key_meta_column->column_type);
            else {
                void *obs_previous_key = get_leaf_node_cell_key(leaf_node, cursor->cell_num - 1, key_len, value_len);
                update_internal_node_key(
                    cursor->table, parent_page, 
                    obs_key, obs_previous_key, key_len, value_len, 
                    primary_key_meta_column->column_type
                );
            }
        }

        /* If old cell is the last one, just make it null. */
        memset(get_leaf_node_cell(leaf_node, key_len, value_len, cell_num - 1), 0, cell_length);

        /* Update refer null. */
        update_refer(oid, cursor->page_num, cell_num - 1, -1, -1);
    } else {
        /* Move right cell forward to cover the obsolute cell sapce. */
        for (uint32_t i = cursor->cell_num; i < cell_num; i++) {
            if (i == cell_num - 1) 
                memset(get_leaf_node_cell(leaf_node, key_len, value_len, i), 0, cell_length);
            else {
                memcpy(get_leaf_node_cell(leaf_node, key_len, value_len, i), 
                       get_leaf_node_cell(leaf_node, key_len, value_len, i + 1), 
                       cell_length);
                /* Update postion-changed row refer. */
                update_refer(oid, cursor->page_num, i + 1, cursor->page_num, i);
            }
        }
        /* Update deleted row refer. */
        update_refer(oid, cursor->page_num, cursor->cell_num, -1, -1);
    }

    /* Decrease cell number. */
    cell_num--;
    decrease_leaf_node_cell_num(leaf_node, value_len);

    MakeBufferDirty(buffer);

    /* Release the buffer. */
    ReleaseBuffer(buffer);
}

/* Deserialize meta column */
MetaColumn *deserialize_meta_column(void *destination) {
    MetaColumn *meta_column = instance(MetaColumn);
    strcpy(meta_column->column_name, destination); 
    meta_column->column_type = (DataType)*(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE);
    meta_column->column_length = *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE);
    meta_column->is_primary = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE);
    if (meta_column->column_type == T_REFERENCE)
        strcpy(meta_column->table_name, destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE);
    meta_column->sys_reserved = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE);
    meta_column->is_unique = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE);
    meta_column->not_null = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE);
    meta_column->array_dim = *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE);
    meta_column->array_cap = *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE);
    meta_column->default_value_type = (DefaultValueType)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE);
    meta_column->has_comment = (bool)*(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE);
    strcpy(meta_column->comment, destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE + ROOT_NODE_HAS_COMMENT_SIZE);
    return meta_column;
}

/* Deserialize meta column */
void *serialize_meta_column(MetaColumn *meta_column) {
    void *destination= dalloc(ROOT_NODE_META_COLUMN_SIZE);
    strcpy(destination, meta_column->column_name);
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE) = (uint32_t) meta_column->column_type;
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE) = (uint32_t) meta_column->column_length;
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE) = meta_column->is_primary;  
    if (meta_column->column_type == T_REFERENCE)
        strcpy(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE, meta_column->table_name);
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE) = meta_column->sys_reserved;  
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE) = meta_column->is_unique;  
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE) = meta_column->not_null;  
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE) = meta_column->array_dim;  
    *(uint32_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE) = meta_column->array_cap;
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE) = meta_column->default_value_type;
    *(uint8_t *)(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE) = meta_column->has_comment;  
    memcpy(destination + ROOT_NODE_META_COLUMN_NAME_SIZE + ROOT_NODE_META_COLUMN_TYPE_SIZE + ROOT_NODE_META_COLUMN_LENGTH_SIZE + ROOT_NODE_IS_PRIMARY_SIZE + ROOT_NODE_META_COLUMN_TABLE_NAME_SIZE + ROOT_NODE_SYS_RESERVED_SIZE + ROOT_NODE_IS_UNIQUE_SIZE + ROOT_NODE_NOT_NULL_SIZE + ROOT_NODE_ARRAY_DIM_SIZE + ROOT_NODE_ARRAY_CAP_SIZE + ROOT_NODE_DEFAULT_VALUE_TYPE_SIZE + ROOT_NODE_HAS_COMMENT_SIZE, 
           meta_column->comment, 
           ROOT_NODE_COMMENT_STRING_SIZE);
    return destination;
}

/* Check cell is null. */
bool is_null_cell(void *destination) {
    return (*(uint8_t *)destination == 1);
}

/* Assign array number. */
void assign_array_number(void *destination, uint32_t array_num) {
    memcpy(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, &array_num, LEAF_NODE_ARRAY_NUM_SIZE);
}

/* Get array number. */
uint32_t get_array_number(void *destination) {
    return *(uint32_t *)(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE);
}

/* Get array value. */
void *get_array_value(void *destination, uint32_t i, uint32_t span) {
    return (destination + LEAF_NODE_CELL_NULL_FLAG_SIZE + LEAF_NODE_ARRAY_NUM_SIZE + span * i);
}

/* Get row value. 
 * Note: firstly, find values in user-passed row,
 * if missing, rturn the default values.
 * */
static void *get_value_from_row(Row *row, MetaColumn *meta_column) {
    char *column_name = meta_column->column_name;

    ListCell *lc;
    foreach (lc, row->data) {
        KeyValue *key_value = lfirst(lc);
        if (streq(column_name, key_value->key))
           return key_value->value;
    }

    switch (meta_column->default_value_type) {
        case DEFAULT_VALUE_NULL:
        case DEFAULT_VALUE_NONE:
            return NULL;
        case DEFAULT_VALUE:
            return meta_column->default_value;
        default:
            UNEXPECTED_VALUE(meta_column->default_value_type);
    }
}

/* Serialize array value. 
 * Note: for array value cell, we will reserve LEAF_NODE_ARRAY_NUM_SIZE bytes length for store array number. */
static void assign_row_array_value(void *destination, ArrayValue *array_value, MetaColumn *meta_column) {
    uint32_t len, span;

    len =len_list(array_value->list);
    /* User insert arrary values number integer multiple of array dim. */
    Assert(len % meta_column->array_dim == 0);

    /* Assign array number. */
    assign_array_number(destination, len);
    /* span: every value in array data lenght. */
    span = (meta_column->column_length - LEAF_NODE_ARRAY_NUM_SIZE - LEAF_NODE_CELL_NULL_FLAG_SIZE) / meta_column->array_cap;

    uint32_t i = 0;
    ListCell *lc;
    foreach (lc, array_value->list) {
        void *value = lfirst(lc);
        memcpy((destination + LEAF_NODE_CELL_NULL_FLAG_SIZE + LEAF_NODE_ARRAY_NUM_SIZE + span * i), value, span);        
        i++;
    }
}

/* Assign value to row destination.  */
static void assign_row_value(void *destination, void *value, MetaColumn *meta_column) {
    /* If not array value, assign value to destination, 
     * if array value, assign values one by one. */
    if (meta_column->array_dim == 0) {
        bool nflag = value == NULL ? true : false;
        memcpy(destination, &nflag, LEAF_NODE_CELL_NULL_FLAG_SIZE);
        if (!nflag)
            memcpy(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, value, meta_column->column_length);
        else
            memset(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, 0, meta_column->column_length);
    } else {
        bool nflag = value == NULL ? true : false;
        memcpy(destination, &nflag, LEAF_NODE_CELL_NULL_FLAG_SIZE);
        if (!nflag)
            assign_row_array_value(destination, value, meta_column);
        else
            memset(destination + LEAF_NODE_CELL_NULL_FLAG_SIZE, 0, meta_column->column_length);
    } 
}

/* Serialize row data */
void *serialize_row_data(Row *row, Table *table) {
    uint32_t row_length = calc_table_row_length(table);
    void *destination = dalloc(row_length);
    MetaTable *meta_table = table->meta_table;
    uint32_t i, offset = 0;
    for (i = 0; i < meta_table->all_column_size; i++) {
        MetaColumn *meta_column = meta_table->meta_column[i]; 
        void *value = get_value_from_row(row, meta_column);
        if (meta_column->not_null && is_null(value)) {
            db_log(ERROR, "Column '%s' does`t have a default value.", meta_column->column_name);
            return NULL;
        }
        /* Assign row value to destination. */
        assign_row_value(destination + offset, value, meta_column);
        offset += meta_column->column_length;
    }
    return destination;
}
