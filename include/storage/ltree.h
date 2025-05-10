#include <stdint.h>
#include "data.h"

/* If obsolute node. */
bool is_obsolute_node(void *node);

/* Get NodeState. */
NodeState get_node_state(void *node);

/* Set node state. */
void set_node_state(void *node, NodeState state);

/* Get node type. */
NodeType get_node_type(void *node);

/* Set node type. */
void set_node_type(void *node, NodeType node_type);

/* Set if root node. */
void set_root_node(void *node, bool if_root_node);

/* Get root node column size. */
uint32_t get_column_size(void *node);

/* Set root node column size. */
void set_column_size(void *node, uint32_t value);

/* Get default value cell. */
void *get_default_value_cell(void *node);

/* Set default value cell. */
void set_default_value_cell(void *node, void *destination, uint32_t default_value_len);

/* Get leaf node cell number. */
uint32_t get_leaf_node_cell_num(void *node, uint32_t default_value_len);

/* Get internal node keys number. */
uint32_t get_internal_node_keys_num(void *node, uint32_t default_value_len);

/* Get right child of internal node. */
uint32_t get_internal_node_right_child(void *node, uint32_t default_value_len);

/* Get keys number in the node by index. */
void* get_internal_node_key(void *node, uint32_t index, uint32_t key_len, uint32_t value_len);

/* Get child value in the node by index. */
uint32_t get_internal_node_child(void *node, uint32_t index, uint32_t key_len, uint32_t value_len);

/* Get internal node child page num. */
uint32_t get_internal_node_cell_child_page_num(void *node, void *key, uint32_t keys_num, uint32_t key_len, uint32_t value_len, DataType primary_key_data_type);

/* Get leaf node cell key. */
void *get_leaf_node_cell_key(void *node, uint32_t index, uint32_t key_len,uint32_t value_len);

/* Get leaf node cell value pointer. */
void *get_leaf_node_cell_value(void *node, uint32_t key_len, uint32_t value_len, uint32_t index);

/* Get leaf node cell index. */
uint32_t get_leaf_node_cell_index(void *node, void *key, uint32_t cell_num, uint32_t key_len, uint32_t value_len, DataType key_data_type);

/* Get index meta column pointer. */
void *get_meta_column_pointer(void *root_node, uint32_t index);

/* Set index meta column. */
void set_meta_column(void *root_node, void *destination, uint32_t index);

/* Check if leaf node page overflow. */
bool overflow_leaf_node(void *leaf_node, uint32_t key_len, uint32_t value_len, uint32_t cell_num);

/* Get meta column size. */
uint32_t get_root_node_meta_column_size();

/* Check cell is null. */
bool is_null_cell(void *destination);

/* Get array number. */
uint32_t get_array_number(void *destination);

/* Get array value. */
void *get_array_value(void *destination, uint32_t index, uint32_t span);

/* Initialize leaf node. */
void initial_leaf_node(void *leaf_node, uint32_t default_value_len, bool is_root);

/* Insert new internal node cell. */
void insert_internal_node_cell(Table *table, uint32_t page_num, uint32_t new_child_page_num, uint32_t key_len, uint32_t value_len);

/* Insert a new leaf node cell. */
void insert_leaf_node_cell(Cursor *cursor, Row *row);

/* Delete leaf node. */
void delete_leaf_node_cell(Cursor *obs_cursor, void *key);

/* If cursor is deleted*/
bool cursor_is_deleted(Cursor *cursor);

/* Update row data. */
void update_row_data(Row *row, Cursor *cursor);

/* Deserialize meta column. */
MetaColumn *deserialize_meta_column(void *destination);

/* Deserialize meta column. */
void *serialize_meta_column(MetaColumn *meta_column);

/* Serialize row data. */
void *serialize_row_data(Row *row, Table *table);

/* Appedn new column. */
void append_new_column(uint32_t page_num, Table *table, MetaColumn *new_column, int pos);

/* Drop the column. */
void drop_column(uint32_t page_num, Table *table, int pos);
