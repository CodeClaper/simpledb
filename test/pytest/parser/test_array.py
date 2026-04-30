# test_array.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# --- 1D Array tests ---

def test_create_table_with_one_dim_array():
    sql = "create table arr_t1 (id int primary key, tags varchar(32)[]);\n" \
          "insert into arr_t1 values (1, ['hello', 'world']);"
    ret = client.execute(sql)
    assert_all(ret)


def test_insert_one_dim_array():
    ret = client.execute("insert into arr_t1 values (2, ['a', 'b', 'c']);")
    assert ret["success"] == True


def test_select_array_data():
    ret = client.execute("select * from arr_t1 where id = 1;")
    assert ret["success"] == True


def test_create_table_int_array():
    sql = "create table arr_t2 (id int primary key, scores int[]);\n" \
          "insert into arr_t2 values (1, [90, 80, 70]);"
    ret = client.execute(sql)
    assert_all(ret)


def test_create_table_float_array():
    sql = "create table arr_t3 (id int primary key, `values` float[]);\n" \
          "insert into arr_t3 values (1, [1.5, 2.5, 3.5]);"
    ret = client.execute(sql)
    assert_all(ret)


def test_create_table_bool_array():
    sql = "create table arr_t4 (id int primary key, flags bool[]);\n" \
          "insert into arr_t4 values (1, [true, false, true]);"
    ret = client.execute(sql)
    assert_all(ret)


# --- 2D Array tests ---

def test_create_table_with_two_dim_array():
    sql = "create table arr_t5 (id int primary key, matrix int[][], name varchar(32));\n" \
          "insert into arr_t5 values (1, [[1, 2], [3, 4]], 'matrix1');"
    ret = client.execute(sql)
    assert_all(ret)


def test_create_table_with_three_dim_array():
    sql = "create table arr_t6 (id int primary key, cube int[][][]);\n" \
          "insert into arr_t6 values (1, [[[1]]]);"
    ret = client.execute(sql)
    assert_all(ret)


# --- Array with null values ---

def test_array_with_null():
    ret = client.execute("insert into arr_t1 values (3, [null, 'x', null]);")
    assert ret["success"] == True


# --- Query tests ---

def test_select_star_from_array_table():
    ret = client.execute("select * from arr_t1;")
    assert ret["success"] == True


def test_select_array_column():
    ret = client.execute("select id, tags from arr_t1;")
    assert ret["success"] == True


def test_select_array_with_where():
    ret = client.execute("select * from arr_t1 where id = 1;")
    assert ret["success"] == True
    assert ret["rows"] == 1


def test_select_array_with_where_compare():
    ret = client.execute("select * from arr_t2 where id > 0;")
    assert ret["success"] == True


def test_select_array_with_count():
    ret = client.execute("select count(*) from arr_t1;")
    assert ret["success"] == True


def test_select_array_with_limit():
    ret = client.execute("select * from arr_t1 limit 1;")
    assert ret["success"] == True
    assert ret["rows"] == 1


def test_select_int_array():
    ret = client.execute("select * from arr_t2;")
    assert ret["success"] == True


def test_select_float_array():
    ret = client.execute("select * from arr_t3;")
    assert ret["success"] == True


def test_select_bool_array():
    ret = client.execute("select * from arr_t4;")
    assert ret["success"] == True


def test_select_two_dim_array():
    ret = client.execute("select * from arr_t5;")
    assert ret["success"] == True


def test_select_three_dim_array():
    ret = client.execute("select * from arr_t6;")
    assert ret["success"] == True


def test_select_two_dim_array_with_where():
    ret = client.execute("select * from arr_t5 where name = 'matrix1';")
    assert ret["success"] == True
    assert ret["rows"] == 1


def test_select_array_with_column_alias():
    ret = client.execute("select id, tags as t from arr_t1;")
    assert ret["success"] == True


def test_select_array_with_count_and_group():
    ret = client.execute("select count(*) from arr_t1 where id > 0;")
    assert ret["success"] == True


def test_select_array_with_in():
    ret = client.execute("select * from arr_t1 where id in (1, 2);")
    assert ret["success"] == True
    assert ret["rows"] == 2


# --- Cleanup ---

def test_cleanup():
    sql = "drop table arr_t1;\n" \
          "drop table arr_t2;\n" \
          "drop table arr_t3;\n" \
          "drop table arr_t4;\n" \
          "drop table arr_t5;\n" \
          "drop table arr_t6;"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
