# test_insert.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# --- INSERT with VALUES ---

def test_insert_with_all_columns():
    sql = "create table ins_t1 (id int primary key, name varchar(32), age int);\n" \
          "insert into ins_t1 values (1, 'alice', 20);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ins_t1 where id = 1;")
    assert ret["data"] == [{'id': 1, 'name': 'alice', 'age': 20}]


def test_insert_specific_columns():
    ret = client.execute("insert into ins_t1 (id, name) values (2, 'bob');")
    assert ret["success"] == True
    ret = client.execute("select * from ins_t1 where id = 2;")
    assert ret["data"] == [{'id': 2, 'name': 'bob', 'age': None}]


def test_insert_multiple_rows():
    ret = client.execute("insert into ins_t1 values (3, 'charlie', 30);\n" \
                         "insert into ins_t1 values (4, 'david', 25);")
    assert_all(ret)
    ret = client.execute("select * from ins_t1 where id in (3, 4);")
    assert ret["data"] == [
        {'id': 3, 'name': 'charlie', 'age': 30},
        {'id': 4, 'name': 'david', 'age': 25},
    ]


def test_insert_string_value():
    sql = "create table ins_t2 (id int primary key, name varchar(32));\n" \
          "insert into ins_t2 values (1, 'test user');"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ins_t2;")
    assert ret["data"] == [{'id': 1, 'name': 'test user'}]


def test_insert_float_value():
    sql = "create table ins_t3 (id int primary key, score float);\n" \
          "insert into ins_t3 values (1, 95.5);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ins_t3;")
    assert ret["data"] == [{'id': 1, 'score': 95.5}]


def test_insert_bool_value():
    sql = "create table ins_t4 (id int primary key, active bool);\n" \
          "insert into ins_t4 values (1, true);\n" \
          "insert into ins_t4 values (2, false);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ins_t4;")
    assert ret["data"] == [
        {'id': 1, 'active': True},
        {'id': 2, 'active': False},
    ]


def test_insert_null_value():
    sql = "create table ins_t5 (id int primary key, name varchar(32));\n" \
          "insert into ins_t5 values (1, null);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ins_t5;")
    assert ret["data"] == [{'id': 1, 'name': None}]


def test_insert_with_negative_number():
    ret = client.execute("insert into ins_t1 values (5, 'eve', -1);")
    assert ret["success"] == True
    ret = client.execute("select * from ins_t1 where id = 5;")
    assert ret["data"] == [{'id': 5, 'name': 'eve', 'age': -1}]


# --- INSERT with subquery ---

def test_insert_from_select():
    ret = client.execute("insert into ins_t1 select id + 10, name, age from ins_t1 where id <= 2;")
    assert ret["success"] == True
    ret = client.execute("select * from ins_t1 where id in (11, 12);")
    assert ret["data"] == [
        {'id': 11, 'name': 'alice', 'age': 20},
        {'id': 12, 'name': 'bob', 'age': None},
    ]


def test_insert_from_select_with_columns():
    sql = "create table ins_t6 (id int primary key, name varchar(32));\n" \
          "insert into ins_t6 (id, name) select id, name from ins_t1 where id = 1;"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ins_t6;")
    assert ret["data"] == [{'id': 1, 'name': 'alice'}]


# --- INSERT error cases ---

def test_insert_duplicate_primary_key():
    ret = client.execute("insert into ins_t1 values (1, 'duplicate', 99);")
    assert ret["success"] == False


def test_insert_nonexistent_table():
    ret = client.execute("insert into nonexistent_table values (1, 'test', 10);")
    assert ret["success"] == False


def test_insert_syntax_error():
    ret = client.execute("insert into ins_t1 (id, name) value (1, 'test');")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Cleanup ---

def test_cleanup():
    sql = "drop table ins_t1;\n" \
          "drop table ins_t2;\n" \
          "drop table ins_t3;\n" \
          "drop table ins_t4;\n" \
          "drop table ins_t5;\n" \
          "drop table ins_t6;"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
