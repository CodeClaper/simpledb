# test_update_delete.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

_table_created = False


def setup_data():
    global _table_created
    if not _table_created:
        sql = "create table ud_test_t (id int primary key, name varchar(32), age int);\n" \
              "insert into ud_test_t values (1, 'alice', 20);\n" \
              "insert into ud_test_t values (2, 'bob', 25);\n" \
              "insert into ud_test_t values (3, 'charlie', 30);\n" \
              "insert into ud_test_t values (4, 'david', 20);"
        ret = client.execute(sql)
        assert_all(ret)
        _table_created = True


# --- UPDATE tests ---

def test_update_set_single_column():
    setup_data()
    ret = client.execute("update ud_test_t set age = 21 where id = 1;")
    assert ret["success"] == True
    ret = client.execute("select id, name, age from ud_test_t where id = 1;")
    assert ret["data"] == [{'id': 1, 'name': 'alice', 'age': 21}]


def test_update_set_multiple_columns():
    ret = client.execute("update ud_test_t set name = 'alice2', age = 22 where id = 1;")
    assert ret["success"] == True
    ret = client.execute("select id, name, age from ud_test_t where id = 1;")
    assert ret["data"] == [{'id': 1, 'name': 'alice2', 'age': 22}]


def test_update_without_where():
    ret = client.execute("update ud_test_t set age = 99;")
    assert ret["success"] == True
    ret = client.execute("select id, age from ud_test_t;")
    assert ret["data"] == [
        {'id': 1, 'age': 99},
        {'id': 2, 'age': 99},
        {'id': 3, 'age': 99},
        {'id': 4, 'age': 99},
    ]


def test_update_set_string():
    ret = client.execute("update ud_test_t set name = 'updated' where id = 2;")
    assert ret["success"] == True
    ret = client.execute("select id, name from ud_test_t where id = 2;")
    assert ret["data"] == [{'id': 2, 'name': 'updated'}]


def test_update_set_with_null():
    ret = client.execute("update ud_test_t set age = null where id = 3;")
    assert ret["success"] == True
    ret = client.execute("select id, age from ud_test_t where id = 3;")
    assert ret["data"] == [{'id': 3, 'age': None}]


def test_update_multiple_assignments():
    ret = client.execute("update ud_test_t set name = 'x', age = 100 where id = 4;")
    assert ret["success"] == True
    ret = client.execute("select id, name, age from ud_test_t where id = 4;")
    assert ret["data"] == [{'id': 4, 'name': 'x', 'age': 100}]


def test_update_with_complex_where():
    ret = client.execute("update ud_test_t set age = 50 where id > 0 and name like '%e%';")
    assert ret["success"] == True
    ret = client.execute("select id, age from ud_test_t where id in (1, 2, 3);")
    assert ret["data"] == [
        {'id': 1, 'age': 50},
        {'id': 2, 'age': 50},
        {'id': 3, 'age': 50},
    ]
    ret = client.execute("select id, age from ud_test_t where id = 4;")
    assert ret["data"] == [{'id': 4, 'age': 100}]


def test_update_nonexistent_table():
    ret = client.execute("update nonexistent_table set x = 1;")
    assert ret["success"] == False


# --- DELETE tests ---

def test_delete_with_where():
    ret = client.execute("insert into ud_test_t values (10, 'to_delete', 0);")
    assert ret["success"] == True
    ret = client.execute("delete from ud_test_t where id = 10;")
    assert ret["success"] == True
    ret = client.execute("select * from ud_test_t where id = 10;")
    assert ret["data"] == []


def test_delete_without_where():
    ret = client.execute("insert into ud_test_t values (11, 'to_delete_all', 0);\n" \
                         "insert into ud_test_t values (12, 'to_delete_all2', 0);")
    assert_all(ret)
    ret = client.execute("delete from ud_test_t where id = 11;")
    assert ret["success"] == True
    ret = client.execute("select * from ud_test_t where id = 11;")
    assert ret["data"] == []
    ret = client.execute("delete from ud_test_t where id = 12;")
    assert ret["success"] == True
    ret = client.execute("select * from ud_test_t where id = 12;")
    assert ret["data"] == []


def test_delete_with_complex_condition():
    ret = client.execute("insert into ud_test_t values (13, 'complex', 50);")
    assert ret["success"] == True
    ret = client.execute("delete from ud_test_t where id = 13 and name = 'complex' and age > 30;")
    assert ret["success"] == True
    ret = client.execute("select * from ud_test_t where id = 13;")
    assert ret["data"] == []


def test_delete_nonexistent_table():
    ret = client.execute("delete from nonexistent_table;")
    assert ret["success"] == False


def test_delete_syntax_error_no_from():
    ret = client.execute("delete ud_test_t;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table ud_test_t;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
