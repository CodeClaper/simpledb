# test_select.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

_table_created = False


def setup_data():
    global _table_created
    if not _table_created:
        sql = "create table sel_test_t (id int primary key, name varchar(32), age int, score float);\n" \
              "insert into sel_test_t values (1, 'alice', 20, 85.5);\n" \
              "insert into sel_test_t values (2, 'bob', 25, 90.0);\n" \
              "insert into sel_test_t values (3, 'charlie', 30, 78.5);"
        ret = client.execute(sql)
        assert_all(ret)
        _table_created = True


# --- Basic SELECT ---

def test_select_star():
    setup_data()
    ret = client.execute("select * from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_single_column():
    ret = client.execute("select id from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_multiple_columns():
    ret = client.execute("select id, name from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_all_columns_explicit():
    ret = client.execute("select id, name, age, score from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


# --- SELECT with alias ---

def test_select_column_with_alias():
    ret = client.execute("select id as uid from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_multiple_aliases():
    ret = client.execute("select id as uid, name as uname from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_expression_with_alias():
    ret = client.execute("select age + 1 as next_age from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


# --- SELECT with table alias ---

def test_select_table_alias_implicit():
    ret = client.execute("select * from sel_test_t t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_table_alias_with_as():
    ret = client.execute("select * from sel_test_t as t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_qualified_column():
    ret = client.execute("select t.id, t.name from sel_test_t t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


# --- SELECT with aggregate functions ---

def test_select_count_star():
    ret = client.execute("select count(*) from sel_test_t;")
    assert ret["success"] == True


def test_select_count_column():
    ret = client.execute("select count(id) from sel_test_t;")
    assert ret["success"] == True


def test_select_count_with_int():
    ret = client.execute("select count(1) from sel_test_t;")
    assert ret["success"] == True


def test_select_max():
    ret = client.execute("select max(age) from sel_test_t;")
    assert ret["success"] == True


def test_select_min():
    ret = client.execute("select min(age) from sel_test_t;")
    assert ret["success"] == True


def test_select_sum():
    ret = client.execute("select sum(age) from sel_test_t;")
    assert ret["success"] == True


def test_select_avg():
    ret = client.execute("select avg(score) from sel_test_t;")
    assert ret["success"] == True


def test_select_multiple_functions():
    ret = client.execute("select min(age), max(age), count(*) from sel_test_t;")
    assert ret["success"] == True


# --- SELECT with arithmetic expressions on columns ---

def test_select_column_add():
    ret = client.execute("select age + 10 from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_column_subtract():
    ret = client.execute("select age - 5 from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_column_multiply():
    ret = client.execute("select age * 2 from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_column_divide():
    ret = client.execute("select age / 2 from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_complex_arithmetic():
    ret = client.execute("select (age + 10) * 2 from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_column_add_column():
    ret = client.execute("select age + score from sel_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


# --- SELECT with WHERE ---

def test_select_where_equals():
    ret = client.execute("select * from sel_test_t where id = 1;")
    assert ret["success"] == True
    assert ret["rows"] == 1


def test_select_where_greater_than():
    ret = client.execute("select * from sel_test_t where age > 20;")
    assert ret["success"] == True
    assert ret["rows"] == 2


def test_select_comparison_operators():
    ops = [("=", 1), ("!=", 2), (">", 2), (">=", 3), ("<", 1), ("<=", 1)]
    for op, expected in ops:
        ret = client.execute(f"select count(1) from sel_test_t where id {op} 2;")
        assert ret["success"] == True


# --- SELECT with LIMIT ---

def test_select_limit():
    ret = client.execute("select * from sel_test_t limit 2;")
    assert ret["success"] == True
    assert ret["rows"] == 2


def test_select_limit_offset_comma():
    ret = client.execute("select * from sel_test_t limit 0, 2;")
    assert ret["success"] == True
    assert ret["rows"] == 2


def test_select_limit_offset_keyword():
    ret = client.execute("select * from sel_test_t limit 2 offset 1;")
    assert ret["success"] == True
    assert ret["rows"] == 2


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table sel_test_t;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
