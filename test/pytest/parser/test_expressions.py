# test_expressions.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

_table_created = False


def setup_data():
    global _table_created
    if not _table_created:
        sql = "create table expr_test_t (id int primary key, a int, b int, c float);\n" \
              "insert into expr_test_t values (1, 10, 3, 2.5);\n" \
              "insert into expr_test_t values (2, 20, 5, 4.0);\n" \
              "insert into expr_test_t values (3, 30, 6, 8.0);"
        ret = client.execute(sql)
        assert_all(ret)
        _table_created = True


# --- Arithmetic expressions in SELECT ---

def test_select_addition():
    setup_data()
    ret = client.execute("select a + b from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_subtraction():
    ret = client.execute("select a - b from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_multiplication():
    ret = client.execute("select a * b from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_division():
    ret = client.execute("select a / b from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_expression_with_parentheses():
    ret = client.execute("select (a + b) * c from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_complex_expression():
    ret = client.execute("select a + b * 2 - c from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_literal_addition_via_explain():
    ret = client.execute("explain select 10 + 20;")
    assert ret["success"] == True


def test_select_literal_expression_via_explain():
    ret = client.execute("explain select 2 * (3 + 4);")
    assert ret["success"] == True


# --- Expression with alias ---

def test_select_expression_alias():
    ret = client.execute("select a + b as sum_val from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_expression_alias_no_as():
    ret = client.execute("select a + b sum_val from expr_test_t;")
    # Note: the current grammar doesn't support alias without AS for scalar_exp
    # AS is required per the grammar rule: scalar_exp AS IDENTIFIER
    assert ret["success"] == False


# --- Expression in WHERE ---

def test_where_expression_comparison():
    ret = client.execute("select * from expr_test_t where a + b > 20;")
    assert ret["success"] == True


def test_where_expression_both_sides():
    ret = client.execute("select * from expr_test_t where a * 2 > b + 10;")
    assert ret["success"] == True


# --- Function in expressions ---

def test_function_in_expression():
    ret = client.execute("select count(*) + 1 from expr_test_t;")
    assert ret["success"] == True


def test_count_sum_expression():
    ret = client.execute("select count(*) * 100 from expr_test_t;")
    assert ret["success"] == True


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table expr_test_t;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
