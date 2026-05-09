# test_expressions.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

ALL_DATA = [
    {'id': 1, 'a': 10, 'b': 3, 'c': 2.5},
    {'id': 2, 'a': 20, 'b': 5, 'c': 4.0},
    {'id': 3, 'a': 30, 'b': 6, 'c': 8.0},
]


def test_setup_data():
    sql = "create table expr_test_t (id int primary key, a int, b int, c float);\n" \
          "insert into expr_test_t values (1, 10, 3, 2.5);\n" \
          "insert into expr_test_t values (2, 20, 5, 4.0);\n" \
          "insert into expr_test_t values (3, 30, 6, 8.0);"
    ret = client.execute(sql)
    assert_all(ret)


# --- Arithmetic expressions in SELECT ---

def test_select_addition():
    ret = client.execute("select a + b as v from expr_test_t;")
    print(ret)
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [{'v': 13}, {'v': 25}, {'v': 36}]


def test_select_subtraction():
    ret = client.execute("select a - b as v from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [{'v': 7}, {'v': 15}, {'v': 24}]


def test_select_multiplication():
    ret = client.execute("select a * b as v from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [{'v': 30}, {'v': 100}, {'v': 180}]


def test_select_division():
    ret = client.execute("select a / b as v from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [{'v': 3}, {'v': 4}, {'v': 5}]


def test_select_expression_with_parentheses():
    ret = client.execute("select (a + b) * c as v from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [{'v': 32.5}, {'v': 100.0}, {'v': 288.0}]


def test_select_complex_expression():
    ret = client.execute("select a + b * 2 - c as v from expr_test_t;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [{'v': 13.5}, {'v': 26.0}, {'v': 34.0}]


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
    assert ret["data"] == [{'sum_val': 13}, {'sum_val': 25}, {'sum_val': 36}]


def test_select_expression_alias_no_as():
    ret = client.execute("select a + b sum_val from expr_test_t;")
    # Note: the current grammar doesn't support alias without AS for scalar_exp
    # AS is required per the grammar rule: scalar_exp AS IDENTIFIER
    assert ret["success"] == False


# --- Expression in WHERE ---

def test_where_expression_comparison():
    ret = client.execute("select * from expr_test_t where a + b > 20;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == [ALL_DATA[1], ALL_DATA[2]]


def test_where_expression_both_sides():
    ret = client.execute("select * from expr_test_t where a * 2 > b + 10;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == ALL_DATA


# --- Function in expressions ---

def test_function_in_expression():
    ret = client.execute("select count(*) + 1 as v from expr_test_t;")
    assert ret["success"] == True
    assert ret["data"] == [{'v': 4}]


def test_count_sum_expression():
    ret = client.execute("select count(*) * 100 as v from expr_test_t;")
    assert ret["success"] == True
    assert ret["data"] == [{'v': 300}]


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table expr_test_t;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
