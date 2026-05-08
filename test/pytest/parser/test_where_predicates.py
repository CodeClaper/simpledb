# test_where_predicates.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

_table_created = False

ALL_DATA = [
    {'id': 1, 'name': 'alice', 'age': 20, 'score': 85.5},
    {'id': 2, 'name': 'bob', 'age': 25, 'score': 90.0},
    {'id': 3, 'name': 'charlie', 'age': 30, 'score': 78.5},
    {'id': 4, 'name': 'david', 'age': 20, 'score': 92.0},
    {'id': 5, 'name': 'eve', 'age': 25, 'score': 88.0},
]


def setup_data():
    global _table_created
    if not _table_created:
        sql = "create table whr_test_t (id int primary key, name varchar(32), age int, score float);\n" \
              "insert into whr_test_t values (1, 'alice', 20, 85.5);\n" \
              "insert into whr_test_t values (2, 'bob', 25, 90.0);\n" \
              "insert into whr_test_t values (3, 'charlie', 30, 78.5);\n" \
              "insert into whr_test_t values (4, 'david', 20, 92.0);\n" \
              "insert into whr_test_t values (5, 'eve', 25, 88.0);"
        ret = client.execute(sql)
        assert_all(ret)
        _table_created = True


# --- Comparison operators ---

def test_comparison_eq():
    setup_data()
    ret = client.execute("select * from whr_test_t where id = 1;")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [ALL_DATA[0]]


def test_comparison_ne():
    ret = client.execute("select * from whr_test_t where id != 1;")
    assert ret["success"] == True
    assert ret["rows"] == 4
    assert ret["data"] == ALL_DATA[1:]


def test_comparison_gt():
    ret = client.execute("select * from whr_test_t where id > 3;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == ALL_DATA[3:]


def test_comparison_ge():
    ret = client.execute("select * from whr_test_t where id >= 3;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == ALL_DATA[2:]


def test_comparison_lt():
    ret = client.execute("select * from whr_test_t where id < 3;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == ALL_DATA[:2]


def test_comparison_le():
    ret = client.execute("select * from whr_test_t where id <= 3;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == ALL_DATA[:3]


def test_comparison_string_eq():
    ret = client.execute("select * from whr_test_t where name = 'alice';")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [ALL_DATA[0]]


def test_comparison_float_gt():
    ret = client.execute("select * from whr_test_t where score > 90.0;")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [ALL_DATA[3]]


# --- LIKE predicate ---

def test_like_prefix():
    ret = client.execute("select * from whr_test_t where name like 'a%';")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [ALL_DATA[0]]


def test_like_suffix():
    ret = client.execute("select * from whr_test_t where name like '%e';")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [ALL_DATA[0], ALL_DATA[2], ALL_DATA[4]]


def test_like_contains():
    ret = client.execute("select * from whr_test_t where name like '%ar%';")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [ALL_DATA[2]]


def test_like_exact():
    ret = client.execute("select * from whr_test_t where name like 'bob';")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [ALL_DATA[1]]


# --- IN predicate ---

def test_in_single_value():
    ret = client.execute("select * from whr_test_t where id in (1);")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [ALL_DATA[0]]


def test_in_multiple_values():
    ret = client.execute("select * from whr_test_t where id in (1, 2, 3);")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == ALL_DATA[:3]


def test_in_string_values():
    ret = client.execute("select * from whr_test_t where name in ('alice', 'bob');")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == ALL_DATA[:2]


def test_in_no_match():
    ret = client.execute("select * from whr_test_t where name in ('zoe', 'zack');")
    assert ret["success"] == True
    assert ret["rows"] == 0
    assert ret["data"] == []


# --- Boolean logic: AND / OR ---

def test_and_condition():
    ret = client.execute("select * from whr_test_t where age = 20 and score > 80;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == [ALL_DATA[0], ALL_DATA[3]]


def test_or_condition():
    ret = client.execute("select * from whr_test_t where id = 1 or id = 5;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == [ALL_DATA[0], ALL_DATA[4]]


def test_and_or_combined():
    ret = client.execute("select * from whr_test_t where (age = 20 or age = 25) and score > 88;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == [ALL_DATA[1], ALL_DATA[3]]


def test_multiple_and():
    ret = client.execute("select * from whr_test_t where age > 18 and age < 30 and score > 80;")
    assert ret["success"] == True
    assert ret["rows"] == 4
    assert ret["data"] == [ALL_DATA[0], ALL_DATA[1], ALL_DATA[3], ALL_DATA[4]]


# --- NOT ---

def test_not_equals():
    ret = client.execute("select * from whr_test_t where not id = 1;")
    assert ret["success"] == True
    assert ret["rows"] == 4
    assert ret["data"] == ALL_DATA[1:]


def test_not_like():
    ret = client.execute("select * from whr_test_t where name not like 'a%';")
    assert ret["success"] == True
    assert ret["rows"] == 4
    assert ret["data"] == ALL_DATA[1:]


# --- IS TRUE / IS FALSE / IS NOT ---

def test_is_true():
    ret = client.execute("select * from whr_test_t where (age > 20) is true;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [ALL_DATA[1], ALL_DATA[2], ALL_DATA[4]]


def test_is_false():
    ret = client.execute("select * from whr_test_t where (age > 100) is false;")
    assert ret["success"] == True
    assert ret["rows"] == 5
    assert ret["data"] == ALL_DATA


def test_is_not_true():
    ret = client.execute("select * from whr_test_t where (age > 100) is not true;")
    assert ret["success"] == True
    assert ret["rows"] == 5
    assert ret["data"] == ALL_DATA


def test_is_not_false():
    ret = client.execute("select * from whr_test_t where (age > 20) is not false;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [ALL_DATA[1], ALL_DATA[2], ALL_DATA[4]]


# --- Nested conditions ---

def test_nested_parentheses():
    ret = client.execute("select * from whr_test_t where ((age = 20) and (score > 80)) or (id = 3);")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [ALL_DATA[0], ALL_DATA[2], ALL_DATA[3]]


def test_deeply_nested_search_condition():
    ret = client.execute("select * from whr_test_t where (id > 0) and ((age = 20) or (age = 30));")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [ALL_DATA[0], ALL_DATA[2], ALL_DATA[3]]


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table whr_test_t;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
