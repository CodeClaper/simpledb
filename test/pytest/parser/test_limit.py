# test_limit.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

_table_created = False

ALL_DATA = [
    {'id': 1, 'name': 'a'},
    {'id': 2, 'name': 'b'},
    {'id': 3, 'name': 'c'},
    {'id': 4, 'name': 'd'},
    {'id': 5, 'name': 'e'},
    {'id': 6, 'name': 'f'},
]


def setup_data():
    global _table_created
    if not _table_created:
        sql = "create table lim_test_t (id int primary key, name varchar(32));\n" \
              "insert into lim_test_t values (1, 'a');\n" \
              "insert into lim_test_t values (2, 'b');\n" \
              "insert into lim_test_t values (3, 'c');\n" \
              "insert into lim_test_t values (4, 'd');\n" \
              "insert into lim_test_t values (5, 'e');\n" \
              "insert into lim_test_t values (6, 'f');"
        ret = client.execute(sql)
        assert_all(ret)
        _table_created = True


# --- LIMIT clause syntax tests ---

def test_select_limit_rows_only():
    setup_data()
    ret = client.execute("select * from lim_test_t limit 3;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == ALL_DATA[:3]


def test_select_limit_offset_comma_syntax():
    ret = client.execute("select * from lim_test_t limit 1, 3;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == ALL_DATA[1:4]


def test_select_limit_offset_keyword_syntax():
    ret = client.execute("select * from lim_test_t limit 3 offset 2;")
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == ALL_DATA[2:5]


def test_select_limit_zero():
    ret = client.execute("select * from lim_test_t limit 0;")
    assert ret["success"] == True
    assert ret["rows"] == 0
    assert ret["data"] == []


# --- Limit with WHERE ---

def test_select_limit_with_where():
    ret = client.execute("select * from lim_test_t where id > 2 limit 2;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == ALL_DATA[2:4]


def test_select_limit_offset_with_where():
    ret = client.execute("select * from lim_test_t where id > 0 limit 1, 2;")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == ALL_DATA[1:3]


# --- Aggregate functions with LIMIT ---

def test_select_count_with_limit_rows_only():
    ret = client.execute("select count(1) from lim_test_t limit 2;")
    assert ret["success"] == True
    assert ret["data"] == [{'count': 6}]


def test_select_count_with_limit_offset():
    ret = client.execute("select count(1) from lim_test_t limit 1 offset 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'count': 0}]


def test_select_count_with_limit_and_where():
    ret = client.execute("select count(1) from lim_test_t where id > 3 limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'count': 3}]


def test_select_sum_with_limit_rows_only():
    ret = client.execute("select sum(id) from lim_test_t limit 1;")
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [{'sum': 21}]


def test_select_sum_with_limit_offset():
    ret = client.execute("select sum(id) from lim_test_t limit 1 offset 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'sum': 0}]


def test_select_sum_with_limit_and_where():
    ret = client.execute("select sum(id) from lim_test_t where id < 4 limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'sum': 6}]


def test_select_avg_with_limit_rows_only():
    ret = client.execute("select avg(id) from lim_test_t limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'avg': 3.5}]


def test_select_avg_with_limit_offset():
    ret = client.execute("select avg(id) from lim_test_t limit 1 offset 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'avg': None}]


def test_select_avg_with_limit_and_where():
    ret = client.execute("select avg(id) from lim_test_t where id <= 4 limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'avg': 2.5}]


def test_select_max_with_limit_rows_only():
    ret = client.execute("select max(id) from lim_test_t limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'max': 6}]


def test_select_max_with_limit_offset():
    ret = client.execute("select max(id) from lim_test_t limit 1 offset 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'max': None}]


def test_select_max_with_limit_and_where():
    ret = client.execute("select max(id) from lim_test_t where id < 5 limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'max': 4}]


def test_select_min_with_limit_rows_only():
    ret = client.execute("select min(id) from lim_test_t limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'min': 1}]


def test_select_min_with_limit_offset():
    ret = client.execute("select min(id) from lim_test_t limit 1 offset 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'min': None}]


def test_select_min_with_limit_and_where():
    ret = client.execute("select min(id) from lim_test_t where id > 2 limit 1;")
    assert ret["success"] == True
    assert ret["data"] == [{'min': 3}]


# --- Error cases ---

def test_limit_negative():
    ret = client.execute("select * from lim_test_t limit -1;")
    assert ret["success"] == False


def test_limit_offset_negative():
    ret = client.execute("select * from lim_test_t limit 10 offset -1;")
    assert ret["success"] == False


def test_select_offset_then_limit_wrong_order():
    # The grammar only supports LIMIT first, not OFFSET first
    ret = client.execute("select * from lim_test_t offset 1 limit 5;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table lim_test_t;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
