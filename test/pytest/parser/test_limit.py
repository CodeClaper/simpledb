# test_limit.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

_table_created = False


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


def test_select_limit_offset_comma_syntax():
    ret = client.execute("select * from lim_test_t limit 1, 3;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_limit_offset_keyword_syntax():
    ret = client.execute("select * from lim_test_t limit 3 offset 2;")
    assert ret["success"] == True
    assert ret["rows"] == 3


def test_select_count_with_limit():
    ret = client.execute("select count(1) from lim_test_t limit 2;")
    assert ret["success"] == True


def test_select_limit_zero():
    ret = client.execute("select * from lim_test_t limit 0;")
    assert ret["success"] == True
    assert ret["rows"] == 0


# --- Limit with WHERE ---

def test_select_limit_with_where():
    ret = client.execute("select * from lim_test_t where id > 2 limit 2;")
    assert ret["success"] == True
    assert ret["rows"] == 2


def test_select_limit_offset_with_where():
    ret = client.execute("select * from lim_test_t where id > 0 limit 1, 2;")
    assert ret["success"] == True
    assert ret["rows"] == 2


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
