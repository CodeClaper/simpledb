# test_explain_express.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# --- EXPLAIN tests ---

def test_explain_select():
    sql = "create table expl_test_t1 (id int primary key, name varchar(32), age int);\n" \
          "insert into expl_test_t1 values (1, 'alice', 20);"
    ret = client.execute(sql)
    assert_all(ret)

    ret = client.execute("explain select * from expl_test_t1;")
    assert ret["success"] == True


def test_explain_select_with_where():
    ret = client.execute("explain select * from expl_test_t1 where id = 1;")
    assert ret["success"] == True


def test_explain_select_with_alias():
    ret = client.execute("explain select id as uid, name from expl_test_t1;")
    assert ret["success"] == True


def test_explain_select_with_function():
    ret = client.execute("explain select count(*) from expl_test_t1;")
    assert ret["success"] == True


# --- EXPRESS tests ---

def test_express_select():
    ret = client.execute("express select * from expl_test_t1;")
    assert ret["success"] == True


def test_express_select_with_where():
    ret = client.execute("express select * from expl_test_t1 where id = 1;")
    assert ret["success"] == True


def test_express_select_with_limit():
    ret = client.execute("express select * from expl_test_t1 limit 10;")
    assert ret["success"] == True


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table expl_test_t1;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
