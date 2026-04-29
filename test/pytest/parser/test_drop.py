# test_drop.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


def test_drop_table():
    sql = "create table drop_test_t1 (id int primary key, name varchar(32));\n" \
          "drop table drop_test_t1;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True


def test_drop_nonexistent_table():
    ret = client.execute("drop table nonexistent_table_xyz;")
    assert ret["success"] == False


def test_drop_index():
    sql = "create table drop_test_t2 (id int primary key, name varchar(32));\n" \
          "create index idx_drop_test on drop_test_t2 (name);\n" \
          "drop index idx_drop_test;\n" \
          "drop table drop_test_t2;"
    ret = client.execute(sql)
    assert_all(ret)


def test_drop_nonexistent_index():
    ret = client.execute("drop index nonexistent_index_xyz;")
    assert ret["success"] == False


def test_drop_table_syntax_error():
    ret = client.execute("drop;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_drop_index_syntax_error():
    ret = client.execute("drop index;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def teardown_module(module):
    client.close()
