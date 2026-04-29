# test_describe_show.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# --- DESCRIBE tests ---

def test_describe_existing_table():
    sql = "create table desc_test_t1 (id int primary key, name varchar(32), age int);\n" \
          "describe desc_test_t1;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True


def test_describe_nonexistent_table():
    ret = client.execute("describe nonexistent_table_xyz;")
    assert ret["success"] == False


def test_describe_with_desc_keyword():
    sql = "create table desc_test_t2 (id int, name varchar(32));\n" \
          "desc desc_test_t2;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True


# --- SHOW tests ---

def test_show_tables():
    ret = client.execute("show tables;")
    assert ret["success"] == True


def test_show_index_from_table():
    sql = "create table show_test_t1 (id int primary key, name varchar(32));\n" \
          "create index idx_show_test on show_test_t1 (name);\n" \
          "show index from show_test_t1;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
    assert ret[2]["success"] == True


def test_show_index_nonexistent_table():
    ret = client.execute("show index from nonexistent_table_xyz;")
    assert ret["success"] == False


def test_show_syntax_error():
    ret = client.execute("show;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Cleanup ---

def test_cleanup():
    sql = "drop table desc_test_t1;\n" \
          "drop table desc_test_t2;\n" \
          "drop table show_test_t1;"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
