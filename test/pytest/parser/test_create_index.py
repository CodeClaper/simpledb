# test_create_index.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


def test_create_index_single_column():
    sql = "create table cidx_t1 (id int primary key, name varchar(32), age int);\n" \
          "create index idx_name on cidx_t1 (name);"
    ret = client.execute(sql)
    assert_all(ret)


def test_create_index_multiple_columns():
    ret = client.execute("create index idx_multi on cidx_t1 (name, age);")
    assert ret["success"] == True


def test_create_unique_index():
    sql = "create table cidx_t2 (id int primary key, email varchar(64));\n" \
          "create unique index idx_email on cidx_t2 (email);"
    ret = client.execute(sql)
    assert_all(ret)


def test_create_index_nonexistent_table():
    ret = client.execute("create index idx_bad on nonexistent_table (col);")
    assert ret["success"] == False


def test_create_unique_index_nonexistent_table():
    ret = client.execute("create unique index idx_bad2 on nonexistent_table (col);")
    assert ret["success"] == False


def test_create_index_duplicate_name():
    ret = client.execute("create index idx_name on cidx_t1 (age);")
    assert ret["success"] == False


# --- Cleanup ---

def test_cleanup():
    sql = "drop table cidx_t2;\n" \
          "drop table cidx_t1;"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
