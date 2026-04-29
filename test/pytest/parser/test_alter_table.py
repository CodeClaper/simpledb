# test_alter_table.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# --- ADD COLUMN tests ---

def test_alter_table_add_column():
    sql = "create table alt_t1 (id int primary key, name varchar(32));\n" \
          "alter table alt_t1 add column age int;"
    ret = client.execute(sql)
    assert_all(ret)


def test_alter_table_add_column_with_position_before():
    sql = "create table alt_t2 (id int primary key, name varchar(32), age int);\n" \
          "alter table alt_t2 add column email varchar(64) before age;"
    ret = client.execute(sql)
    assert_all(ret)


def test_alter_table_add_column_with_position_after():
    sql = "create table alt_t3 (id int primary key, name varchar(32));\n" \
          "alter table alt_t3 add column email varchar(64) after id;"
    ret = client.execute(sql)
    assert_all(ret)


def test_alter_table_add_column_with_default():
    sql = "create table alt_t4 (id int primary key, name varchar(32));\n" \
          "alter table alt_t4 add column status int default 0;"
    ret = client.execute(sql)
    assert_all(ret)


def test_alter_table_add_column_not_null():
    sql = "create table alt_t5 (id int primary key, name varchar(32));\n" \
          "alter table alt_t5 add column email varchar(64) not null;"
    ret = client.execute(sql)
    assert_all(ret)


# --- DROP COLUMN tests ---

def test_alter_table_drop_column():
    sql = "create table alt_t6 (id int primary key, name varchar(32), age int);\n" \
          "alter table alt_t6 drop column age;"
    ret = client.execute(sql)
    assert_all(ret)


def test_alter_table_drop_primary_key_column():
    sql = "create table alt_t7 (id int primary key, name varchar(32));\n" \
          "alter table alt_t7 drop column id;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == False


def test_alter_table_drop_nonexistent_column():
    sql = "create table alt_t8 (id int primary key, name varchar(32));\n" \
          "alter table alt_t8 drop column nonexistent;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == False


# --- Error cases ---

def test_alter_table_nonexistent():
    ret = client.execute("alter table nonexistent_table add column x int;")
    assert ret["success"] == False


def test_alter_table_syntax_error():
    ret = client.execute("alter table;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Cleanup ---

def test_cleanup():
    sql = "drop table alt_t1;\n" \
          "drop table alt_t2;\n" \
          "drop table alt_t3;\n" \
          "drop table alt_t4;\n" \
          "drop table alt_t5;\n" \
          "drop table alt_t6;\n" \
          "drop table alt_t8;"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
