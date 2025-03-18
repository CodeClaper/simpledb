from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table A(id string primary key, name string);\n" \
          "create table B(id string primary key, a A);"
    ret = client.execute(sql)
    assert_all(ret)

## test drop table which is refred by others.
def test_drop_refered_table():
    ret = client.execute("drop table A;")
    assert ret["success"] == False

## test drop table
def test_drop_table():
    sql = "drop table B;\n" \
          "drop table A;"
    ret = client.execute(sql)
    assert_all(ret)

## test drop not exist table.
def test_drop_not_exist_table():
    ret = client.execute("drop table X;")
    assert ret["success"] == False
    assert ret["message"] == "Table 'X' not exists."

