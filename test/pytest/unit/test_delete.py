## delete_test.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## Firstly create table.
def test_create_table():
    sql = "create table A(id string, name string, score float, primary key (id));"
    ret = client.execute(sql)
    assert ret["success"] == True

## insert some data
def test_batch_insert():
    sql = "insert into A values('S001', 'BOBN', 98.3);\n" \
          "insert into A values('S002', 'JKD', 99.2);\n" \
          "insert into A values('S003', 'MOOR', 87.2);\n" \
          "insert into A values('S004', 'LOU', 86.2);\n" \
          "insert into A values('S005', 'LISA', 33.2);" 
    ret = client.execute(sql)
    assert_all(ret)

## test delete with condition.
def test_delete_with_condition():
    sql = "delete from A where name = 'BOBN';\n"\
          "select * from A;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[0]["rows"] == 1
    assert ret[1]["success"] == True
    assert ret[1]["rows"] == 4

## test delete with condition.
def test_delete_with_fake_condition():
    sql = "delete from A where name = 'BOB';\n"\
          "select * from A;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[0]["rows"] == 0
    assert ret[1]["success"] == True
    assert ret[1]["rows"] == 4

## test delete with condition.
def test_delete_without_condition():
    sql = "delete from A;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 4

## Finally drop table.
def test_drop_table():
    sql = "drop table A;"
    ret = client.execute(sql)
    assert ret["success"] == True
