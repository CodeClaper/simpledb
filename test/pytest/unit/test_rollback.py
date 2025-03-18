# test_trans.py
from support.db_cli import DbClient
from support.asserts import assert_all

client1 = DbClient("127.0.0.1", 4083)
client2 = DbClient("127.0.0.1", 4083)
client1.login("root", "Zc120130211")
client2.login("root", "Zc120130211")

## create mock table:
def test_create_mock_table():
    sql = "create table Class (id string primary key, location string);\n" \
          "create table Student (id string primary key, name string, age int, birth date, class Class);\n"  
    ret = client1.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

# test nothing-to-do rollback.
def test_nothing_to_do_rollback():
    sql = "rollback;"
    ret = client1.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Not in any transaction, please begin a transaction"


## begin transaction and insert one row data.
def test_insert_data():
    sql = "begin;\n" \
          "insert into Student values ('S001', 'zhangsan', 10, '2014-10-12', ('C001', 'mid of third'));\n"
    ret = client1.execute(sql)
    assert_all(ret)


## query first
def test_select_data1():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True and ret1["rows"] == 1
    assert ret2["success"] == True and ret2["rows"] == 0


## rollback
def test_rollback():
    sql = "rollback;"
    ret = client1.execute(sql)
    assert ret["success"] == True 


## query first
def test_select_data2():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True and ret1["rows"] == 0
    assert ret2["success"] == True and ret2["rows"] == 0


## begin transaction and insert one row data.
def test_insert_data_agin():
    sql = "begin;\n" \
          "insert into Student values ('S001', 'zhangsan', 10, '2014-10-12', ('C001', 'mid of third'));\n"
    ret = client1.execute(sql)
    assert_all(ret)

## query first
def test_select_data3():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True and ret1["rows"] == 1
    assert ret2["success"] == True and ret2["rows"] == 0

## begin transaction and insert one row data.
def test_commit_after_insert():
    sql = "commit;" 
    ret = client1.execute(sql)
    assert ret["success"] == True


## query first
def test_select_data4():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True and ret1["rows"] == 1
    assert ret2["success"] == True and ret2["rows"] == 1


def test_update_under_transaction():
    sql = "begin;\n" \
          "update Student set name = 'lisi' where id = 'S001';\n"
    ret = client1.execute(sql)
    assert ret[0]["success"] == True 
    assert ret[1]["success"] == True and ret[1]["rows"] == 1

def test_select_after_update():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret1["data"] == [{"id": "S001", "name": "lisi", "age": 10, "birth": "2014-10-12", 
                            "class": {"id": "C001", "location": "mid of third"}}]
    assert ret2["success"] == True
    assert ret2["data"] == [{"id": "S001", "name": "zhangsan", "age": 10, "birth": "2014-10-12", 
                            "class": {"id": "C001", "location": "mid of third"}}]



## rollback
def test_rollback_after_update():
    sql = "rollback;"
    ret = client1.execute(sql)
    assert ret["success"] == True 

def test_select_after_update2():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret1["data"] == [{"id": "S001", "name": "zhangsan", "age": 10, "birth": "2014-10-12", 
                            "class": {"id": "C001", "location": "mid of third"}}]
    assert ret2["success"] == True
    assert ret2["data"] == [{"id": "S001", "name": "zhangsan", "age": 10, "birth": "2014-10-12", 
                            "class": {"id": "C001", "location": "mid of third"}}]


def test_delete_under_transaction():
    sql = "begin;\n" \
          "delete from Student where id = 'S001';\n"
    ret = client1.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True and ret[1]["rows"] == 1

def test_select_after_delete():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret1["data"] == []
    assert ret2["success"] == True
    assert ret2["data"] == [{"id": "S001", "name": "zhangsan", "age": 10, "birth": "2014-10-12", 
                            "class": {"id": "C001", "location": "mid of third"}}]
## rollback
def test_rollback_after_delete():
    sql = "rollback;"
    ret = client1.execute(sql)
    assert ret["success"] == True 

def test_select_after_delete2():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret1["data"] == [{"id": "S001", "name": "zhangsan", "age": 10, "birth": "2014-10-12", 
                            "class": {"id": "C001", "location": "mid of third"}}]
    assert ret2["success"] == True
    assert ret2["data"] == [{"id": "S001", "name": "zhangsan", "age": 10, "birth": "2014-10-12", 
                            "class": {"id": "C001", "location": "mid of third"}}]

## test drop table
def test_drop_mock_tables():
    sql = "drop table Student;\n"\
          "drop table Class;"
    ret = client1.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

