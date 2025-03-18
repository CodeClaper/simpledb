## test_insert_multi_values.py

from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")


## create table
def test_create_table():
    sql = "create table Student (id string primary key, name varchar(32), age int, sex char);\n"
    ret = client.execute(sql)
    assert ret["success"] == True


## multi-values insert.
def test_multi_values_insert():
    sql = "insert into Student values('S001', 'zhangsan', 12, 'M'),"\
          "('S002', 'lisi', 11, 'F'),"\
          "('S003', 'duli', 9, 'F'),"\
          "('S004', 'sandi', 10, 'M'),"\
          "('S005', 'junay', 12, 'F');"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 5


## conflict multi-values. 
def test_conflict_multi_values():
    sql = "insert into Student values('S006', 'dongfan', 12, 'F'),"\
          "('S007', 'junly', 11, 'F'),"\
          "('S007', 'tom', 9, 'M');"
    ret = client.execute(sql)
    assert ret["success"] == False


## query after conflict multi-values.
def test_query_after_multi_values():
    sql = "select * from Student;"
    ret = client.execute(sql)
    assert ret["success"] ==  True
    assert ret["rows"] == 5


## drop table
def test_drop_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True

