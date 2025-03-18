# test_trans.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

## create mock table:
def test_create_mock_table():
    sql = "create table Class (id string primary key, location string);\n" \
          "create table Student (id string primary key, name string, age int, birth date, class Class);\n"  
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

## insert mock data. 
def test_insert_mock_data():
    sql = "insert into Class values('C001', 'Northwest corner ');\n" \
          "insert into Class values('C002', 'Middle');\n" \
          "insert into Class values('C003', 'South side');\n" \
          "insert into Class values('C004', 'East side');\n" \
          "insert into Student values('S001', 'kail', 10, '2014-10-03', ref(id = 'C001'));\n" \
          "insert into Student values('S002', 'sun', 11, '2013-11-20', ref(id = 'C001'));\n" \
          "insert into Student values('S003', 'ben', 12, '2012-04-23', ref(id = 'C002'));\n" \
          "insert into Student values('S004', 'david', 14, '2010-01-05', ref(id = 'C002'));\n" \
          "insert into Student values('S005', 'kunting', 9, '2015-06-23', ref(id = 'C002'));\n" \
          "insert into Student values('S006', 'bob', 9, '2015-07-07', ref(id = 'C003'));\n" \
          "insert into Student values('S007', 'july', 11, '2013-03-05', ref(id = 'C003'));\n" \
          "insert into Student values('S008', 'alice', 13, '2011-08-08', ref(id = 'C004'));\n" 
    ret = client.execute(sql)
    assert_all(ret)

## test begin -> commit routine.
def test_begin_commit_routine():
    sql = "begin;\n"\
          "insert into Student values('S009', 'benj', 12, '2012-10-08', ref(id = 'C004'));\n" \
          "commit;"
    ret = client.execute(sql)
    assert_all(ret)

## test query data after commit
def test_select_after_begin_commit_routine():
    ret = client.execute("select * from Student where id = 'S009';")
    assert ret["success"] == True
    assert ret["rows"] == 1

## test begin -> commit routine.
def test_begin_rollback_routine():
    sql = "begin;\n"\
          "insert into Student values('S010', 'rose', 9, '2015-03-09', ref(id = 'C004'));\n" \
          "rollback;"
    ret = client.execute(sql)
    assert_all(ret)

## test query data after commit
def test_select_after_begin_rollback_routine():
    ret = client.execute("select * from Student where id = 'S010';")
    assert ret["success"] == True
    assert ret["rows"] == 0
    assert ret["data"] == []

## test auto rollback.
def test_auto_rollback():
    sql = "begin;\n" \
          "insert into Student values('S009', 'holy', 8, '2016-02-08', ref(id = 'C002'));\n" 
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == False


## test commit after auto rollback.
def test_commit_after_auto_rollback():
    sql = "commit;\n" 
    ret = client.execute(sql)
    assert ret["success"] == True


## test query data after commit
def test_select_after_auto_rollback_routine():
    ret = client.execute("select * from Student where name = 'holy';")
    assert ret["success"] == True
    assert ret["rows"] == 0
    assert ret["data"] == []

## test drop table
def test_drop_mock_tables():
    sql = "drop table Student;\n"\
          "drop table Class;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

