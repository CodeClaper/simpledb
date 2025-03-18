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

## test begin;
def test_begin():
    sql = "begin;\n"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret2["success"] == True

## insert data. 
def test_client1_insert_data():
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
    ret = client1.execute(sql)
    assert_all(ret)

## test client1 query 
def test_client1_query():
    sql = "select count(1) from Student;\n"
    ret = client1.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 8 }]


## test client2 query.
def test_client2_query():
    sql = "select count(1) from Student;\n"
    ret = client2.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 0 }]


def test_commit():
    sql = "commit;\n"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret2["success"] == True


## test drop table
def test_drop_mock_tables():
    sql = "drop table Student;\n"\
          "drop table Class;"
    ret = client1.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

