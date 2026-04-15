# test_index.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Student (id string primary key, name varchar(32), age int, birth date, address string);"
    ret = client.execute(sql)
    assert ret["success"] == True

## insert mock data. 
def test_insert_mock_data():
    sql = "insert into Student values('S001', 'kail', 10, '2014-10-03', 'beijing');\n" \
          "insert into Student values('S002', 'sun', 11, '2013-11-20', 'shanghai');\n" \
          "insert into Student values('S003', 'ben', 12, '2012-04-23', 'wuhan');\n" \
          "insert into Student values('S004', 'david', 14, '2010-01-05', 'jinan');\n" \
          "insert into Student values('S005', 'kunting', 9, '2015-06-23', 'shijiazhuang');\n" \
          "insert into Student values('S006', 'bob', 9, '2015-07-07', 'guangzhou');\n" \
          "insert into Student values('S007', 'july', 11, '2013-03-05', 'shenzhen');\n" \
          "insert into Student values('S008', 'tail', 13, '2011-08-08', 'xian');\n" 
    ret = client.execute(sql)
    assert_all(ret)

## test like predicate
def test_like_predicate_string():
    sql = "select * from Student where id like '%001';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { 'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'address': 'beijing' }
    ]

## test like predicate
def test_like_predicate_varchar():
    sql = "select * from Student where name like '%unting';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { 'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'address': 'shijiazhuang' }
    ]

## test like predicate for date typpe
def test_like_predicate_date():
    sql = "select * from Student where birth like '2015-%';"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "For like predicate, only support string data type."

## test left wildcard.
def test_like_predicate_left_wildcard():
    sql = "select * from Student where name like '%ail';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { 'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'address': 'beijing' },
        { 'id': 'S008', 'name': 'tail', 'age': 13, 'birth': '2011-08-08', 'address': 'xian' }
    ]

## test right wildcard.
def test_like_predicate_right_wildcard():
    sql = "select * from Student where address like 'sh%';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'address': 'shanghai'}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'address': 'shijiazhuang'}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'address': 'shenzhen'}
    ]

## test full wildcard.
def test_like_predicate_full_wildcard():
    sql = "select * from Student where address like '%ang%';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'address': 'shanghai'}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'address': 'shijiazhuang'}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'address': 'guangzhou'}
    ]

## test no wildcard.
def test_like_predicate_no_wildcard():
    sql = "select * from Student where address like 'jinan';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'address': 'jinan'}
    ]

## test error wildcard.
def test_like_predicate_error_wildcard():
    sql = "select * from Student where address like 'an%g';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == []

## test drop table.
def test_drop_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True

