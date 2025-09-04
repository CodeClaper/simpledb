# select_test.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

## create mock tables;
def test_create_mock_table():
    sql = "create table Class (id string primary key, location string, studentNum int);\n" \
          "create table Student (id string primary key, name string, age int, birth date, class Class);\n"  
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

## insert mock data. 
def test_insert_mock_data():
    sql = "insert into Class values('C001', 'Northwest corner', 32);\n" \
          "insert into Class values('C002', 'Middle', 28);\n" \
          "insert into Class values('C003', 'South side', 33);\n" \
          "insert into Class values('C004', 'East side', 30);\n" \
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

## test in string values.
def test_in_string_values():
    sql = "select * from Student where id in ('S001', 'S002', 'S008');"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test in int values.
def test_in_int_values():
    sql = "select * from Student where age in (10, 100, 13);"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]
 
## test in date values.
def test_in_date_values():
    sql = "select * from Student where birth in ('2014-10-03', '2015-06-23');"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}
    ]

## test in reference values.
def test_in_indirect_refer_values():
    sql = "select * from Student where class in (ref(id = 'C001'),  ref(id = 'C004'));"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test in reference values.
def test_in_direct_refer_values():
    sql = "select * from Student where class in (('C001', 'Northwest corner', 32), ('C004', 'East side', 30));"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == 'Not allowed use directly refer value in search condition.'
 
## drop mock tables   
def test_drop_mock_tables():
    sql = "drop table Student;\n"\
          "drop table Class;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

