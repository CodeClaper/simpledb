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
 
## test or search condition.
def test_or_search_condition():
    sql = "select * from Student where age = 9 or (class).id = 'C001';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test and boolean factro.
def test_and_boolean_factor():
    sql = "select * from Student where age > 10 and birth < '2013-01-01';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test not boolean test.
def test_not_boolean_test():
    sql = "select * from Student where not age > 9;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test is truth value.
def test_is_truth_value():
    sql = "select * from Student where class = ref(id = 'C002') is true;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}
    ]

## test is not truth value.
def test_is_not_truth_value():
    sql = "select * from Student where (class).id = 'C001' is false;"; 
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test with paren.
def test_with_paren():
    sql = "select * from Student where (age > 11);"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test with comparing two int values. 
def test_with_compare_values():
    sql = "select * from Student where 1 = 1;"
    ret = client.execute(sql )
    assert ret["success"] == True
    assert ret["data"] == [ 
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test with reverse 
def test_with_reversed():
    sql = "select * from Student where 9 = age;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}} 
    ]


## test with compare tow int vlaues
def test_with_compare_values2():
    sql = "select * from Student where 1 = 2;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == []

## test with compare calculate
def test_with_compare_calculate():
    sql = "select * from Student where (1 + 1 ) = 2;"
    ret = client.execute(sql )
    assert ret["success"] == True
    assert ret["data"] == [ 
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test with compare calculate
def test_with_compare_calculate2():
    sql = "select * from Student where age = (4 + 5);"
    ret = client.execute(sql )
    assert ret["success"] == True
    assert ret["data"] == [ 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test with compare calculate
def test_with_compare_calculate3():
    sql = "select * from Student where age / 3 = 3;"
    ret = client.execute(sql )
    assert ret["success"] == True
    assert ret["data"] == [ 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test with compare calculate
def test_with_compare_calculate4():
    sql = "select * from Student where age + 2 < 12;"
    ret = client.execute(sql )
    assert ret["success"] == True
    assert ret["data"] == [ 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test with compare calculate
def test_with_compare_calculate5():
    sql = "select * from Student where age * 2 < 19;"
    ret = client.execute(sql )
    assert ret["success"] == True
    assert ret["data"] == [ 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test with aggregate function.
def test_with_compare_aggregate_function():
    sql = "select * from Student where sum(age) > 10;"
    ret = client.execute(sql )
    assert ret["success"] == False
    assert ret["message"] == "Aggregate function not allowd in where."
 
## test compare with string values.
def test_compare_with_string_value():
    sql = "select * from Student where age = '9';"
    ret = client.execute(sql )
    assert ret["success"] == True
    assert ret["data"] == [ 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test compare wrong type.
def test_compare_wrong_type():
    sql = "select * from Student where age = 'BIG';"
    ret = client.execute(sql )
    assert ret["success"] == False
    assert ret["message"] == "Invalid input BIG for type int."

## test wrong date string.
def test_compare_wrong_date_string():
    sql = "select * from Student where birth = 'XXXX-XX-XX';"
    ret = client.execute(sql )
    print(ret)
    assert ret["success"] == False
    assert ret["message"] == "Invalid input XXXX-XX-XX for type date."

## test wrong date string.
def test_compare_wrong_date_string2():
    sql = "select * from Student where birth = '2025-03-0X';"
    ret = client.execute(sql )
    print(ret)
    assert ret["success"] == False
    assert ret["message"] == "Invalid input 2025-03-0X for type date."

## test compare date and timstamp .
def test_compare_date_timestamp():
    sql = "select * from Student where birth = '2015-07-07 10:12:00';"
    ret = client.execute(sql )
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test complex search condition. 
def test_complex_search_condition1():
    sql = "select * from Student where age > 9 and age < 12 or (class).studentNum <= 32 and (class).studentNum >= 30;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}},
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test complex search condition. 
def test_complex_search_condition2():
    sql = "select * from Student where (age > 9 and age < 12 or (class).studentNum <= 32 and (class).studentNum >= 30) is false;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## test complex search condition. 
def test_complex_search_condition3():
    sql = "select * from Student where not (age > 9 and age < 12 or (class).studentNum <= 32 and (class).studentNum >= 30) is false;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}},
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test complex search condition. 
def test_complex_search_condition4():
    sql = "select * from Student where age > 9 and age < 12 or (class).studentNum <= 32 and (class).studentNum < 30 is false;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}},
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## test complex search condition. 
def test_complex_search_condition5():
    sql = "select * from Student where age > 9 and not age >= 12 or (class).studentNum <= 32 and (class).studentNum < 30 is false;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}},
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side', 'studentNum': 30}}
    ]

## drop mock tables   
def test_drop_mock_tables():
    sql = "drop table Student;\n"\
          "drop table Class;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True


def teardown_module(module):
    client.close()
