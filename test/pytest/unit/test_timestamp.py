from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Student(id varchar(8), name varchar(16), age int, class int, sex char, createdTime timestamp, primary key(id));\n"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_insert_some_data():
    sql = "INSERT INTO Student values('S001', 'Jack', 10, 2, 'M', '2025-02-17 00:00:00');\n"\
          "INSERT INTO Student values('S002', 'Modi', 9, 2, 'W', '2025-03-27 11:24:00');\n"\
          "INSERT INTO Student values('S003', 'Benjim', 11, 4, 'M', '2025-12-17 09:20:00');\n"\
          "INSERT INTO Student values('S004', 'Docken', 12, 1, 'M', '2025-03-05 16:10:00');\n"\
          "INSERT INTO Student values('S005', 'Harm', 10, 1, 'W', '2025-11-07 16:00:00');\n"\
          "INSERT INTO Student values('S006', 'Welson', 8, 3, 'M', '2025-08-08 11:20:00');\n"
    ret = client.execute(sql)
    assert_all(ret)
    

def test_insert_ms_timestamp():
    sql = "INSERT INTO Student values('S007', 'Doglas', 11, 1, 'W', '2025-01-09 10:30:01.092');"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_select():
    sql = "select * from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'Jack', 'age': 10, 'class': 2, 'sex': 'M', 'createdTime': '2025-02-17 00:00:00'}, 
        {'id': 'S002', 'name': 'Modi', 'age': 9, 'class': 2, 'sex': 'W', 'createdTime': '2025-03-27 11:24:00'}, 
        {'id': 'S003', 'name': 'Benjim', 'age': 11, 'class': 4, 'sex': 'M', 'createdTime': '2025-12-17 09:20:00'}, 
        {'id': 'S004', 'name': 'Docken', 'age': 12, 'class': 1, 'sex': 'M', 'createdTime': '2025-03-05 16:10:00'}, 
        {'id': 'S005', 'name': 'Harm', 'age': 10, 'class': 1, 'sex': 'W', 'createdTime': '2025-11-07 16:00:00'}, 
        {'id': 'S006', 'name': 'Welson', 'age': 8, 'class': 3, 'sex': 'M', 'createdTime': '2025-08-08 11:20:00'}, 
        {'id': 'S007', 'name': 'Doglas', 'age': 11, 'class': 1, 'sex': 'W', 'createdTime': '2025-01-09 10:30:01'}
    ]

def test_compare_with_date():
    sql = "select * from Student where createdTime = '2025-02-17';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'Jack', 'age': 10, 'class': 2, 'sex': 'M', 'createdTime': '2025-02-17 00:00:00'}, 
    ]

def test_compare_with_date2():
    sql = "select * from Student where createdTime < '2025-02-17';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S007', 'name': 'Doglas', 'age': 11, 'class': 1, 'sex': 'W', 'createdTime': '2025-01-09 10:30:01'}
    ]

## test for drop table.
def test_drop_table():
    sql = "DROP TABLE Student;" 
    ret = client.execute(sql)
    assert ret["success"] == True

