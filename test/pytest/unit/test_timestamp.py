from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Student(id varchar(8), name varchar(16), age int, class int, sex char, createdTime timestamp, primary key(id));\n"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_insert_timestamp1():
    sql = "INSERT INTO Student values('S001', 'jack', 10, 2, 'M', '2025-02-17 10:20:00');"
    ret = client.execute(sql)
    assert ret["success"] == True


def test_insert_timestamp2():
    sql = "INSERT INTO Student values('S002', 'Bob', 11, 2, 'M', '2025-02-17 10:30:01.092');"
    ret = client.execute(sql)
    assert ret["success"] == True


def test_select():
    sql = "select id, name, createdTime from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { "id": "S001", "name": "jack", "createdTime": "2025-02-17 10:20:00"},
        { "id": "S002", "name": "Bob", "createdTime": "2025-02-17 10:30:01"},
    ]

## test for drop table.
def test_drop_table():
    sql = "DROP TABLE Student;" 
    ret = client.execute(sql)
    assert ret["success"] == True

