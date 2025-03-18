# test_default.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## create table with default value but invalid.
def test_invalid_default_value():
    sql = "CREATE TABLE A (id int primary key, name varchar(32) default 0, age int default 0);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Invalid default value for 'name', can`t convert to 'varchar'."

## test conflict default value.
def test_conflict_default_value():
    sql = "CREATE TABLE B (id int primary key, name varchar(32) not null default null, age int);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Invalid default value for 'name'"

## create table with default value
def test_right_default_value():
    sql = "CREATE TABLE Student (id int primary key, name varchar(32) default '', age int default 0, phone varchar(13), address varchar(100) default 'unknown');"
    ret = client.execute(sql)
    assert ret["success"] == True

## create table with default value
def test_insert_values():
    sql = "INSERT INTO Student(id) values(1);"
    ret = client.execute(sql)
    assert ret["success"] == True

## Select after inserting.
def test_select_after_inserting():
    sql = "SELECT * FROM Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "id": 1, "name": " ", "age": 0, "phone": None, "address": "unknown"}]

## drop table
def test_drop_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True

