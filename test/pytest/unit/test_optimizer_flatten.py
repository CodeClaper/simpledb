# insert_optimizer_flatten.py
import json
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table.
def test_create_table():
    sql = "create table Student(id varchar(32) primary key, name varchar(48), age int, grade varchar(16), sex varchar(4), birth date, phone string, address string, createTime timestamp, teacherId varchar(32));\n" 
    ret = client.execute(sql)
    assert ret["success"] == True
    sql = "create table Teacher(id varchar(32) primary key, name varchar(48), age int,  sex varchar(4), birth date, phone string, address string, createTime timestamp);\n" 
    ret = client.execute(sql)
    assert ret["success"] == True

def test_complex_sql1():
    ret = client.execute("express select * from Student s, Teacher t where (s.name = 'zhangsan' or t.name = 'Benj') and (s.sex = 'M' or t.sex = 'M') or t.createTime < '2025-10-10';")
    assert ret["success"] == True
    assert ret["data"] == {"type": "OR_SET", "children": ["VAR", {"type": "AND_SET", "children": ["VAR", "VAR"]}, {"type": "AND_SET", "children": ["VAR", "VAR"]}, {"type": "AND_SET", "children": ["VAR", "VAR"]}, {"type": "AND_SET", "children": ["VAR", "VAR"]}]}

def test_complex_sql2():
    ret = client.execute("express select * from Student s, Teacher t where (s.name = 'zhangsan' or t.name = 'Benj' and t.id = 'T001') and (s.sex = 'M' or t.sex = 'M') or t.createTime < '2025-10-10' or t.createTime >= '2020-01-01';")
    assert ret["success"] == True
    assert ret["data"] == {"type": "OR_SET", "children": ["VAR", "VAR", {"type": "AND_SET", "children": ["VAR", "VAR", "VAR"]}, {"type": "AND_SET", "children": ["VAR", "VAR"]}, {"type": "AND_SET", "children": ["VAR", "VAR", "VAR"]}, {"type": "AND_SET", "children": ["VAR", "VAR"]}]}

def test_complex_sql3():
    ret = client.execute("express select * from Student s, Teacher t where s.age = 10 or (s.grade = 'G1' or (t.age >= 23 AND t.age <=40 AND (t.sex = 'M' or t.sex = 'F'))) ;")
    assert ret["success"] == True
    assert ret["data"] == {"type": "OR_SET", "children": [{"type": "AND_SET", "children": ["VAR", "VAR", "VAR"]}, {"type": "AND_SET", "children": ["VAR", "VAR", "VAR"]}, "VAR", "VAR"]}

## test drop table.
def test_drop_table():
    ret = client.execute("drop table Student;")
    assert ret["success"] == True
    ret = client.execute("drop table Teacher;")
    assert ret["success"] == True
