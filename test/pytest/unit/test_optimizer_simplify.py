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
    ret = client.execute("express select * from Student s, Teacher t where (s.name = 'zhangsan' or t.name = 'Benj') and (s.sex = 'M' or t.sex = 'M') or t.createTime < '2025-10-10' or 1 = 1;")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "VAR", "op": "LT", "sflag": "none"}, 
        {"type": "VAR", "op": "EQ", "sflag": "success"}], 
     "sflag": "one of success"}

def test_complex_sql2():
    ret = client.execute("express select * from Student s, Teacher t where (s.name = 'zhangsan' or t.name = 'Benj' and t.id = 'T001' and 1 = 2) and (s.sex = 'M' or t.sex = 'M' or 1 = 1) or t.createTime < '2025-10-10' or t.createTime >= '2020-01-01';")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "success"}], "sflag": "none"}, 
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "fail"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "one of fail"}, 
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "fail"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "one of fail"}, 
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "fail"}, {"type": "VAR", "op": "EQ", "sflag": "success"}], "sflag": "one of fail"}, 
            {"type": "VAR", "op": "LT", "sflag": "none"}, 
            {"type": "VAR", "op": "GE", "sflag": "none"}], 
       "sflag": "none"}


def test_complex_sql3():
    ret = client.execute("express select * from Student s, Teacher t;")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == None

def test_complex_sql4():
    ret = client.execute("express select * from Student s, Teacher t where 1 = 1 and 2 = 2 and 3 != 3;")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "AND_SET", "children": [
        {"type": "VAR", "op": "EQ", "sflag": "success"}, 
        {"type": "VAR", "op": "EQ", "sflag": "success"}, 
        {"type": "VAR", "op": "NE", "sflag": "fail"}],
    "sflag": "one of fail"}

def test_complex_sql5():
    ret = client.execute("express select * from Student s, Teacher t where 1 = 2 or 2 = 3 or 3 != 3;")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "VAR", "op": "EQ", "sflag": "fail"}, 
        {"type": "VAR", "op": "EQ", "sflag": "fail"}, 
        {"type": "VAR", "op": "NE", "sflag": "fail"}],
    "sflag": "all fail"}

def test_complex_sql6():
    ret = client.execute("express select * from Student s, Teacher t where s.name = 'zhangsan' or t.name = 'Benj' and t.id = 'T001' or true;")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
            {"type": "VAR", "op": "EQ", "sflag": "none"}, 
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
            {"type": "TRUTH_VALUE", "truth": "true", "sflag": "success"}], 
         "sflag": "one of success"}

def test_complex_sql7():
    ret = client.execute("express select * from Student where (false);")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "TRUTH_VALUE", "truth": "false", "sflag": "fail"}

def test_complex_sql8():
    ret = client.execute("express select * from Student s, Teacher t where (true and true);")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "AND_SET", "children": [
        {"type": "TRUTH_VALUE", "truth": "true", "sflag": "fail"}, 
        {"type": "TRUTH_VALUE", "truth": "true", "sflag": "fail"}], 
    "sflag": "all success"}

## test drop table.
def test_drop_table():
    ret = client.execute("drop table Student;")
    assert ret["success"] == True
    ret = client.execute("drop table Teacher;")
    assert ret["success"] == True
