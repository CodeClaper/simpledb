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
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
           {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
           {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"},
           {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
           {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
           {"type": "VAR", "op": "LT", "sflag": "none"}], 
        "sflag": "none"}

def test_complex_sql2():
    ret = client.execute("express select * from Student s, Teacher t where (s.name = 'zhangsan' or t.name = 'Benj' and t.id = 'T001') and (s.sex = 'M' or t.sex = 'M') or t.createTime < '2025-10-10' or t.createTime >= '2020-01-01';")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "VAR", "op": "LT", "sflag": "none"}, {"type": "VAR", "op": "GE", "sflag": "none"}], 
      "sflag": "none"}

def test_complex_sql3():
    ret = client.execute("express select * from Student s, Teacher t where s.age = 10 or (s.grade = 'G1' or (t.age >= 23 AND t.age <=40 AND (t.sex = 'M' or t.sex = 'F'))) ;")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
       {"type": "VAR", "op": "EQ", "sflag": "none"}, 
       {"type": "VAR", "op": "EQ", "sflag": "none"}, 
       {"type": "AND_SET", "children": [{"type": "VAR", "op": "GE", "sflag": "none"}, {"type": "VAR", "op": "LE", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
       {"type": "AND_SET", "children": [{"type": "VAR", "op": "GE", "sflag": "none"}, {"type": "VAR", "op": "LE", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}], 
    "sflag": "none"}

def test_complex_sql4():
    ret = client.execute("express select * from Student where not (birth >= '2010-01-01' AND birth <= '2015-12-31' OR sex = 'M');");
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "LT", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "GT", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}], "sflag": "none"}], 
     "sflag": "none"}

def test_complex_sql5():
    ret = client.execute("express select * from Student where (birth >= '2010-01-01' AND birth <= '2015-12-31' OR sex = 'M') is false;");
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "LT", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}], "sflag": "none"}, 
            {"type": "AND_SET", "children": [{"type": "VAR", "op": "GT", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}], "sflag": "none"}], 
         "sflag": "none"}

def test_complex_sql6():
    ret = client.execute("express select * from Student where not (birth >= '2010-01-01' AND birth <= '2015-12-31' OR sex = 'M') is false;");
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "GE", "sflag": "none"}, {"type": "VAR", "op": "LE", "sflag": "none"}], "sflag": "none"}, 
        {"type": "VAR", "op": "EQ", "sflag": "none"}], 
     "sflag": "none"}

def test_complex_sql7():
    ret = client.execute("express select * from Student where not (birth >= '2010-01-01' AND birth <= '2015-12-31' OR not sex = 'M');");
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "LT", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "GT", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}], 
     "sflag": "none"}

# def test_complex_sql8():
#     ret = client.execute("express select * from Student where not not (birth >= '2010-01-01' AND birth <= '2015-12-31' OR sex = 'M');");
#     assert ret["success"] == True
#     print(json.dumps(ret))
#     assert ret["data"] == {"type": "OR_SET", "children": [
#           {"type": "AND_SET", "children": [{"type": "VAR", "op": "GE"}, {"type": "VAR", "op": "LE"}]}, 
#           {"type": "VAR", "op": "EQ"}
#           ]}

def test_complex_sql9():
    ret = client.execute("express select * from Student s, Teacher t where not (s.name = 'zhangsan' or t.name = 'Benj' and t.id = 'T001') and (s.sex = 'M' or t.sex = 'M') or t.createTime < '2025-10-10' or t.createTime >= '2020-01-01';")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"},
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "NE", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}], "sflag": "none"}, 
        {"type": "VAR", "op": "LT", "sflag": "none"}, 
        {"type": "VAR", "op": "GE", "sflag": "none"}], 
     "sflag": "none"}


def test_complex_sql10():
    ret = client.execute("express select * from Student s, Teacher t where ((s.name = 'zhangsan' or t.name = 'Benj' and t.id = 'T001' and true) or t.id != 'T002');")
    assert ret["success"] == True
    print(json.dumps(ret))
    assert ret["data"] == {"type": "OR_SET", "children": [
        {"type": "VAR", "op": "EQ", "sflag": "none"}, 
        {"type": "AND_SET", "children": [{"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "VAR", "op": "EQ", "sflag": "none"}, {"type": "TRUTH_VALUE", "truth": "true", "sflag": "fail"}], "sflag": "none"}, 
        {"type": "VAR", "op": "NE", "sflag": "none"}], 
    "sflag": "none"}


## test drop table.
def test_drop_table():
    ret = client.execute("drop table Student;")
    assert ret["success"] == True
    ret = client.execute("drop table Teacher;")
    assert ret["success"] == True
