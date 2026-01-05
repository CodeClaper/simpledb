# test_multi_tables.py
from support.db_cli import DbClient
from support.data_mock import generate_single_student, generate_teachers

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


def test_insert_some_students():
    for id in range(1, 10000):
        student = generate_single_student(id)
        sql = f"insert into Student values ('{student["id"]}', '{student["name"]}', {student["age"]},  '{student["grade"]}', '{student["sex"]}', '{student["birth"]}', '{student["phone"]}', '{student["address"]}', '{student["createTime"]}', '{student["teacherId"]}');"
        ret = client.execute(sql)
        assert ret["success"] == True

def test_insert_some_teachers():
    teachers = generate_teachers(1000)
    for teacher in teachers:
        sql = f"insert into Teacher values ('{teacher["id"]}', '{teacher["name"]}', {teacher["age"]}, '{teacher["sex"]}', '{teacher["birth"]}', '{teacher["phone"]}', '{teacher["address"]}', '{teacher["createTime"]}');"
        ret = client.execute(sql)
        assert ret["success"] == True

## test drop table.
def test_drop_table():
    ret = client.execute("drop table Student;")
    assert ret["success"] == True
    ret = client.execute("drop table Teacher;")
    assert ret["success"] == True
