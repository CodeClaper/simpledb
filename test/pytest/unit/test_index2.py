# test_index2.py
from support.db_cli import DbClient
from support.data_mock import generate_students

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table.
def test_create_table():
    sql = "create table Student(id string primary key, name varchar(48), age int, grade varchar(16), sex varchar(4), birth date, phone string, address string, createTime timestamp);\n"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_insert_mock_data():
    students = generate_students(1000);
    for student in students:
        sql = f"insert into Student values ('{student["id"]}', '{student["name"]}', {student["age"]},  '{student["grade"]}', '{student["sex"]}', '{student["birth"]}', '{student["phone"]}', '{student["address"]}', '{student["createTime"]}');"
        ret = client.execute(sql)
        assert ret["success"] == True

## test create name index.
def test_create_name_index():
    sql = "create index name_index on Student (name);"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_query_by_index():
    sql = "select * from Student where id = 999;"
    ret = client.execute(sql)
    assert ret["success"] == True
    data = ret["data"]
    name = data[0]["name"]
    sql = f"select * from Student where name = '{name}';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] >= 1

## test drop table.
def test_drop_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
