# test_index_perf.py
from support.db_cli import DbClient
from support.data_mock import generate_single_student
import math

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table.
def test_create_table():
    sql = "create table Student(id varchar(32) primary key, rid varchar(32), name varchar(48), age int, grade varchar(16), sex varchar(4), birth date, phone string, address string, createTime timestamp);\n"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_insert_mock_data():
    for id in range(1, 100000):
        student = generate_single_student(id)
        sql = f"insert into Student values ('{student["id"]}', '{student["id"]}', '{student["name"]}', {student["age"]},  '{student["grade"]}', '{student["sex"]}', '{student["birth"]}', '{student["phone"]}', '{student["address"]}', '{student["createTime"]}');"
        ret = client.execute(sql)
        assert ret["success"] == True

def test_perf_without_index():
    ret1 = client.execute("select * from Student where id = '99999';")
    ret2 = client.execute("select * from Student where rid = '99999';")
    assert ret1["success"] == True
    assert ret2["success"] == True
    assert ret1["data"] == ret2["data"]
    assert ret1["duration"] * 10 <= ret2["duration"]

## test create name index.
def test_create_rid_index():
    sql = "create index i_rid on Student(rid);"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_perf_with_index():
    ret1 = client.execute("select * from Student where id = '99999';")
    ret2 = client.execute("select * from Student where rid = '99999';")
    assert ret1["success"] == True
    assert ret2["success"] == True
    assert ret1["data"] == ret2["data"]
    assert math.isclose(ret1["duration"], ret2["duration"], rel_tol=1e-3, abs_tol=1e-3)

def test_create_phone_index():
    sql = "create index i_phone on Student(phone);"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_plain_phone_index():
    sql = "explain select * from Student where phone = '18378299110';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] ==  {'stmt_type': 'select', 'index': 'i_phone', 'only_count': False, 'only_scan': False }
    
def test_perf_i_phone():
    ret1 = client.execute("select * from Student where id = '99999';")
    assert ret1["success"] == True
    ret2 = client.execute(f"select * from Student where phone = '{ret1["data"][0]["phone"]}';")
    assert ret2["success"] == True
    assert ret1["data"] == ret2["data"]
    assert math.isclose(ret1["duration"], ret2["duration"], rel_tol=1e-3, abs_tol=1e-3)

## test drop table.
def test_drop_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
