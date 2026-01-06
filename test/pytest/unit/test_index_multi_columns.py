# test_index_multi-columns.py
from support.db_cli import DbClient
from support.data_mock import generate_single_student
import math

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table.
def test_create_table():
    sql = "create table Student(id varchar(32) primary key, name varchar(48), age int, grade varchar(16), sex varchar(4), birth date, phone string, address string, createTime timestamp);\n"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_insert_mock_data():
    for id in range(1, 100000):
        student = generate_single_student(id)
        sql = f"insert into Student values ('{student["id"]}', '{student["name"]}', {student["age"]},  '{student["grade"]}', '{student["sex"]}', '{student["birth"]}', '{student["phone"]}', '{student["address"]}', '{student["createTime"]}');"
        ret = client.execute(sql)
        assert ret["success"] == True


def test_create_multi_columns_index():
    sql = "create index my_index on Student(name, phone, address);"
    ret = client.execute(sql)
    assert ret["success"] == True


def test_show_index():
    sql = "show index from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
            {'index_name': 'Student_pri_index', 'table_name': 'Student', 'is_unique': True, 'index_type': 'BTREE', 'columns': 'id'}, 
            {'index_name': 'my_index', 'table_name': 'Student', 'is_unique': False, 'index_type': 'BTREE', 'columns': 'name,phone,address'}
        ]

def test_index_hit():
    sql = "explain select * from Student where name = 'zhangsan' and phone='13861698001' and address='上海市西湖区科技路78号';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] ==  {'stmt_type': 'select', 'index': 'my_index', 'only_count': False, 'only_scan': False}

def test_primary_index_hit():
    sql = "explain select * from Student where id = '98989';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] ==  {'stmt_type': 'select', 'index': 'primary', 'only_count': False, 'only_scan': False}

def test_perf_my_index():
    ret1 = client.execute("select * from Student where id = '1';")
    assert ret1["success"] == True
    assert ret1["rows"] == 1
    row = ret1["data"][0]
    ret2 = client.execute(f"select * from Student where name = '{row["name"]}' and phone='{row["phone"]}' and address='{row["address"]}';")
    assert ret2["success"] == True
    assert ret2["rows"] >= 1
    assert math.isclose(ret1["duration"], ret2["duration"], rel_tol=1e-2, abs_tol=1e-2)

## test drop table.
def test_drop_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
