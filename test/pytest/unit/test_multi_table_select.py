# test_alter.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## create tables.
def test_create_mock_table():
    sql = "create table Student (id varchar(32) primary key, name varchar(32), age int, master_id varchar(32) not null);\n"\
          "create table Teacher (id varchar(32) primary key, name varchar(32), class varchar(16));\n"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

## Insert some mocking data.
def test_mock_table_data():
    sql = "insert into Teacher values ('T001', 'sunqing', 'C01');\n"\
          "insert into Teacher values ('T002', 'duli', 'C02');\n"\
          "insert into Student values ('S0001', 'zhangchuran', 10, 'T001');\n" \
          "insert into Student values ('S0002', 'chengzhen', 11, 'T001');\n" \
          "insert into Student values ('S0003', 'dongxiaojun', 8, 'T002');\n"
    ret = client.execute(sql)
    assert_all(ret)

## test select mult-tables.
def test_select_mutl_tables():
    sql = "select * from Student, Teacher;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 6

## test select with condition.
def test_select_with_condition():
    sql = "select * from Student s, Teacher t where s.master_id = t.id;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == True
    assert ret["rows"] == 3
    assert ret["data"] == [
        {'Student.id': 'S0001', 'Student.name': 'zhangchuran', 'age': 10, 'master_id': 'T001', 'Teacher.id': 'T001', 'Teacher.name': 'sunqing', 'class': 'C01'}, 
        {'Student.id': 'S0002', 'Student.name': 'chengzhen', 'age': 11, 'master_id': 'T001', 'Teacher.id': 'T001', 'Teacher.name': 'sunqing', 'class': 'C01'}, 
        {'Student.id': 'S0003', 'Student.name': 'dongxiaojun', 'age': 8, 'master_id': 'T002', 'Teacher.id': 'T002', 'Teacher.name': 'duli', 'class': 'C02'}
    ]

## drop mock table
def test_drop_mock_table():
    sql = "drop table Student;\n"\
          "drop table Teacher;\n"
    ret = client.execute(sql)
    assert_all(ret)

