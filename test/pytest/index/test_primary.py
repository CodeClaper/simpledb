# test_primary.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

## create one-primary table.
def test_create_one_primary_table():
    sql = "create table Student (id varchar(32) primary key, name varchar(32), age int, master_id varchar(32) not null);"
    ret = client.execute(sql)
    assert ret["success"] == True

## query indexs.
def test_show_indexs1():
    sql = "show index from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{'index_name': 'Student_pri_index', 'table_name': 'Student', 'is_unique': True, 'index_type': 'BTREE', 'columns': 'id'}]

## create mult-primary table.
def test_create_multi_primary_table():
    sql = "create table Student_Courses (student_id varchar(32), course_id varchar(32), encrollment_date date, grade float, primary key (student_id, course_id));"
    ret = client.execute(sql)
    assert ret["success"] == True

## query indexs.
def test_show_indexs2():
    sql = "show index from Student_Courses;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{'index_name': 'Student_Courses_pri_index', 'table_name': 'Student_Courses', 'is_unique': True, 'index_type': 'BTREE', 'columns': 'student_id,course_id'}]

## drop tables.
def test_drop_tables():
    sql = "drop table Student;\n"\
          "drop table Student_Courses;\n"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
