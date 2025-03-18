## test_bd_insert.py
from support.db_cli import DbClient
import random
import uuid

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

names = ["zhangsan", "lisi", "Sun", "July", "Kaili", "James", "Max"]
address = ["beijing", "shanghai", "nanjing", "jinan", "zhengzhou", "xian", "guangzhou", "hongkong", "shenzhen", "wuhan", "zhejiang", "chognqing", "shengdu"]

## mock table
def test_mock_table():
    sql = "create table Student (id varchar(48), name varchar(48), address varchar(100), primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True


## test insert 3000 students
def test_3000_student():
    for i in range(1, 3000):
        sql = f"insert into Student values ('{uuid.uuid4()}', '{names[i % 7]}', '{address[random.randint(0,12)]}');"
        ret = client.execute(sql)
        assert ret["success"] == True


# query after inserting.
def test_query_student():
    sql = "select * from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 2999

## alter table add column.
def test_alter_table_add_column():
    sql = "alter table Student add column phone varchar(13) comment 'your phone' before address;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["message"] == "Add column 'phone' for table 'Student' successfully."

## query after adding new column.
def query_after_add_column():
    sql = "select * from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 2999
    for row in ret["data"]:
        assert row["phone"] == None

## add not null column but not support default value.
def test_add_column_not_column_without_default_value():
    sql = "alter table Student add column class varchar(16) not null;"
    ret = client.execute(sql)
    assert ret['success'] == True

## add row after adding column.
def test_add_data_after_add_column():
    sql = f"insert into Student(id, name, phone, address, class) values ('{uuid.uuid4()}', '{names[0]}', '130028382912', '{address[random.randint(0,12)]}', null);"
    ret = client.execute(sql)
    assert ret['success'] == False
    assert ret['message'] == "Column 'class' can`t be null."


## drop mock table
def test_drop_mock_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True

