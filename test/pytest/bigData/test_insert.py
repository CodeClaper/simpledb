from support.db_cli import DbClient
from support.asserts import assert_all
import uuid

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

# mock table
def test_mock_table():
    sql = "create table Student(id varchar(48), name varchar(48), age int, primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True

## test insert 1000 students
def test_10000_student_insert():
    names = ["zhangsan", "lisi", "Sun", "July", "Kaili", "James", "Max"]
    for i in range(0, 20000):
        sql = f"insert into Student values ('{uuid.uuid4()}', '{names[i % 7]}', {i});"
        ret = client.execute(sql)
        assert ret["success"] == True

## query count
def test_query_count():
    sql = "select count(1) from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 20000 }]


# roll back after delete
def test_roll_back_after_delete():
    sql = "begin;\n"\
          "delete from Student;\n"\
          "select count(1) from Student;\n"\
          "rollback;\n"\
          "select count(1) from Student;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[2]["data"] == [{ "count": 0 }]
    assert ret[4]["data"] == [{ "count": 20000 }]

# drop mock table
def test_drop_mock_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
