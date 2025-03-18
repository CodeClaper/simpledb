## test_bd_insert.py
from support.db_cli import DbClient
import random
import uuid

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

# mock table
def test_mock_table():
    sql = "create table Student(id varchar(48), name varchar(48), age int, primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True

## test insert 1000 students
def test_1000_student_insert():
    names = ["zhangsan", "lisi", "Sun", "July", "Kaili", "James", "Max"]
    for i in range(1, 1000):
        sql = f"insert into Student values ('{uuid.uuid4()}', '{names[i % 7]}', { random.randint(6, 15) });"
        ret = client.execute(sql)
        assert ret["success"] == True


## test add new column
def test_add_new_column():
    sql = "alter table Student add column sex char default 'M' comment 'M if man Woman if W' before age;"
    ret = client.execute(sql)
    assert ret["success"] == True


## desc table
def test_desc_tables():
    sql = "desc Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][2] == { "field": "sex", "key": None, "type": "char", "length": 1, "array": False, "default": "M", "comment": "M if man Woman if W" }

## count
def test_select_count():
    sql = "select count(sex) from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0] == { "count": 999 }


## select add column.
def test_select_add_column():
    sql = "select * from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 999


## test add already exists column.
def test_add_alreay_exists_column():
    sql = "alter table Student add column name varchar(32);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Table 'Student' already exists column 'name'."


## drop column.
def test_drop_column():
    sql = "alter table Student drop column `age`;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["message"] == "Drop column 'age' for table 'Student' successfully."


## drop column.
def test_select_after_drop_column():
    sql = "select age from Student;"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Unknown column name 'age'. "


# select after drop column:
def test_select_count_after_drop_column():
    sql = "select count(1) from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 999 }]
    

## try drop primary-key column
def test_drop_primary_key_column():
    sql = "alter table Student drop column `id`;"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Column 'id' is priamry-key, not allowed to drop."


## add column after drop 
def test_add_afer_drop_column():
    sql = "alter table Student add column `age` int default 0;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["message"] == "Add column 'age' for table 'Student' successfully."


## query add column.
def test_query_add_column():
    sql = "select age from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 999
    assert ret["data"][11] ==  { "age": 0 }
    assert ret["data"][100] ==  { "age": 0 }
    assert ret["data"][503] ==  { "age": 0 }


# drop mock table
def test_drop_mock_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
