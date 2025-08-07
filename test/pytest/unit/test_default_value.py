# test_default.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## create table with default value but invalid.
def test_invalid_default_value():
    sql = "CREATE TABLE A (id int primary key, name varchar(32) default 0, age int default 0);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Invalid default value for 'name', can`t convert to 'varchar'."

## test conflict default value.
def test_conflict_default_value():
    sql = "CREATE TABLE B (id int primary key, name varchar(32) not null default null, age int);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Invalid default value for 'name'"


## test create table 
def test_create_table_class():
    sql = "CREATE TABLE Class(id varchar(32) primary key, grade varchar(32), master_id varchar(32));"
    ret = client.execute(sql)
    assert ret["success"] == True

## test direct subrow value as default value.
def test_direct_subrow_default_value():
    sql = "CREATE TABLE Student (id int primary key, name varchar(32) default '', age int default 0, phone varchar(13), address varchar(100) default 'unknown', class Class default ('C001', 'Grade-01', 'T003'));"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Default value does not support directly subrow value. You can try indirect refer value, but make sure the referenct exists."

## test not exist refer value
def test_nonexists_refer_value_default_value():
    sql = "CREATE TABLE Student (id int primary key, name varchar(32) default '', age int default 0, phone varchar(13), address varchar(100) default 'unknown', class Class default ref(id ='C001'));"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Try to use refer value as default value, but it does not exist."

## test insert class data.
def test_insert_class_data():
    sql = "Insert into Class values ('C001', 'Grade-001', 'T001');"
    ret = client.execute(sql)
    assert ret["success"] == True

## test alreay exist refer value
def test_already_exists_refer_value_default_value():
    sql = "CREATE TABLE Student (id int primary key, name varchar(32) default '', age int default 0, phone varchar(13), address varchar(100) default 'unknown', class Class default ref(id ='C001'));"
    ret = client.execute(sql)
    assert ret["success"] == True

## test insert Student data.
def test_insert_student_data():
    sql = "INSERT INTO Student(id) values(1);"
    ret = client.execute(sql)
    assert ret["success"] == True

## Select after inserting.
def test_select_after_inserting():
    sql = "SELECT * FROM Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "id": 1, "name": " ", "age": 0, "phone": None, "address": "unknown", "class": { "id": "C001", "grade": "Grade-001", "master_id": "T001"}}]

## drop table
def test_drop_table():
    sql = "drop table Student;\n" \
          "drop table Class;\n"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

