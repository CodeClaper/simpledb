# create_test.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    ret = client.execute("create table A(id varchar(32), name varchar(16), age int, class int, sex char,  score float, phone varchar(11), address varchar(48), primary key(id));")
    assert ret["success"] == True

## test create already exist table.
def test_create_already_exist_table():
    ret = client.execute("create table A (id varchar(32), name varchar(16), age int, address varchar(48), primary key(id));")
    assert ret["success"] == False

## test create already exist table.
def test_create_table_lack_primary_key():
    ret = client.execute("create table F(id varchar(32), name varchar(16), age int, address varchar(48));")
    assert ret["success"] == True

## test create already exist table.
def test_create_table_unknown_data_type():
    ret = client.execute("create table G (id varchar(32), name varchar(16), age int, address text, primary key(id));")
    assert ret["success"] == False
    assert ret["message"] == "Table 'text' not exists."


## test create duplicate column.
def test_create_table_duplicate_column():
    ret = client.execute("create table H(id varchar(32), name varchar(16), age int, name varchar(32), primary key(id));")
    assert ret["success"] == False
    assert ret["message"] == "Column 'name' already exists, not allowd duplicate defination."

## test create test without user-level primary key
def test_create_table_without_priamry_key():
    sql = "CREATE TABLE Pointer(x int, y int);\n" \
          "CREATE TABLE Circle(r float, p Pointer);\n"
    ret = client.execute(sql)
    assert_all(ret)

## test create no columns table.
def test_create_none_column_table():
    sql = "CREATE TABLE NON_TABLE();"
    ret = client.execute(sql)
    assert ret["success"] == False

## test create table with conflict default value define.
def test_create_conflict_default_value():
    sql = "create table Student(id varchar(32) primary key, name varchar(64) not null default null, age int);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Invalid default value for 'name'"

def test_create_system_reserved_table():
    sql = "create table sys_string (id varchar(32), body varchar(1024));"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Table 'sys_string' is system reserved, not allowd duplication."

## test drop table
def test_drop_table():
    sql = "DROP TABLE A;\n"\
          "DROP TABLE F;\n"\
          "DROP TABLE Circle;\n" \
          "DROP TABLE Pointer;\n"
    ret = client.execute(sql)
    assert_all(ret)
