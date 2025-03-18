# insert_test.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## create table
def test_create_tables():
    sql = "create table A(id varchar(32) primary key, name varchar(48) not null, age int, phone varchar(13), idCard varchar(20) not null);\n\
           create table Array(id varchar(32) primary key, name varchar(48) not null, array int[]);\n\
           create table B(id varchar(32) primary key, name varchar(32), a A);\n\
           create table C(id varchar(32) primary key, name varchar(32), a A not null);"
    ret = client.execute(sql)
    assert_all(ret)

## insert allow null data
def test_insert_allow_null_data():
    sql = "insert into A values('S001', 'shangsan', 10, null, '37023237472877674672');"
    ret = client.execute(sql)
    assert ret['success'] == True

## test query null value
def test_select_null_value():
    sql = "select * from A;"
    ret = client.execute(sql)
    assert ret['success'] == True
    assert ret["data"] == [{ "id": "S001", "name": "shangsan", "age": 10, "phone": None, "idCard": "37023237472877674672"}]

## insert allow null data
def test_insert_not_allow_null_data():
    sql = "insert into A values('S002', 'daly', 10, null, null);"
    ret = client.execute(sql)
    assert ret['success'] == False
    assert ret['message'] == "Column 'idCard' can`t be null."

## insert priamry key null data
def test_insert_null_primary_key():
    sql = "insert into A values(null, 'Jim', 10, '1300223232', '370083342324672');"
    ret = client.execute(sql)
    assert ret['success'] == False
    assert ret['message'] == "Column 'id' can`t be null."

## insert primary key for specifying columns
def test_insert_null_primary_key_specify_columns():
    sql = "insert into A (id, name, idCard) values(null, 'Jim', '370083342324672');"
    ret = client.execute(sql)
    assert ret['success'] == False
    assert ret['message'] == "Column 'id' can`t be null."

## insert allow null data
def test_insert_null_array():
    sql = "insert into Array values('001', 'kil', null);"
    ret = client.execute(sql)
    assert ret['success'] == True

## test query null value
def test_select_null_array_value():
    sql = "select * from Array;"
    ret = client.execute(sql)
    assert ret['success'] == True
    assert ret["data"] == [{ "id": "001", "name": "kil", "array": None }]

## insert null refer
def test_insert_null_refer():
    sql = "insert into B values('B001', 'shangsan', null);"
    ret = client.execute(sql)
    assert ret['success'] == True


## query after insert sub columns null data.
def test_query_after_insert_null_refer():
    sql = "select * from B where id = 'B001';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{
        "id": "B001",
        "name": "shangsan",
        "a": None
    }]

## insert sub columns null data
def test_insert_subcolumn_null_data():
    sql = "insert into B values('B002', 'shangsan', ('S003', 'wangli', null, null, '3233242345433332333'));"
    ret = client.execute(sql)
    assert ret['success'] == True

## query after insert sub columns null data.
def test_query_after_subcolumn_null():
    sql = "select * from B where id = 'B002';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{
        "id": "B002",
        "name": "shangsan",
        "a": {
            "id": "S003",
            "name": "wangli",
            "age": None,
            "phone": None,
            "idCard": "3233242345433332333"
        }
    }]

## insert not allowd null refer column.
def test_insert_not_allow_null_refer():
    sql = "insert into C values('C001', 'Jim', null);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Column 'a' can`t be null."

## drop table
def test_drop_table():
    sql = "drop table B;\n\
           drop table C;\n\
           drop table A;\n\
           drop table Array;\n"
    ret = client.execute(sql)
    assert_all(ret)


