from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Score(project varchar(32) primary key, name varchar(64), score int, total long);"
    ret = client.execute(sql)
    assert ret["success"] == True


## test insert some data.
def test_boundary_int_value():
    sql = "insert into Score values ('S001', 'Math', 2147483647, 8092);\n"\
          "insert into Score values ('S002', 'English', -2147483648, 11023);\n"
    ret = client.execute(sql)
    assert_all(ret)

## test query after insert.
def test_after_inserting():
    sql = "select * from Score;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { "project": "S001", "name": "Math", "score": 2147483647, "total": 8092 },
        { "project": "S002", "name": "English", "score": -2147483648, "total": 11023 },
    ]

## test insert exceed int scope value.
def test_insert_exceed_int_scope():
    sql = "insert into Score values ('S003', 'Anymouse', 2147483648, 99020);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Value is int overflow for column 'score'."


def test_insert_exceed_nagative_int_scope():
    sql = "insert into Score values ('S003', 'Anymouse', -2147483649, 99020);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Value is int overflow for column 'score'."

## test insert some data.
def test_boundary_long_value():
    sql = f"insert into Score values ('S004', 'Computer', 2147483647, ${(1 << 63) - 1});\n"
    ret = client.execute(sql)
    assert ret["success"] == True


## test query after insert.
def test_after_inserting_long_value():
    sql = "select * from Score where project = 'S004';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { "project": "S004", "name": "Computer", "score": 2147483647, "total": 9223372036854775807 },
    ]

## test insert some data.
def test_exceed_long_value():
    sql = f"insert into Score values ('S004', 'Computer', 2147483647, ${(1 << 63) + 1});\n"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "'9223372036854775809' is overflow."

## test drop table
def test_drop_table():
    sql = "drop table Score;" 
    ret = client.execute(sql)
    assert ret["success"] == True

