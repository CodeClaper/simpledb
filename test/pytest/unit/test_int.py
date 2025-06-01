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
def test_insert_some_data():
    sql = "insert into Score values ('S001', 'Math', 98, 8092);\n"\
          "insert into Score values ('S002', 'English', 92, 11023);\n"\
          "insert into Score values ('S003', 'Art', 89, 9920);\n"
    ret = client.execute(sql)
    assert_all(ret)

def test_insert_exceed_int_scope():
    sql = "insert into Score values ('S004', 'Anymouse', 130203202020303030, 99020);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Value is int overflow for column 'score'."


## test drop table
def test_drop_table():
    sql = "drop table Score;" 
    ret = client.execute(sql)
    assert ret["success"] == True

