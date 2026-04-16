# test_systable.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## create mock table
def test_create_mock_table():
    sql = "create table Book (id varchar(32), content String, createTime timestamp, primary key(id));\n" \
          "create table Student (id varchar(32), name varchar(32), age int, birthdate timestamp, primary key(id));\n" \
          "create table Class (id varchar(32), position varchar(32), primary key(id));\n" 
    ret = client.execute(sql)
    assert_all(ret)

## test query from systable.
def test_query_from_systable():
    sql = "select relname, reltype from sys_table;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [
        {'relname': 'Book', 'reltype': 0}, 
        {'relname': 'Book', 'reltype': 4}, 
        {'relname': 'Book', 'reltype': 5}, 
        {'relname': 'Book', 'reltype': 6}, 
        {'relname': 'Book', 'reltype': 7}, 
        {'relname': 'Student', 'reltype': 0}, 
        {'relname': 'Student', 'reltype': 4}, 
        {'relname': 'Student', 'reltype': 5}, 
        {'relname': 'Student', 'reltype': 6}, 
        {'relname': 'Student', 'reltype': 7}, 
        {'relname': 'Class', 'reltype': 0}, 
        {'relname': 'Class', 'reltype': 4},
        {'relname': 'Class', 'reltype': 5},
        {'relname': 'Class', 'reltype': 6}, 
        {'relname': 'Class', 'reltype': 7} 
    ]

## test query from systable.
def test_query_only_table():
    sql = "select relname from sys_table where reltype = 0;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { 'relname': 'Book' }, 
        { 'relname': 'Student' }, 
        { 'relname': 'Class' }
    ]

## test drop systable.
def test_drop_systable():
    sql = "drop table systable;"
    ret = client.execute(sql)
    assert ret["success"] == False


## drop mock table.
def test_drop_mock_table():
    sql = "drop table Book;\n"\
          "drop table Student;\n"\
          "drop table Class;\n"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
