## test_mvvc.py
from support.db_cli import DbClient
from support.asserts import assert_all

client1 = DbClient("127.0.0.1", 4083)    
client1.login("root", "Zc120130211")

client2 = DbClient("127.0.0.1", 4083)    
client2.login("root", "Zc120130211")

# mock table
def test_mock_table():
    sql = "create table Student(id int, name varchar(48), age int, primary key(id));"
    ret = client1.execute(sql)
    assert ret["success"] == True

def test_desc_mock_table():
    sql = "desc Student;"
    ret = client2.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0]["type"] ==  "int"

## begin transcaction.
def test_begin_trans():
    sql = "begin;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret2["success"] == True

## select for load table buffer.
def test_select():
    sql = "select * from Student;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret1["data"] == []
    assert ret2["success"] == True
    assert ret2["data"] == []


## insert 
def test_client1_insert():
    sql = "insert into Student values (1, 'zhangsan', 20);\n" \
          "insert into Student values (3, 'wangwu', 20);\n" 
    ret = client1.execute(sql)
    assert_all(ret)


## select 
def test_client2_select():
    sql = "select * from Student;"
    ret2 = client2.execute(sql)
    assert ret2["success"] == True
    assert ret2["data"] == []


## insert
def test_client2_insert():
    sql = "insert into Student values (2, 'lisi', 20);\n"\
          "insert into Student values (4, 'liuliu', 20);\n"
    ret = client2.execute(sql)
    assert_all(ret)

## commit transcaction.
def test_commit():
    sql = "commit;"
    sql = "commit;"
    ret1 = client1.execute(sql)
    ret2 = client2.execute(sql)
    assert ret1["success"] == True
    assert ret2["success"] == True


## select
def test_query_data():
    sql = "select * from Student;"
    ret = client1.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { "id": 1, "name": "zhangsan", "age": 20 },
        { "id": 2, "name": "lisi", "age": 20 },
        { "id": 3, "name": "wangwu", "age": 20 },
        { "id": 4, "name": "liuliu", "age": 20 },
    ]

# drop mock table
def test_drop_mock_table():
    sql = "drop table Student;"
    ret = client1.execute(sql)
    assert ret["success"] == True
