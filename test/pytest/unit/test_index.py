# test_index.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Demo(id varchar(32), sid varchar(32), primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_insert_data():
    for i in range(0, 200000):
        sql = f"insert into Demo values ('{i}', '{i}');"
        ret = client.execute(sql)
        assert ret["success"] == True

## test index valid
def test_compare_index_valid():
    sql = "select * from Demo where id = '99999';\n"\
          "select * from Demo where sid = '99999';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid
def test_compare_index_valid2():
    sql = "select count(1) from Demo where id < '99999' and id >= '9999';\n" \
          "select count(1) from Demo where sid < '99999' and sid >= '9999';"
    ret = client.execute(sql)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid
def test_compare_index_valid3():
    sql = "select count(1) from Demo where id < '111' or id >= '999';\n" \
          "select count(1) from Demo where sid < '111' or sid >= '999';"
    ret = client.execute(sql)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]



## test drop tables
def test_drop_mock_tables():
    sql = "drop table Demo;"
    ret = client.execute(sql)
    assert ret["success"] == True
