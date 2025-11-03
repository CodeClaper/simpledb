# create_test.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Student(id varchar(32), name varchar(16), age int, class int, sex char,  score float, phone varchar(11), address varchar(48), primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True

## test create index.
def test_create_index():
    sql = "create index name_index on Student (name);"
    ret = client.execute(sql)
    assert ret["success"] == True

## test drop table
def test_drop_table():
    sql = "DROP INDEX name_index;\n" \
          "DROP TABLE Student; "
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
