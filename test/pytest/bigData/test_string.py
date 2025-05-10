# test_string.py
import time
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

names = ["zhangsan", "lisi", "Sun", "July", "Kaili", "James", "Max"]
constents = [
    "All work no play,  make jack a doll boy",
    "A apple a day keeps doctor away",
    "No cross, no crown",
    "Out of sight, out of mind",
    "No pains, no gains"
]

## create mock table
def test_create_mock_table():
    sql = "create table Email(id String primary key, content String, fromId varchar(32), createTime timestamp);"
    ret = client.execute(sql)
    assert ret["success"] == True

## Insert String values
def test_insert_10000_string_values():
    for i in range(1, 10000):
        sql = f"insert into Email values ('{i}', '{constents[i % 5]}', '{names[i % 7]}', '{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))}');"  
        ret = client.execute(sql)
        assert ret["success"] == True

## Query after inserting.
def test_select_string_values():
    sql = "select * from Email;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 9999

## Query with condition.
def test_select_with_condition():
    sql = "select * from Email where content = 'A apple a day keeps doctor away';"
    ret = client.execute(sql)
    assert ret["success"] == True
    for item in ret["data"]:
        assert item["content"] == 'A apple a day keeps doctor away'

## test lick condition
def test_select_like_condtion():
    sql = "select * from Email where content like '%no%';"
    ret = client.execute(sql)
    assert ret["success"] == True

## test lick condition
def test_select_in_condtion():
    sql = "select * from Email where id in ('1', '3', '8');"
    ret = client.execute(sql)
    assert ret["success"] == True


## Query with key condition.
def test_select_with_key():
    sql = "select * from Email where id = '2';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0]["content"] == "No cross, no crown"


## Delete with key condition.
def test_delete_with_key():
    sql = "delete from Email where id = '3';"
    ret = client.execute(sql)
    assert ret["success"] == True

## Delete with key condition.
def test_select_after_delete():
    sql = "select * from Email where id = '3';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == []

## test for drop table.
def test_drop_table():
    sql = "DROP TABLE Email;" 
    ret = client.execute(sql)
    assert ret["success"] == True

