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
def test_insert_string_values():
    for i in range(1, 10):
        sql = f"insert into Email values ('{i}', '{constents[i % 5]}', '{names[i % 7]}', '{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))}');"  
        ret = client.execute(sql)
        assert ret["success"] == True

## Query after inserting.
def test_select_string_values():
    sql = "select * from Email;"
    ret = client.execute(sql)
    assert ret["success"] == True
    for i, item in enumerate(ret["data"]):
        assert item["content"] == constents[(i + 1) % 5]

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
    assert ret["rows"] == 5

## test lick condition
def test_select_in_condtion():
    sql = "select * from Email where id in ('1', '3', '8');"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 3


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

## test insert long text value.
def test_insert_long_text():
    content = "An abstract syntax tree (AST) is a data structure used in computer science to represent the structure of a program or code snippet. It is a tree representation of the abstract syntactic structure of text (often source code) written in a formal language. Each node of the tree denotes a construct occurring in the text. It is sometimes called just a syntax tree."
    sql = f"insert into Email values ('11', '{content}', 'zhangsan', '{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))}');"
    ret = client.execute(sql)
    assert ret["success"] == True


## test query long text value.
def test_query_long_text():
    content = "An abstract syntax tree (AST) is a data structure used in computer science to represent the structure of a program or code snippet. It is a tree representation of the abstract syntactic structure of text (often source code) written in a formal language. Each node of the tree denotes a construct occurring in the text. It is sometimes called just a syntax tree."
    sql = "select content from Email where id = '11';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [ { "content": content }]


## test insert file text.
def test_insert_file_text():
    with open('test/pytest/unit/files/whatisdocker.txt', encoding= 'utf-8') as file:
        content = file.read()
        sql = f"insert into Email values ('12', '{content}', 'lily', '{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))}');"
        ret = client.execute(sql)
        assert ret["success"] == True


## test query file text.
def test_query_file_content():
    with open('test/pytest/unit/files/whatisdocker.txt', encoding= 'utf-8') as file:
        content = file.read()
        sql = "select content from Email where id = '12';"
        ret = client.execute(sql)
        assert ret["success"] == True
        assert ret["data"] == [ { "content": content }]

## test for drop table.
def test_drop_mock_table():
    sql = "drop table Email;"
    ret = client.execute(sql)
    assert ret["success"] == True
