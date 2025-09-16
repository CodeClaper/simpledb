from support.db_cli import DbClient
import threading
import random

clients = []
threads = []
share_resource = {}

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

for _ in range(0, 30):
    cli = DbClient("127.0.0.1", 4083)    
    cli.login("root", "Zc120130211")
    clients.append(cli)


# thread to insert.
def thread_insert(cli):
    try:
        names = ["zhangsan", "lisi", "Sun", "July", "Kaili", "James", "Max"]
        for i in range(1, 10000):
            sql = f"insert into Student values ('{i}', '{names[i % 7]}', { random.randint(6, 15) });"
            cli.execute(sql)
    except Exception as e:
        share_resource["insert_exception"] = e

for i in range(0, 30):
    threads.append(threading.Thread(target=thread_insert, args= { clients[i] }))

# mock table
def test_mock_table():
    sql = "create table Student(id varchar(48), name varchar(48), age int, primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True

# test concurrent insert.
def test_concurrent_insert():
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if 'insert_exception' in share_resource:
        raise share_resource['insert_exception']

## test count after concurrent insert.
def test_count():
    sql = "select count(1) from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{"count": 10000}]

def test_select():
    sql = "select id from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    data = ret["data"]
    ids = [item['id'] for item in data]
    seen = set()
    duplicates = set()
    for item in ids:
        if item in seen:
            duplicates.add(item)
        else:
            seen.add(item)
    print(duplicates)
    assert len(duplicates) == 0

# drop mock table
def test_drop_mock_table():
    sql = "drop table Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
