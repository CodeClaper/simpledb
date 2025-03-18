## test_insert_with_select.py
## Test Simpledb concurrent security issue. 
## One thread insert while four threads select.

from support.db_cli import DbClient
import threading
import random
import time

stop = False
clients = []
threads = []
share_resource = {}

for _ in range(0, 30):
    cli = DbClient("127.0.0.1", 4083)    
    cli.login("root", "Zc120130211")
    clients.append(cli)


# thread to insert.
def thread_insert(cli):
    try:
        names = ["zhangsan", "lisi", "Sun", "July", "Kaili", "James", "Max"]
        for i in range(1, 20000):
            sql = f"insert into Student values ('{i}', '{names[i % 7]}', { random.randint(6, 15) });"
            ret = cli.execute(sql)
            assert ret["success"] == True
    except Exception as e:
        share_resource["insert_exception"] = e
    finally:
        stop = True


# thread1 to select
def thread_select(cli):
    try:
        for _ in range(1, 200):
            sql = "select count(1) from Student;"
            ret = cli.execute(sql)
            assert ret["success"] == True
            count = ret["data"][0]["count"]
            assert (count >= 0 and count <= 20000)
            time.sleep(0.1)
            if stop:
                break
    except Exception as e:
        share_resource["select_exception"] = e

threads.append(threading.Thread(target=thread_insert, args= { clients[0] }))
for i in range(1, 30):
    threads.append(threading.Thread(target=thread_select, args= { clients[i] }))


# mock table
def test_mock_table():
    sql = "create table Student(id varchar(48), name varchar(48), age int, primary key(id));"
    ret = clients[0].execute(sql)
    assert ret["success"] == True

# test insert with select.
def test_insert_with_select():
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if 'select_exception' in share_resource:
        raise share_resource['select_exception']
    if 'insert_exception' in share_resource:
        raise share_resource['insert_exception']

# drop mock table
def test_drop_mock_table():
    sql = "drop table Student;"
    ret = clients[0].execute(sql)
    assert ret["success"] == True
