from support.db_cli import DbClient
import threading
import time

stop = False
clients = []
threads = []
share_resource = {}

for _ in range(0, 10):
    cli = DbClient("127.0.0.1", 4083)    
    cli.login("root", "Zc120130211")
    clients.append(cli)


# thread1 to select
def thread_select(cli):
    try:
        for _ in range(1, 30):
            sql = "select count(1) from bo_qdbixlsx_sheet1;"
            ret = cli.execute(sql)
            assert ret["success"] == True
            count = ret["data"][0]["count"]
            assert count == 131962
            time.sleep(0.5)
            if stop:
                break
    except Exception as e:
        share_resource["select_exception"] = e

for i in range(0, 10):
    threads.append(threading.Thread(target=thread_select, args= { clients[i] }))


# test insert with select.
def test_concurrency_select():
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if 'select_exception' in share_resource:
        raise share_resource['select_exception']
