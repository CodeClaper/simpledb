# test_transaction.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


def test_begin_transaction():
    ret = client.execute("begin;")
    assert ret["success"] == True

    ret = client.execute("rollback;")


def test_commit_transaction():
    ret = client.execute("begin;")
    assert ret["success"] == True
    ret = client.execute("commit;")
    assert ret["success"] == True


def test_rollback_transaction():
    ret = client.execute("begin;")
    assert ret["success"] == True
    ret = client.execute("rollback;")
    assert ret["success"] == True


def test_begin_with_newline():
    ret = client.execute("begin;\n")
    assert ret["success"] == True
    ret = client.execute("rollback;")


def test_multiple_transaction_statements():
    sql = "begin;\ncommit;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True


def test_begin_commit_rollback_sequence():
    sql = "begin;\ncommit;\nbegin;\nrollback;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
    assert ret[2]["success"] == True
    assert ret[3]["success"] == True


def teardown_module(module):
    client.close()
