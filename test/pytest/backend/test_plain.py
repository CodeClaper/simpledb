# test_plain.py
from support.db_cli import DbClient
from support.asserts import assert_all
client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


def teardown_module(module):
    client.close()
