## delete_test.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## Firstly create table.
def test_create_table():
    sql = "create table A(id string, name string, score float, primary key (id));"
    ret = client.execute(sql)
    assert ret["success"] == True


def test_insert_and_delete():
    sql = "insert into A values('S001', 'BOBN', 98.3);\n" \
          "delete from A where id = 'S001';\n" \
          "select * from A where id = 'S001';\n" 
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
    assert ret[2]["success"] == True
    assert ret[2]["data"] == []

## drop mock tables
def test_drop_mock_tables():
    sql = "drop table A;\n"
    ret = client.execute(sql)
    assert ret["success"] == True

def teardown_module(module):
    client.close()
