# select_test.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

## test wrong create statement.
def test_wrong_create():
    sql = "create table (id int, name varchar(32), age int, adress string);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "syntax error, unexpected '(', expecting IDENTIFIER."

## test wrong drop statement.
def test_wrong_drop():
    sql = "drop Student;"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "syntax error, unexpected IDENTIFIER, expecting TABLE or INDEX at or near [Student]."

## test wrong insert statement.
def test_wrong_insert():
    sql = "insert into Student (id, name, age , address) value (1, 'Ben', 13, 'Washington DC.');"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "syntax error, unexpected IDENTIFIER, expecting SELECT or VALUES at or near [value]."

## test wront multi statements. 
def test_wrong_multi_statements():
    sql = "begin;\n" \
          "insert into Student (id, name, age , address) value (1, 'Ben', 13, 'Washington DC.');" \
          "rollback;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == False
    assert ret["message"] == "syntax error, unexpected IDENTIFIER, expecting SELECT or VALUES at or near [value]."


def teardown_module(module):
    client.close()
