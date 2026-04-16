# test_trans.py
from support.db_cli import DbClient
from support.asserts import assert_all
client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

## create mock table:
def test_create_mock_table():
    sql = "create table Student (id string primary key, name string, age int, birth date, address string);\n"  
    ret = client.execute(sql)
    assert ret["success"] == True

## insert mock data. 
def test_insert_mock_data():
    sql = "insert into Student values('S001', 'kail', 10, '2014-10-03', 'beijing');\n" \
          "insert into Student values('S002', 'sun', 11, '2013-11-20', 'shanghai');\n" \
          "insert into Student values('S003', 'ben', 12, '2012-04-23', 'wuhan');\n" \
          "insert into Student values('S004', 'david', 14, '2010-01-05', 'jinan');\n" \
          "insert into Student values('S005', 'kunting', 9, '2015-06-23', 'shijiazhuang');\n" \
          "insert into Student values('S006', 'bob', 9, '2015-07-07', 'guangzhou');\n" \
          "insert into Student values('S007', 'july', 11, '2013-03-05', 'shenzhen');\n" \
          "insert into Student values('S008', 'tail', 13, '2011-08-08', 'xian');\n" 
    ret = client.execute(sql)
    assert_all(ret)

def test_insert_duplicate_key():
    sql = "begin; insert into Student values ('S009', 'bell', 12, '2013-11-12', 'guangzhou');"\
          "insert into Student values('S009', 'Ali', 10, '2015-03-12', 'wuhan'); commit;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
    assert ret[2]["success"] == False
    assert ret[2]["message"] == "key 'S009' in table 'Student' already exists, not allow duplicate key."

def test_commit():
    sql = "commit;"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_select_after_duplicate_key():
    sql = "select * from Student;"
    ret = client.execute(sql)
    print(ret)
    assert ret["success"] == True
    assert ret["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'address': 'beijing'}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'address': 'shanghai'}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'address': 'wuhan'}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'address': 'jinan'}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'address': 'shijiazhuang'}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'address': 'guangzhou'}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'address': 'shenzhen'}, 
        {'id': 'S008', 'name': 'tail', 'age': 13, 'birth': '2011-08-08', 'address': 'xian'}
    ]

def test_keep_insert_duplicate_key():
    ret = client.execute("begin;")
    assert ret["success"] == True
    for __i__ in range(1, 10):
        ret = client.execute("insert into Student values ('S008', 'bell', 12, '2013-11-12', 'guangzhou');")
        assert ret["success"] == False
        assert ret["message"] == "key 'S008' in table 'Student' already exists, not allow duplicate key." 
 
def test_commit2():
    sql = "commit;"
    ret = client.execute(sql)
    assert ret["success"] == True

## test drop table
def test_drop_mock_tables():
    sql = "drop table Student;\n"
    ret = client.execute(sql)
    assert ret["success"] == True


def teardown_module(module):
    client.close()
