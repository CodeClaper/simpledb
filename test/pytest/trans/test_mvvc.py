# test_trans.py
from ..support.db_cli import DbClient
from ..support.asserts import assert_all
client1 = DbClient("127.0.0.1", 4083)
client2 = DbClient("127.0.0.1", 4083)
client1.login("root", "Zc120130211")
client2.login("root", "Zc120130211")

## create mock table:
def test_create_mock_table():
    sql = "create table Class (id string primary key, location string);\n" \
          "create table Student (id string primary key, name string, age int, birth date, class Class);\n"  
    ret = client1.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

## insert data. 
def test_insert_data():
    ret1 = client1.execute("begin;")
    ret2 = client2.execute("begin;")
    assert ret1["success"] == True
    assert ret2["success"] == True

    sql = "insert into Class values('C001', 'Northwest corner ');\n" \
          "insert into Class values('C002', 'Middle');\n" \
          "insert into Class values('C003', 'South side');\n" \
          "insert into Class values('C004', 'East side');\n" \
          "insert into Student values('S001', 'kail', 10, '2014-10-03', ref(id = 'C001'));\n" \
          "insert into Student values('S002', 'sun', 11, '2013-11-20', ref(id = 'C001'));\n" \
          "insert into Student values('S003', 'ben', 12, '2012-04-23', ref(id = 'C002'));\n" \
          "insert into Student values('S004', 'david', 14, '2010-01-05', ref(id = 'C002'));\n" \
          "insert into Student values('S005', 'kunting', 9, '2015-06-23', ref(id = 'C002'));\n" \
          "insert into Student values('S006', 'bob', 9, '2015-07-07', ref(id = 'C003'));\n" \
          "insert into Student values('S007', 'july', 11, '2013-03-05', ref(id = 'C003'));\n" \
          "insert into Student values('S008', 'alice', 13, '2011-08-08', ref(id = 'C004'));\n" 
    ret1 = client1.execute(sql)
    assert_all(ret1)

    ret = client1.execute("select count(1) from Student;")
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 8 }]

    ret = client2.execute("select count(1) from Student;")
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 0 }]

    ret1 = client1.execute("commit;")
    ret2 = client2.execute("commit;")
    assert ret1["success"] == True
    assert ret2["success"] == True

## test delete row.
def test_delete_row_roll_back():
    ret1 = client1.execute("begin;")
    ret2 = client2.execute("begin;")
    assert ret1["success"] == True
    assert ret2["success"] == True

    ret1 = client1.execute("delete from Student where id = 'S004';")
    assert ret1["success"] == True

    ret1 = client1.execute("select * from Student;")
    print(ret1)
    assert ret1["success"] == True
    assert ret1["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}}
    ]

    ret2 = client2.execute("select * from Student;")
    assert ret2["success"] == True
    assert ret2["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}}
    ]

    ret1 = client1.execute("rollback;")
    assert ret1["success"] == True

    ret1 = client1.execute("select * from Student;")
    assert ret1["success"] == True
    assert ret1["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}}
    ]
 
    ret2 = client2.execute("commit;")
    assert ret2["success"] == True

## test delete row.
def test_delete_row_commit():
    ret1 = client1.execute("begin;")
    ret2 = client2.execute("begin;")
    assert ret1["success"] == True
    assert ret2["success"] == True

    ret1 = client1.execute("delete from Student where id = 'S004';")
    assert ret1["success"] == True

    ret1 = client1.execute("select * from Student;")
    print(ret1)
    assert ret1["success"] == True
    assert ret1["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}}
    ]

    ret2 = client2.execute("select * from Student;")
    assert ret2["success"] == True
    assert ret2["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}}
    ]

    ret1 = client1.execute("commit;")
    assert ret1["success"] == True

    ret2 = client2.execute("select * from Student;")
    assert ret2["success"] == True
    assert ret2["data"] == [
        {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
        {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}}
    ]
 
    ret2 = client2.execute("commit;")
    assert ret2["success"] == True

# def test_duplicate_key():
#     ret1 = client1.execute("begin;")
#     ret2 = client2.execute("begin;")
#     assert ret1["success"] == True
#     assert ret2["success"] == True
#
#     ret1 = client1.execute("insert into Student values('S009', 'harden', 16, '2009-11-18', ref(id = 'C004'));") 
#     assert ret1["success"] == True
#
#     ret1 = client1.execute("select * from Student;")
#     assert ret1["success"] == True
#     assert ret1["data"] == [
#         {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
#         {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
#         {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
#         {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
#         {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
#         {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
#         {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}},
#         {'id': 'S009', 'name': 'harden', 'age': 16, 'birth': '2009-11-18', 'class': {'id': 'C004', 'location': 'East side'}}
#     ]
#
#     ret2 = client2.execute("select * from Student;")
#     assert ret2["success"] == True
#     assert ret2["data"] == [
#         {'id': 'S001', 'name': 'kail', 'age': 10, 'birth': '2014-10-03', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
#         {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner '}}, 
#         {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
#         {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle'}}, 
#         {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side'}}, 
#         {'id': 'S007', 'name': 'july', 'age': 11, 'birth': '2013-03-05', 'class': {'id': 'C003', 'location': 'South side'}}, 
#         {'id': 'S008', 'name': 'alice', 'age': 13, 'birth': '2011-08-08', 'class': {'id': 'C004', 'location': 'East side'}}
#     ]
#
#     ret1 = client1.execute("insert into Student values('S009', 'willson', 12, '2013-09-11', ref(id = 'C004'));") 
#     assert ret1["success"] == False

## test drop table
def test_drop_mock_tables():
    sql = "drop table Student;\n"\
          "drop table Class;"
    ret = client1.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

