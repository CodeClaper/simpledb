# test_refer.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


def test_refer_directly_in_insert():
    sql = "create table ref_test_dept (id varchar(32) primary key, name varchar(32));\n" \
          "create table ref_test_emp (id int primary key, name varchar(32), dept ref_test_dept);\n" \
          "insert into ref_test_dept values ('D001', 'Engineering');\n" \
          "insert into ref_test_emp values (1, 'alice', ('D002', 'Computing'));"
    ret = client.execute(sql)
    assert_all(ret)


def test_refer_directly_select():
    ret = client.execute("select * from ref_test_emp;")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [{'id': 1, 'name': 'alice', 'dept': {'id': 'D002', 'name': 'Computing'}}]


def test_refer_column_access_with_parens():
    ret = client.execute("select (dept).id as dept_id from ref_test_emp;")
    assert ret["success"] == True
    assert ret["data"] == [{'dept_id': 'D002'}]


def test_refer_column_access_with_parens_name():
    ret = client.execute("select (dept).name as dept_name from ref_test_emp;")
    assert ret["success"] == True
    assert ret["data"] == [{'dept_name': 'Computing'}]


def test_refer_indirectly_ref_insert():
    ret = client.execute("insert into ref_test_emp values (2, 'bob', ref(id = 'D001'));")
    assert ret["success"] == True
    ret = client.execute("select * from ref_test_emp where id = 2;")
    assert ret["data"] == [{'id': 2, 'name': 'bob', 'dept': {'id': 'D001', 'name': 'Engineering'}}]


def test_refer_indirectly_ref_with_condition():
    ret = client.execute("insert into ref_test_emp values (3, 'charlie', ref(id = 'D001' and name = 'Engineering'));")
    assert ret["success"] == True
    ret = client.execute("select * from ref_test_emp where id = 3;")
    assert ret["data"] == [{'id': 3, 'name': 'charlie', 'dept': {'id': 'D001', 'name': 'Engineering'}}]


def test_refer_select_where_ref_condition():
    ret = client.execute("select * from ref_test_emp where (dept).id = 'D001';")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == [
        {'id': 2, 'name': 'bob', 'dept': {'id': 'D001', 'name': 'Engineering'}},
        {'id': 3, 'name': 'charlie', 'dept': {'id': 'D001', 'name': 'Engineering'}},
    ]


def test_refer_sub_access_in_where():
    ret = client.execute("select id, name from ref_test_emp where (dept).name = 'Engineering';")
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"] == [
        {'id': 2, 'name': 'bob'},
        {'id': 3, 'name': 'charlie'},
    ]


# --- Cleanup ---

def test_cleanup():
    sql = "drop table ref_test_emp;\n" \
          "drop table ref_test_dept;"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
