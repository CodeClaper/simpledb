# create_test.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Student(id varchar(32), name varchar(16), age int, class int, sex char,  score float, phone varchar(11), address varchar(48), primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True

## test create index.
def test_create_index():
    sql = "create index name_index on Student (name);"
    ret = client.execute(sql)
    assert ret["success"] == True

## test show index.
def test_show_indexs():
    sql = "show index from Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {"index_name": "name_index", "table_name": "Student", "is_unique": False, "index_type": "BTREE" }
    ]

## test show index from not exists table.
def test_show_indexs_not_exits_table():
    sql = "show index from Class;"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Table 'Class' not exists."

## test not exists table.
def test_create_not_exists_table():
    sql = "create index socre_index on student(score);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Table 'student' not exists."

## test note exists columns.
def test_create_not_exists_columns():
    sql = "create index idcard_index on Student (idcard);"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Table 'Student' not exists column 'idcard'."

## test not any columns.
def test_create_not_any_columns():
    sql = "create index any_index on Student();"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "syntax error, unexpected ')', expecting '(' or IDENTIFIER at or near [Student]."

# test drop table
def test_drop_table():
    sql = "DROP TABLE Student; "
    ret = client.execute(sql)
    assert ret["success"] == True
