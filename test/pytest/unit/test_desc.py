## test_desc.py
from support.db_cli import DbClient

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## Firstly create table.
def test_create_table():
    sql = "CREATE TABLE Student(id varchar(32) primary key comment 'Student Id', name varchar(32) not null comment 'Student full name', \
           age int comment 'Student age', address varchar(64) default 'unknown' comment 'Student address');"
    ret = client.execute(sql)
    assert ret["success"] == True

## Test for desc table after creating table.
def test_desc_table():
    sql = "DESC Student;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        { "field": "id", "type": "varchar", "length": 32, "array": False, "key": "primary", "comment": "Student Id" },
        { "field": "name", "type": "varchar", "length": 32, "array": False, "key": None, "comment": "Student full name" },
        { "field": "age", "type": "int", "length": 4, "array": False, "key": None, "comment": "Student age" },
        { "field": "address", "type": "varchar", "length": 64, "array": False, "key": None, "comment": "Student address", "default": "unknown" }
    ]

## Test for desc non-existen table.
def test_desc_non_existen_table():
    sql = "DESC non_table;"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Table 'non_table' not exists."

## Test for drop table Student. 
def test_drop_table():
    sql = "DROP TABLE Student;"
    ret = client.execute(sql)
    assert ret["success"] == True


