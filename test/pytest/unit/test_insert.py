# test_insert.py

from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

# create table
def test_create_mock_table():
    sql = "create table Student(id string primary key, name string, age int, grade int, sex char, birth date, phone string, address string, createdTime timestamp);\n" \
          "create table Parent(id string primary key, name string, student Student);\n" \
          "create table ParentDup1(id string primary key, name string, student Student);\n" \
          "create table ParentDup2(id string primary key, name int, student Student);\n" \
          "create table ParentDup3(id string primary key, content string, student Student);\n" \
          "create table Pointer(x float, y float);\n" \
          "create table Circle(r float, p Pointer);\n" 

    ret = client.execute(sql)
    assert_all(ret)

## test insert one.
def test_insert_row():
    sql = "insert into Student values('S001', 'lili', 8, 3, 'F', '2010-11-12', '13001332823', 'beijing', '2024-04-01 15:54:00');\n" \
          "insert into Student values('S002', 'wanglang', 10, 5, 'M', '2013-03-05', '18856239982', 'beijing', '2024-03-20 16:08:30');\n"
    ret = client.execute(sql)
    assert_all(ret)

## test insert with direct reference column value.
def test_insert_with_direct_reference():
    sql = "insert into Parent values ('P001', 'Kim', ('S003', 'lili', 10, 5, 'M', '2013-03-05', '139924422323', 'nanjing', '2024-03-20 16:08:30'));"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 1

## test select after insert directg reference.
def test_select_after_insert_direct_reference():
    sql = "select (student).id from Parent where id = 'P001';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0] == { "id": "S003" }

## test insert with indirect reference column value.
def test_insert_with_indirect_reference():
    sql = "insert into Parent values ('P002', 'Jerry', ref(id = 'S001'));"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 1

## test select after insert indirectg reference.
def test_select_after_insert_indirect_reference():
    sql = "select (student).id from Parent where id = 'P002';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0] == { "id": "S001" }

## test dupliacate key.
def test_duplicate_key():
    ret = client.execute("insert into Student values('S001', 'Jack', 9, 5, 'M', '2009-10-12', '17232323223', 'NewYork', '2023-12-12 09:30:21');")
    assert ret["success"] == False

## test lack column.
def test_lack_column():
    ret = client.execute("insert into Student values('S001', 9, 5, 'M', '17232323223', 'NewYork', '2023-08-09 10:20:00');")
    assert ret["success"] == False
    assert ret["message"] == "Column count doesn`t match value count: 9 != 7."

## test date data type
def test_date_data_type():
    ret = client.execute("insert into Student values('S001', 'Jerry', 9, 5, 'M', '2009/03/05', '17232323223', 'NewYork', '2023-08-09 10:20:00');")
    assert ret["success"] == False
    assert ret["message"] == "Try to convert value '2009/03/05' to date value fail."

## test date data type
def test_timestamp_data_type():
    ret = client.execute("insert into Student values('S001', 'Jerry', 9, 5, 'M', '2009-03-05', '17232323223', 'NewYork', '2023-08-09T10:20:00');")
    assert ret["success"] == False
    assert ret["message"] == "Try to convert value '2023-08-09T10:20:00' to timestamp value fail."

## test char data type
def test_char_data_type():
    ret = client.execute("insert into Student values('S001', 'Jerry',  9, 5, 'MALE', '2009-03-05', '17232323223', 'NewYork', '2023-08-09T10:20:00');")
    assert ret["success"] == False
    assert ret["message"] == "Try to convert value 'MALE' to char value type fail."

## test char data type
def test_string_data_type():
    ret = client.execute("insert into Student values('S001', 'Jerry', 9, 5, 'M', '2009-03-05', 17232323223, 'NewYork', '2023-08-09T10:20:00');")
    assert ret["success"] == False

## test insert query spc
def test_insert_for_query_spec():
    ret = client.execute("insert into ParentDup1 select * from Parent;")
    assert ret["success"] == True
    assert ret["rows"] == 2

## test insert query spc with wrong data type.
def test_insert_for_query_spec_with_wrong_type():
    ret = client.execute("insert into ParentDup2 select * from Parent;")
    assert ret["success"] == False

## test insert query spec not match columns
def test_insert_for_query_spec_not_match_columns():
    ret = client.execute("insert into ParentDup3 select * from Parent;")
    assert ret["success"] == False

## test insert into not exist table.
def test_no_exist_table():
    ret = client.execute("insert into X values('S001', 'lili', 8, 3, 'F', '2010-11-12', '13001332823', 'beijing', '2024-04-01 15:54:00');")
    assert ret["success"] == False

## test insert table without user-level priamry key
def test_insert_table_without_primary_key():
    sql = "insert into Circle values (10.23, (10.0, 20.56));\n" \
          "insert into Circle values (33.23, (107.0, 77));\n" \
          "insert into Circle values (19.75, (23.0, 17.55));\n" \
          "insert into Circle values (100, (66.0, 73));\n" \
          "insert into Circle values (45, (92.0, 29.5));\n"
    ret = client.execute(sql)
    assert_all(ret)

## query after insert table without user-level priamry key
def test_select_table_without_primary_key():
    sql = "select * from Circle where r > 20 and (p).x > 70;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [
        {"r": 33.23, "p": { "x": 107.0, "y": 77}},
        {"r": 45, "p": { "x": 92.0, "y": 29.5}},
    ]

## specify part columns when insert
def test_insert_part_columns():
    sql = "insert into Student (id, name, phone) values('S004', 'bingo', '13001332823');" 
    ret = client.execute(sql)
    assert ret["success"] == True
    
## query after insert part columns
def test_query_after_insert_part_columns():
    sql = "select * from Student where id = 'S004';" 
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ 
        "id": "S004", 
        "name": "bingo", 
        "age": None, 
        "grade": None, 
        "sex": None, 
        "birth": None, 
        "phone": "13001332823", 
        "address": None, 
        "createdTime": None
    }]

## specify part columns when insert
def test_insert_part_columns2():
    sql = "insert into `Student` (`id`, `name`, `phone`) values('S005', 'Tail', '183728494929');" 
    ret = client.execute(sql)
    assert ret["success"] == True

## drop table
def test_drop_table():
    sql = "drop table Parent;\n"\
          "drop table ParentDup1;\n"\
          "drop table ParentDup2;\n"\
          "drop table ParentDup3;\n"\
          "drop table Student;\n"\
          "drop table Circle;\n"\
          "drop table Pointer;\n"
    ret = client.execute(sql)
    assert_all(ret)

