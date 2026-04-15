# test_array.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## create mock table
def test_create_mock_tables():
    sql = "create table Pointer(id int primary key, x float, y float);\n"\
          "create table Square(id int primary key, p Pointer[]);\n"\
          "create table Scheduler(id int primary key, trigger string, timers timestamp[]);"
    ret = client.execute(sql)
    assert_all(ret)

## insert array data
def test_insert_array_data():
    sql = "insert into Square values(1, [(10, 20, 30), (11, 30, 30), (12, 20, 40), (13, 30, 40)]);\n"\
          "insert into Square values(2, [(14, 40, 40), (15, 50, 40), (16, 50, 40), (17, 50, 50)]);"
    ret = client.execute(sql)
    assert_all(ret)

## test select array data.
def test_select_array_data():
    sql = "select * from Square;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 2
    assert ret["data"][0] == {
        "id": 1,
        "p": [
            { "id": 10, "x": 20, "y": 30 },
            { "id": 11, "x": 30, "y": 30 },
            { "id": 12, "x": 20, "y": 40 },
            { "id": 13, "x": 30, "y": 40 }
        ]
    }

## test update array refer.
def test_update_array_refer():
    ret1 = client.execute("select * from Square;")
    assert ret1["success"] == True
    ret2 = client.execute("insert into Pointer values(1, 2, 3);")
    assert ret2["success"] == True
    ret3 = client.execute("select * from Square;")
    assert ret1["data"] == ret3["data"]


## test insert right type data
def test_insert_right_type_data():
    sql = "insert into Scheduler values(1, 'check code test.', ['2024-04-24 10:00:00', '2024-04-24 16:00:00']);\n"\
          "insert into Scheduler values(2, 'push code.', ['2024-04-24 17:30:00']);\n"\
          "insert into Scheduler values(3, 'clock in.', ['2024-04-24 08:00:00', '2024-04-24 17:30:00']);\n"
    ret = client.execute(sql)
    assert_all(ret)

## test insert wrong type data
def test_insert_wrong_type_data():
    sql = "insert into Scheduler values(1, 'check code test.', ['2024-04-24', '2024-04-24 16:00:00']);\n"
    ret = client.execute(sql)
    assert ret["success"] == False

## test drop tables
def test_drop_mock_tables():
    sql = "drop table Square;\n"\
          "drop table Pointer;\n" \
          "drop table Scheduler;\n" 
    ret = client.execute(sql)
    assert_all(ret)
