## update_test.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

## create mock table;
def test_create_mock_table():
    sql = "create table A (id string, name string, sex char, primary key(id));\n" \
          "create table B (id string, name string, age int, a A, primary key(id));"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

## insert mock data
def test_insert_mock_data():
    sql = "insert into A values ('1', 'A01', 'M');\n" \
          "insert into A values ('2', 'A02', 'F');\n" \
          "insert into A values ('3', 'A03', 'F');\n"\
          "insert into A values ('4', 'A03', 'M');\n"\
          "insert into B values ('1', 'B01', 10, ref(id = '1')); \n"\
          "insert into B values ('2', 'B02', 12, ref(id = '2')); \n"\
          "insert into B values ('3', 'B03', 13, ref(id = '3')); \n"\
          "insert into B values ('4', 'B04', 14, ref(id = '4')); \n"
    ret = client.execute(sql)
    assert_all(ret)

## test simple update
def test_simple_update():
    sql = "update A set sex = 'F' where id = '1';\n"\
          "select * from A;\n"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
    assert ret[1]["data"][0] == {"id": "1", "name": "A01", "sex": "F" }

##  test table change when sub row change. 
def test_main_select():
    sql = "select a as result from B where id = '1';";
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0] == {"result": { "id": "1", "name": "A01", "sex": "F" }}

## test update primary column
def test_update_primary_column():
    sql = "update A set id = '5' where id = '1';"
    ret = client.execute(sql)
    assert ret["success"] == True

## test select old id after update primary column
def test_query_old_after_update_priamry():
    sql = "select * from A where id = '1';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == []

##  test table change when sub row change. 
def test_main_select_after_update_primary():
    sql = "select a from B where id = '1';";
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0] == { "a": { "id": "5", "name": "A01", "sex": "F" } }

## test update duplicate key
def test_update_duplicate_key():
    sql = "update A set id = '4' where id = '2';"
    ret = client.execute(sql)
    assert ret["success"] == False

## test update duplicate key
def test_duplicate_key_when_batch_update():
    sql = "update A set id = '6' where id <= '4';"
    ret = client.execute(sql)
    assert ret["success"] == False

## test loop update
def test_loop_update():
    for _ in range(1, 101):
        sql = "update A set name = 'change' where id = '2';\n" \
              "select a from B where id = '2';\n"
        ret = client.execute(sql)
        assert ret[0]["success"] == True
        assert ret[1]["success"] == True
        assert ret[1]["data"][0] == { "a": { "id": "2", "name": "change", "sex": "F"}}

## test update with complex condition.
def test_update_with_complex_condition():
    sql = "update B set name = 'bingo' where age > 13 or (a).id = '1' or name like '%02';";
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 2

## test update indirect reference column. 
def test_update_indirect_reference_column():
    sql = "update B set a = ref(id = '3') where id = '2';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 1

## test select after update indirect reference column.
def test_select_after_update_indirect_reference():
    sql = "select count(1) from B where (a).id = '3';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0] == { "count": 2 }

## test update direct reference column.
def test_update_direct_column():
    sql = "update B set a = ('6', 'A06', 'M') where id = '3';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 1

## test select after update direct reference column.
def test_select_after_update_direct_reference():
    sql = "select a from B where id = '3';"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0] == { "a" : { "id": "6", "name": "A06", "sex": "M" } }

## delete mock table;
def test_delete_mock_table():
    sql = "drop table B;\n" \
          "drop table A;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True
