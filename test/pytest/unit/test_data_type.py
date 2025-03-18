from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table A(id varchar(8), name varchar(16), age int, class int, sex char, score float, phone varchar(11), address varchar(48), primary key(id));\n"\
          "create table F(id int, diamend double);"
    ret = client.execute(sql)
    assert_all(ret)

## test overflow varchar.
def test_overflow_varchar():
    sql = "INSERT INTO A values('S00000000001', 'jack', 10, 2, 'M', 3.23, '130012323482333', 'beijing');"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Exceed the limit of data length: 12 > 8, for column 'id'. "

## test overflow varchar.
def test_overflow_varchar2():
    sql = "INSERT INTO A values('S001', 'jack', 10, 2, 'M', 3.23, '130012323482333', 'beijing');"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Exceed the limit of data length: 15 > 11, for column 'phone'. "

## test overflow char.
def test_overflow_char():
    sql = "INSERT INTO A values('S001', 'jack', 10, 2, 'MAN', 3.23, '13001542702', 'beijing');"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Try to convert value 'MAN' to char value type fail."

## test overflow char.
def test_overflow_int():
    sql = "INSERT INTO A values('S001', 'jack', 1314232272847489294756329290476549394673, 2, 'M', 3.23, '13001542702', 'beijing');"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "'1314232272847489294756329290476549394673' is overflow."


## test overflow char.
def test_invalid_number():
    sql = "INSERT INTO A values('S001', 'jack', 1223232323erwswq, 2, 'M', 3.23, '13001542702', 'beijing');"
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "syntax error, unexpected IDENTIFIER, expecting ')' or ',', that happened near [erwswq]."

## test float overflow.
def test_overflow_float():
    sql = "INSERT INTO A VALUES('S001', 'mary', 10, 4, 'F', 9999992312313131238173472748273472874827348738478274832784782748273847824782748728478294782783472893213287382738274872837827387283728892832738728478273872837827382.231872463478718273817483784723847823, '13023237472', 'beijing');";
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "Value is float overflow for column 'score'."

## test insert big double value.
def test_insert_big_double():
    sql = "INSERT INTO F VALUES(1, 9999992312313131238173472748273472874827348738478274832784782748273847824782748728478294782783472893213287382738274872837827387283728892832738728478273872837827382.231872463478718273817483784723847823);";
    ret = client.execute(sql)
    assert ret["success"] == True

## test query double value.
def test_query_double_value():
    sql = "SELECT diamend from F;";
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"][0]["diamend"] == 9999992312313131238173472748273472874827348738478274832784782748273847824782748728478294782783472893213287382738274872837827387283728892832738728478273872837827382.231872463478718273817483784723847823

## test insert negative value.
def test_insert_negative_value():
    sql = "INSERT INTO F values (-10, -11.232);"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 1


## test insert negative value.
def test_select_negative_value():
    sql = "SELECT * FROM F WHERE id = -10;"
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"] == [{ "id": -10, "diamend": -11.232 }]

## test for drop table.
def test_drop_table():
    sql = "DROP TABLE A;\n" \
          "DROP TABLE F;"
    ret = client.execute(sql)
    assert_all(ret)

