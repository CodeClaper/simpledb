# test_index.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

## test create table
def test_create_table():
    sql = "create table Demo(id varchar(32), sid varchar(32), primary key(id));"
    ret = client.execute(sql)
    assert ret["success"] == True

def test_insert_data():
    for i in range(0, 200000):
        sql = f"insert into Demo values ('{i}', '{i}');"
        ret = client.execute(sql)
        assert ret["success"] == True

## test index valid
def test_compare_index_valid1():
    sql = "select * from Demo where id = '99999';\n"\
          "select * from Demo where sid = '99999';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid
def test_compare_index_valid2():
    sql = "select count(1) from Demo where id != '888';\n"\
          "select count(1) from Demo where sid != '888';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid
def test_compare_index_valid3():
    sql = "select count(1) from Demo where id < '99999' and id >= '9999';\n" \
          "select count(1) from Demo where sid < '99999' and sid >= '9999';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid
def test_compare_index_valid4():
    sql = "select count(1) from Demo where id < '111' or id >= '999';\n" \
          "select count(1) from Demo where sid < '111' or sid >= '999';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index invalid
def test_compare_index_invalid1():
    sql = "select count(1) from Demo where id < '111' or sid >= '999';\n" \
          "select count(1) from Demo where sid < '111' or sid >= '999';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert abs(ret[0]["duration"] - ret[1]["duration"]) < 1

## test critical value
def test_critical_value1():
    sql = "select count(1) from Demo where id >= '76150' and id <= '76247';\n" \
          "select count(1) from Demo where sid >= '76150' and sid <= '76247';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test critical value
def test_critical_value2():
    sql = "select count(1) from Demo where id > '75959' and id < '76149';\n" \
          "select count(1) from Demo where sid > '75959' and sid < '76149';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]
    
## test critical value
def test_critical_value3():
    sql = "select * from Demo where id = '31180' or id = '144460' or id = '5357' or id = '75862';\n" \
          "select * from Demo where sid = '31180' or sid = '144460' or sid = '5357' or sid = '75862';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for like predicate
def test_index_valid_like1():
    sql = "select * from Demo where id like '%99999%';\n" \
          "select * from Demo where sid like '%99999%';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert abs(ret[0]["duration"] - ret[1]["duration"]) < 1

## test index valid for like predicate
def test_index_valid_like2():
    sql = "select * from Demo where id like '%99999';\n" \
          "select * from Demo where sid like '%99999';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert abs(ret[0]["duration"] - ret[1]["duration"]) < 0.1

## test index valid for like predicate
def test_index_valid_like3():
    sql = "select * from Demo where id like '99999%';\n" \
          "select * from Demo where sid like '99999%';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for in predicate. 
def test_index_valid_in():
    sql = "select * from Demo where id in ('1', '111', '1111', '11111');\n"\
          "select * from Demo where sid in ('1', '111', '1111', '11111');\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for not. 
def test_index_valid_for_not1():
    sql = "select count(*) from Demo where not id = '888' ;\n"\
          "select count(*) from Demo where not sid = '888';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for not. 
def test_index_valid_for_not2():
    sql = "select count(*) from Demo where not id != '888' ;\n"\
          "select count(*) from Demo where not sid != '888';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for not. 
def test_index_valid_for_not3():
    sql = "select count(*) from Demo where not id > '11' ;\n"\
          "select count(*) from Demo where not sid > '11';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for not. 
def test_index_valid_for_not4():
    sql = "select count(*) from Demo where not id >= '11' ;\n"\
          "select count(*) from Demo where not sid >= '11';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for not. 
def test_index_valid_for_not5():
    sql = "select count(*) from Demo where not id < '999' ;\n"\
          "select count(*) from Demo where not sid < '999';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for not. 
def test_index_valid_for_not6():
    sql = "select count(*) from Demo where not id <= '999' ;\n"\
          "select count(*) from Demo where not sid <= '999';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for not. 
def test_index_valid_for_not7():
    sql = "select count(*) from Demo where not id in ('1', '111', '1111', '11111');\n"\
          "select count(*) from Demo where not sid in ('1', '111', '1111', '11111');\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for not. 
def test_index_valid_for_not8():
    sql = "select count(1) from Demo where not id like '1%';\n" \
          "select count(1) from Demo where not sid like '1%';\n"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for not. 
def test_index_valid_for_not9():
    sql = "select count(1) from Demo where not (id > '75959' and id < '76149');\n" \
          "select count(1) from Demo where not (sid > '75959' and sid < '76149');"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for not. 
def test_index_valid_for_not10():
    sql = "select count(1) from Demo where not id != '75959' or id < '111';\n" \
          "select count(1) from Demo where not sid != '75959' or sid < '111';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for not. 
def test_index_valid_for_not11():
    sql = "select count(1) from Demo where not id < '111' and id < '11111';\n" \
          "select count(1) from Demo where not sid < '111' and sid < '11111';"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for is true
def test_index_valid_for_is_true1():
    sql = "select count(1) from Demo where id = '888' is true;\n" \
          "select count(1) from Demo where sid = '888' is true;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for is true
def test_index_valid_for_is_true2():
    sql = "select count(1) from Demo where id != '888' is true;\n" \
          "select count(1) from Demo where sid != '888' is true;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for is true
def test_index_valid_for_is_true3():
    sql = "select count(1) from Demo where id > '222' and id < '2222' is true;\n" \
          "select count(1) from Demo where sid > '222' and id < '2222' is true;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for is false
def test_index_valid_for_is_false1():
    sql = "select count(1) from Demo where id = '888' is false;\n" \
          "select count(1) from Demo where sid = '888' is false;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for is true
def test_index_valid_for_is_false2():
    sql = "select count(1) from Demo where id != '888' is false;\n" \
          "select count(1) from Demo where sid != '888' is false;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for is false
def test_index_valid_for_is_false3():
    sql = "select count(1) from Demo where (id < '222' or id > '2222') is false;\n" \
          "select count(1) from Demo where (sid < '222' or sid > '2222') is false;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for is false
def test_index_valid_for_is_false4():
    sql = "select count(1) from Demo where not (id > '222' and id < '2222') is false;\n" \
          "select count(1) from Demo where not (sid > '222' and sid < '2222') is false;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for is not true
def test_index_valid_for_is_not_true1():
    sql = "select count(1) from Demo where id = '888' is not true;\n" \
          "select count(1) from Demo where sid = '888' is not true;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for is not true
def test_index_valid_for_is_not_true2():
    sql = "select count(1) from Demo where id != '888' is not true;\n" \
          "select count(1) from Demo where sid != '888' is not true;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test index valid for is not true
def test_index_valid_for_is_not_true3():
    sql = "select count(1) from Demo where (id < '222' or id > '2222') is not true;\n" \
          "select count(1) from Demo where (sid < '222' or sid > '2222') is not true;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]

## test index valid for is not true
def test_index_valid_for_is_not_true4():
    sql = "select count(1) from Demo where not (id > '222' and id < '2222') is not true;\n" \
          "select count(1) from Demo where not (sid > '222' and sid < '2222') is not true;"
    ret = client.execute(sql)
    assert_all(ret)
    assert ret[0]["data"] == ret[1]["data"]
    assert ret[0]["duration"] * 10 < ret[1]["duration"]

## test drop tables
def test_drop_mock_tables():
    sql = "drop table Demo;"
    ret = client.execute(sql)
    assert ret["success"] == True
