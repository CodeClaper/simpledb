# select_test.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")

## create mock tables;
def test_create_mock_table():
    sql = "create table Class (id string primary key, location string, studentNum int);\n" \
          "create table Student (id string primary key, name string, age int, birth date, class Class);\n"  
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

## insert mock data. 
def test_insert_mock_data():
    sql = "insert into Class values('C001', 'Northwest corner', 32);\n" \
          "insert into Class values('C002', 'Middle', 28);\n" \
          "insert into Class values('C003', 'South side', 33);\n" \
          "insert into Class values('C004', 'East side', 30);\n" \
          "insert into Student values('S001', 'kail', 10, '2014-10-03', ref(id = 'C001'));\n" \
          "insert into Student values('S002', 'sun', 11, '2013-11-20', ref(id = 'C001'));\n" \
          "insert into Student values('S003', 'ben', 12, '2012-04-23', ref(id = 'C002'));\n" \
          "insert into Student values('S004', 'david', 14, '2010-01-05', ref(id = 'C002'));\n" \
          "insert into Student values('S005', 'kunting', 9, '2015-06-23', ref(id = 'C002'));\n" \
          "insert into Student values('S006', 'bob', 9, '2015-07-07', ref(id = 'C003'));\n" \
          "insert into Student values('S007', 'july', 11, '2013-03-05', ref(id = 'C003'));\n" \
          "insert into Student values('S008', 'alice', 13, '2011-08-08', ref(id = 'C004'));\n" 
    ret = client.execute(sql)
    assert_all(ret)

## test select all
def test_select_all():
    ret = client.execute("select * from Student;")
    assert ret["success"] == True
    assert ret["rows"] == 8

## test select one item
def test_select_one_item():
    ret = client.execute("select id from Student;")
    assert ret["success"] == True
    assert ret["data"] == [{ "id": "S001" }, {"id": "S002"}, { "id": "S003" }, {"id": "S004"}, { "id": "S005"}, { "id": "S006" }, { "id": "S007"}, { "id": "S008"}]

## test select multiple items
def test_select_items():
    ret = client.execute("select id, name from Student;")
    assert ret["success"] == True
    assert ret["data"] == [{ "id": "S001", "name": "kail" }, \
                           { "id": "S002", "name": "sun" }, \
                           { "id": "S003", "name": "ben" }, \
                           { "id": "S004", "name": "david" }, \
                           { "id": "S005", "name": "kunting" }, \
                           { "id": "S006", "name": "bob" }, \
                           { "id": "S007", "name": "july" }, \
                           { "id": "S008", "name": "alice" }]


## test select not exists items.
def test_select_not_exist_items():
    ret = client.execute("select email from Student;")
    assert ret["success"] == False

## test select condition field not exist.
def test_select_not_exist_condition_filed():
    ret = client.execute("select * from Student where name = 'xxx';")
    assert ret["data"] == []

## test select simple condition.
def test_select_simple_condition():
    ret = client.execute("select id from Student where id = 'S001';")
    assert ret["success"] == True
    assert ret["rows"] == 1
    assert ret["data"][0] == { "id": "S001" }

## test select complex condition
def test_select_complex_condition1():
    ret = client.execute("select id from Student where age = 10 and name = 'kail';")
    assert ret["success"] == True
    assert ret["data"][0] == { "id": "S001" }

## test select complex condition
def test_select_complex_condition2():
    ret = client.execute("select id from Student where age > 10 and (class).id = 'C001';")
    assert ret["success"] == True
    assert ret["rows"] == 1

## test select complex condition
def test_select_complex_condition3():
    ret = client.execute("select id from Student where age < 10 and age > 5 or (class).id = 'C002';")
    assert ret["success"] == True
    assert ret["rows"] == 4

## test select complex condition
def test_select_complex_condition4():
    ret = client.execute("select id from Student where (age < 10 and age > 5 or (class).id = 'C003');")
    assert ret["success"] == True
    assert ret["rows"] == 3

## test select like predicate.
def test_select_like_predicate1():
    ret = client.execute("select id from Student where name like '%un%';")
    assert ret["success"] == True
    assert ret["rows"] == 2

## test select like predicate.
def test_select_like_predicate2():
    ret = client.execute("select id from Student where name like '%ing';")
    assert ret["success"] == True
    assert ret["rows"] == 1

## test select like predicate.
def test_select_like_predicate3():
    ret = client.execute("select id from Student where name like 'k%';")
    assert ret["success"] == True
    assert ret["rows"] == 2

## test select like predicate.
def test_select_like_predicate4():
    ret = client.execute("select id from Student where name like 'bob';")
    assert ret["success"] == True
    assert ret["rows"] == 1


## test select in predicate.
def test_select_in_predicate1():
    ret = client.execute("select id from Student where id in ('S001');")
    assert ret["success"] == True
    assert ret["rows"] == 1

## test select in predicate.
def test_select_in_predicate2():
    ret = client.execute("select id from Student where id in ('S001', 'S002');")
    assert ret["success"] == True
    assert ret["rows"] == 2

## test select in predicate.
def test_select_in_predicate3():
    ret = client.execute("select id from Student where name in ('bob', 'alice');")
    assert ret["success"] == True
    assert ret["rows"] == 2

## test select in predicate.
def test_select_in_predicate4():
    ret = client.execute("select id from Student where name in ('jim', 'jerry');")
    assert ret["success"] == True
    assert ret["rows"] == 0

## test max function.
def test_max_function1():
    ret = client.execute("select max(age) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "max": 14 }

## test max function.
def test_max_function2():
    ret = client.execute("select max(age), id from Student where id in ('S001', 'S002');")
    assert ret["success"] == True
    assert ret["data"][0] == { "max": 11, "id": "S001" }

## test max function.
def test_max_function3():
    ret = client.execute("select max(*) from Student where id in ('S001', 'S002');")
    assert ret["success"] == False

## test max function.
def test_max_function4():
    ret = client.execute("select max(100) from Student where id in ('S001', 'S002', 'S003');")
    assert ret["success"] == True
    assert ret["data"][0] == { "max": 100 }

## test max function.
def test_max_function5():
    ret = client.execute("select max(age) as age from Student where id in ('S001', 'S002', 'S003');")
    assert ret["success"] == True
    assert ret["data"][0] == { "age": 12 }

## test min function.
def test_min_function1():
    ret = client.execute("select min(id) from Student where name like '%a%';")
    assert ret["success"] == True
    assert ret["data"][0] == { "min": "S001" }

## test min function.
def test_min_function2():
    ret = client.execute("select min(age), name from Student where name like '%a%';")
    assert ret["success"] == True
    assert ret["data"][0] == { "min": 10, "name": "kail" }

## test min function.
def test_min_function3():
    ret = client.execute("select min(age), max(name) from Student where name like '%n%';")
    assert ret["success"] == True
    assert ret["data"][0] == { "min": 9, "max": "sun" }

## test min function.
def test_min_function4():
    ret = client.execute("select min(10) from Student where id like '%ng%';")
    assert ret["success"] == True
    assert ret["data"][0] == { "min": 10 }

## test count function.
def test_count_function1():
    ret = client.execute("select count(1) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "count": 8 }

## test count function.
def test_count_function2():
    ret = client.execute("select count(10) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "count": 8 }

## test count function.
def test_count_function3():
    ret = client.execute("select count(age) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "count": 8 }

## test count function.
def test_count_function4():
    ret = client.execute("select count(id) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "count": 8 }

## test sum function.
def test_sum_function1():
    ret = client.execute("select sum(id) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "sum": 0 }


## test sum function.
def test_sum_function2():
    ret = client.execute("select sum(age) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "sum": 89 }

## test avg function.
def test_avg_function1():
    ret = client.execute("select avg(age) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "avg": 11.125}

## test avg function.
def test_avg_function2():
    ret = client.execute("select avg(id) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "avg": 0.0 }

## test calculate selection
def test_calculate_selection_add1():
    ret = client.execute("select count(id) + count(1) from Student;")
    assert ret["success"] == True
    assert ret["data"][0] == { "add": 16 }

## test calculate selection
def test_calculate_selection_add2():
    ret = client.execute("select age + age from Student where (class).id = 'C002';")
    assert ret["success"] == True
    assert ret["data"] == [{ "add": 24 }, { "add": 28 }, { "add": 18 }]

## test calculate selection
def test_calculate_selection_add3():
    ret = client.execute("select age + age from Student where id = 'S001';")
    assert ret["success"] == True
    assert ret["data"][0] == { "add": 20 }

## test calculate selection
def test_calculate_selection_sub1():
    ret = client.execute("select sum(age) - sum(class) from Student;")
    assert ret["success"] == False

## test calculate selection
def test_calculate_selection_sub2():
    ret = client.execute("select sum(age) from Student where (class).id = 'C001';")
    assert ret["success"] == True
    assert ret["data"][0] == { "sum": 21 }

## test calculate select not exists column
def test_calculate_selection_none_column():
    ret = client.execute("select age - none from Student;")
    assert ret["success"] == False


## query with limit clause.
def test_query_with_limit_clause1():
    sql = "select count(1) from Student limit 5;";
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 5 }]

## query with limit clause.
def test_query_with_limit_clause2():
    sql = "select * from Student limit 1, 5;";
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["rows"] == 5
    assert ret["data"] == [
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]


## query with limit clause.
def test_query_with_limit_claus3():
    sql = "select * from Student limit 5 offset 1;";
    ret = client.execute(sql)
    assert ret["rows"] == 5
    assert ret["data"] == [
        {'id': 'S002', 'name': 'sun', 'age': 11, 'birth': '2013-11-20', 'class': {'id': 'C001', 'location': 'Northwest corner', 'studentNum': 32}}, 
        {'id': 'S003', 'name': 'ben', 'age': 12, 'birth': '2012-04-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S004', 'name': 'david', 'age': 14, 'birth': '2010-01-05', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S005', 'name': 'kunting', 'age': 9, 'birth': '2015-06-23', 'class': {'id': 'C002', 'location': 'Middle', 'studentNum': 28}}, 
        {'id': 'S006', 'name': 'bob', 'age': 9, 'birth': '2015-07-07', 'class': {'id': 'C003', 'location': 'South side', 'studentNum': 33}}
    ]

## query with limit clause.
def test_query_with_limit_claus4():
    sql = "select * from Student offset 1 limit 5;";
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "syntax error, unexpected OFFSET, expecting ';' at or near [Student]."


## query with limit clause.
def test_query_with_limit_claus5():
    sql = "select count(1) from Student limit 5 offset 5;";
    ret = client.execute(sql)
    assert ret["success"] == True
    assert ret["data"] == [{ "count": 3 }]


## query with limit clause.
def test_query_with_limit_claus6():
    sql = "select count(1) from Student limit -1;";
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "LIMIT must not be negative."


## query with limit clause.
def test_query_with_limit_claus7():
    sql = "select count(1) from Student limit 10 offset -1;";
    ret = client.execute(sql)
    assert ret["success"] == False
    assert ret["message"] == "OFFSET must not be negative."


## drop mock tables   
def test_drop_mock_tables():
    sql = "drop table Student;\n"\
          "drop table Class;"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == True

