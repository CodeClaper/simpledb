# test_create_table.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# --- Data type tests ---

def test_create_table_int_type():
    ret = client.execute("create table ct_int (id int primary key);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_int;")
    assert ret["success"] == True


def test_create_table_long_type():
    ret = client.execute("create table ct_long (id int primary key, val long);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_long;")
    assert ret["success"] == True


def test_create_table_char_type():
    ret = client.execute("create table ct_char (id int primary key, c char);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_char;")
    assert ret["success"] == True


def test_create_table_varchar_type():
    ret = client.execute("create table ct_vc (id int primary key, name varchar(64));")
    assert ret["success"] == True
    ret = client.execute("drop table ct_vc;")
    assert ret["success"] == True


def test_create_table_string_type():
    ret = client.execute("create table ct_str (id int primary key, `desc` string);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_str;")
    assert ret["success"] == True


def test_create_table_bool_type():
    sql = "create table ct_bool (id int primary key, active bool);\n" \
          "insert into ct_bool values (1, true);\n" \
          "insert into ct_bool values (2, false);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ct_bool;")
    assert ret["data"] == [
        {'id': 1, 'active': True},
        {'id': 2, 'active': False},
    ]
    ret = client.execute("drop table ct_bool;")
    assert ret["success"] == True


def test_create_table_float_type():
    ret = client.execute("create table ct_float (id int primary key, score float);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_float;")
    assert ret["success"] == True


def test_create_table_double_type():
    ret = client.execute("create table ct_double (id int primary key, value double);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_double;")
    assert ret["success"] == True


def test_create_table_timestamp_type():
    ret = client.execute("create table ct_ts (id int primary key, ts timestamp);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_ts;")
    assert ret["success"] == True


def test_create_table_date_type():
    ret = client.execute("create table ct_date (id int primary key, birth date);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_date;")
    assert ret["success"] == True


def test_create_table_rid_type():
    sql = "create table ct_rid (id int primary key, name varchar(32));\n" \
          "create table ct_ref (id int primary key, ref_ct ct_rid);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("drop table ct_ref;\ndrop table ct_rid;")
    assert_all(ret)


# --- Constraint tests ---

def test_create_table_not_null():
    ret = client.execute("create table ct_nn (id int primary key, name varchar(32) not null);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_nn;")
    assert ret["success"] == True


def test_create_table_unique():
    ret = client.execute("create table ct_unq (id int primary key, email varchar(32) unique);")
    assert ret["success"] == True
    ret = client.execute("drop table ct_unq;")
    assert ret["success"] == True


def test_create_table_default_value():
    sql = "create table ct_def (id int primary key, name varchar(32) default 'unknown');\n" \
          "insert into ct_def (id) values (1);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from ct_def;")
    assert ret["data"] == [{'id': 1, 'name': 'unknown'}]
    ret = client.execute("drop table ct_def;")
    assert ret["success"] == True


def test_create_table_default_null():
    sql = "create table ct_dn (id int primary key, name varchar(32) default null);"
    ret = client.execute(sql)
    assert ret["success"] == True
    ret = client.execute("drop table ct_dn;")
    assert ret["success"] == True


def test_create_table_comment():
    ret = client.execute("create table ct_cmt (id int primary key, name varchar(32) comment 'user name');")
    assert ret["success"] == True
    ret = client.execute("drop table ct_cmt;")
    assert ret["success"] == True


# --- Table-level constraint tests ---

def test_create_table_primary_key():
    ret = client.execute("create table ct_pk (id int, name varchar(32), primary key(id));")
    assert ret["success"] == True
    ret = client.execute("drop table ct_pk;")
    assert ret["success"] == True


def test_create_table_multi_column_primary_key():
    ret = client.execute("create table ct_mpk (a int, b int, primary key(a, b));")
    assert ret["success"] == True
    ret = client.execute("drop table ct_mpk;")
    assert ret["success"] == True


def test_create_table_unique_constraint():
    ret = client.execute("create table ct_uq (id int primary key, email varchar(32), unique(email));")
    assert ret["success"] == True
    ret = client.execute("drop table ct_uq;")
    assert ret["success"] == True


# --- Complex CREATE TABLE ---

def test_create_table_multiple_columns():
    sql = "create table ct_multi (id int primary key, name varchar(32), age int, score float, email varchar(64), active bool, birth date, created timestamp);"
    ret = client.execute(sql)
    assert ret["success"] == True
    ret = client.execute("drop table ct_multi;")
    assert ret["success"] == True


def test_create_table_with_all_constraints():
    sql = "create table ct_all (id int primary key, name varchar(32) not null, email varchar(64) unique default 'unknown', age int);"
    ret = client.execute(sql)
    assert ret["success"] == True
    ret = client.execute("drop table ct_all;")
    assert ret["success"] == True


def test_create_table_with_foreign_key():
    sql = "create table ct_dep (id int primary key, name varchar(32));\n" \
          "create table ct_emp (id int primary key, name varchar(32), depid int);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("drop table ct_emp;\ndrop table ct_dep;")
    assert_all(ret)


# --- Error cases ---

def test_create_table_no_name():
    ret = client.execute("create table (id int);")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_create_table_no_columns():
    ret = client.execute("create table ct_empty ();")
    assert ret["success"] == False


def test_create_table_no_parens():
    ret = client.execute("create table ct_bad id int;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def teardown_module(module):
    client.close()
