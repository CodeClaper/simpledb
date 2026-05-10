from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# ============================================================
# BOOL type tests
# ============================================================

def test_bool_create_and_insert():
    sql = "create table type_bool (id int primary key, active bool);\n" \
          "insert into type_bool values (1, true);\n" \
          "insert into type_bool values (2, false);\n" \
          "insert into type_bool values (3, true);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_bool;")
    assert ret["data"] == [
        {"id": 1, "active": True},
        {"id": 2, "active": False},
        {"id": 3, "active": True},
    ]


def test_bool_boundary_true_false():
    """BOOL only accepts true/false (case-insensitive in some systems)."""
    ret = client.execute("insert into type_bool values (4, true);")
    assert ret["success"] == True
    ret = client.execute("insert into type_bool values (5, false);")
    assert ret["success"] == True


def test_bool_invalid_value():
    """Inserting a non-boolean string into a bool column should fail."""
    ret = client.execute("insert into type_bool values (6, 'yes');")
    assert ret["success"] == False


# ============================================================
# CHAR type tests
# ============================================================

def test_char_create_and_insert():
    sql = "create table type_char (id int primary key, sex char);\n" \
          "insert into type_char values (1, 'M');\n" \
          "insert into type_char values (2, 'F');"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_char;")
    assert ret["data"] == [
        {"id": 1, "sex": "M"},
        {"id": 2, "sex": "F"},
    ]


def test_char_boundary_single_char():
    """CHAR must be exactly 1 character."""
    ret = client.execute("insert into type_char values (3, 'X');")
    assert ret["success"] == True


def test_char_overflow_multi_char():
    """Multi-character string rejected for CHAR column."""
    ret = client.execute("insert into type_char values (4, 'MAN');")
    assert ret["success"] == False
    assert ret["message"] == "Try to convert value 'MAN' to char value type fail."


def test_char_overflow_empty():
    """Empty string should also fail for CHAR column."""
    ret = client.execute("insert into type_char values (5, '  ');")
    assert ret["success"] == False


# ============================================================
# VARCHAR type tests
# ============================================================

def test_varchar_create_and_insert():
    sql = "create table type_varchar (id int primary key, name varchar(32), addr varchar(64));\n" \
          "insert into type_varchar values (1, 'alice', 'Beijing');\n" \
          "insert into type_varchar values (2, 'bob', 'Shanghai');"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_varchar;")
    assert ret["data"] == [
        {"id": 1, "name": "alice", "addr": "Beijing"},
        {"id": 2, "name": "bob", "addr": "Shanghai"},
    ]


def test_varchar_boundary_exact_length():
    """Insert a string exactly at the varchar length limit."""
    ret = client.execute("insert into type_varchar values (3, 'abcdefghijklmnopqrstuvwxyz012345', 'OK');")
    assert ret["success"] == True


def test_varchar_overflow():
    """String exceeding varchar(n) limit should fail."""
    ret = client.execute("insert into type_varchar values (4, 'this_name_is_way_too_long_for_32_chars', 'OK');")
    assert ret["success"] == False
    assert "Exceed the limit of data length" in ret["message"]


def test_varchar_overflow2():
    """String exceeding varchar(n) limit on non-primary column."""
    sql = "create table type_varchar2 (id int primary key, code varchar(8));\n" \
          "insert into type_varchar2 values (1, '123456789');"
    ret = client.execute(sql)
    assert ret[0]["success"] == True
    assert ret[1]["success"] == False
    assert "Exceed the limit of data length: 9 > 8" in ret[1]["message"]


# ============================================================
# INT type tests
# ============================================================

def test_int_create_and_insert():
    sql = "create table type_int (id int primary key, score int, age int);\n" \
          "insert into type_int values (1, 100, 25);\n" \
          "insert into type_int values (2, -50, 30);\n" \
          "insert into type_int values (3, 0, 18);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_int;")
    assert ret["data"] == [
        {"id": 1, "score": 100, "age": 25},
        {"id": 2, "score": -50, "age": 30},
        {"id": 3, "score": 0, "age": 18},
    ]


def test_int_boundary_max():
    """INT_MAX = 2147483647 should be accepted."""
    ret = client.execute("insert into type_int values (4, 2147483647, 20);")
    assert ret["success"] == True


def test_int_boundary_min():
    """INT_MIN = -2147483648 should be accepted."""
    ret = client.execute("insert into type_int values (5, -2147483648, 20);")
    assert ret["success"] == True


def test_int_overflow_max():
    """INT_MAX + 1 should be rejected."""
    ret = client.execute("insert into type_int values (6, 2147483648, 20);")
    assert ret["success"] == False
    assert ret["message"] == "Value is int overflow for column 'score'."


def test_int_overflow_min():
    """INT_MIN - 1 should be rejected."""
    ret = client.execute("insert into type_int values (7, -2147483649, 20);")
    assert ret["success"] == False
    assert ret["message"] == "Value is int overflow for column 'score'."


def test_int_overflow_large_number():
    """A very large number far beyond INT range should be rejected as overflow."""
    ret = client.execute("insert into type_int values (8, 1314232272847489294756329290476549394673, 20);")
    assert ret["success"] == False
    assert "'1314232272847489294756329290476549394673' is overflow." in ret["message"]


# ============================================================
# LONG type tests
# ============================================================

def test_long_create_and_insert():
    sql = "create table type_long (id int primary key, total long);\n" \
          "insert into type_long values (1, 1000);\n" \
          "insert into type_long values (2, -500);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_long;")
    assert ret["data"] == [
        {"id": 1, "total": 1000},
        {"id": 2, "total": -500},
    ]


def test_long_boundary_max():
    """LONG_MAX = 2^63 - 1 = 9223372036854775807 should be accepted."""
    ret = client.execute(f"insert into type_long values (3, {(1 << 63) - 1});")
    assert ret["success"] == True


def test_long_boundary_min():
    """LONG_MIN = -2^63 should be accepted."""
    ret = client.execute(f"insert into type_long values (4, {- (1 << 63)});")
    assert ret["success"] == True


def test_long_overflow_max():
    """LONG_MAX + 1 should be rejected."""
    ret = client.execute(f"insert into type_long values (5, {(1 << 63) + 1});")
    assert ret["success"] == False
    assert "'9223372036854775809' is overflow." in ret["message"]


# ============================================================
# FLOAT type tests
# ============================================================

def test_float_create_and_insert():
    sql = "create table type_float (id int primary key, score float);\n" \
          "insert into type_float values (1, 3.14);\n" \
          "insert into type_float values (2, -0.001);\n" \
          "insert into type_float values (3, 0.0);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_float;")
    assert ret["success"] == True
    assert len(ret["data"]) == 3


def test_float_overflow():
    """A value exceeding FLT_MAX should be rejected."""
    ret = client.execute(
        "insert into type_float values (4, "
        "9999992312313131238173472748273472874827348738478274832784782748273847824782748728478294782783472893213287382738274872837827387283728892832738728478273872837827382.23);"
    )
    assert ret["success"] == False
    assert ret["message"] == "Value is float overflow for column 'score'."


def test_float_normal_precision():
    """Float with reasonable decimal precision should work."""
    ret = client.execute("insert into type_float values (5, 12345.6789);")
    assert ret["success"] == True


# ============================================================
# DOUBLE type tests
# ============================================================

def test_double_create_and_insert():
    sql = "create table type_double (id int primary key, value double);\n" \
          "insert into type_double values (1, 3.141592653589793);\n" \
          "insert into type_double values (2, -2.718281828459045);"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_double;")
    assert ret["success"] == True
    assert len(ret["data"]) == 2


def test_double_large_value():
    """Very large double values are accepted (no overflow check for double)."""
    ret = client.execute(
        "insert into type_double values (3, "
        "9999992312313131238173472748273472874827348738478274832784782748273847824782748728478294782783472893213287382738274872837827387283728892832738728478273872837827382.23);"
    )
    assert ret["success"] == True


def test_double_negative():
    """Negative double values should work."""
    ret = client.execute("insert into type_double values (4, -11.232);")
    assert ret["success"] == True
    ret = client.execute("select * from type_double where id = 4;")
    assert ret["data"][0]["value"] == -11.232


# ============================================================
# STRING type tests
# ============================================================

def test_string_create_and_insert():
    sql = "create table type_string (id int primary key, content string);\n" \
          "insert into type_string values (1, 'Hello World');\n" \
          "insert into type_string values (2, 'SimpleDb is a relational database.');"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_string;")
    assert ret["success"] == True
    assert ret["data"][0]["content"] == "Hello World"
    assert ret["data"][1]["content"] == "SimpleDb is a relational database."


def test_string_long_text():
    """STRING type should accept very long text."""
    long_text = "A" * 10000
    ret = client.execute(f"insert into type_string values (3, '{long_text}');")
    assert ret["success"] == True
    ret = client.execute("select * from type_string where id = 3;")
    assert len(ret["data"][0]["content"]) == 10000


def test_string_empty():
    """STRING type should accept empty string."""
    ret = client.execute("insert into type_string values (4, '');")
    assert ret["success"] == True


# ============================================================
# REFER type tests
# ============================================================

def test_refer_create_and_insert():
    sql = "create table type_refer_parent (id string primary key, name varchar(32));\n" \
          "create table type_refer_child (id string primary key, age int, parent type_refer_parent);"
    ret = client.execute(sql)
    assert_all(ret)


def test_refer_insert_valid():
    sql = "insert into type_refer_parent values ('P001', 'parent1');\n" \
          "insert into type_refer_child values ('C001', 10, ref(id = 'P001'));"
    ret = client.execute(sql)
    assert_all(ret)


def test_refer_select():
    ret = client.execute("select * from type_refer_child where id = 'C001';")
    assert ret["success"] == True
    assert ret["data"][0]["id"] == "C001"
    assert ret["data"][0]["age"] == 10
    assert ret["data"][0]["parent"]["id"] == "P001"
    assert ret["data"][0]["parent"]["name"] == "parent1"


def test_refer_subcolumn_access():
    """Access a subcolumn of a refer type."""
    ret = client.execute("select (parent).id as pid from type_refer_child where id = 'C001';")
    assert ret["success"] == True
    assert ret["data"] == [{"pid": "P001"}]


def test_refer_subcolumn_json():
    """Access refer with JSON-like subcolumn syntax."""
    ret = client.execute("select parent{id as pid} from type_refer_child where id = 'C001';")
    assert ret["success"] == True
    assert ret["data"] == [{"parent": {"pid": "P001"}}]


def test_refer_comparison():
    """Compare refer value with ref(...)."""
    sql = "insert into type_refer_parent values ('P002', 'parent2');\n" \
          "insert into type_refer_child values ('C002', 12, ref(id = 'P002'));"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_refer_child where parent = ref(id = 'P002');")
    assert ret["success"] == True
    assert len(ret["data"]) == 1
    assert ret["data"][0]["id"] == "C002"


# ============================================================
# DATE type tests
# ============================================================

def test_date_create_and_insert():
    sql = "create table type_date (id int primary key, birth date);\n" \
          "insert into type_date values (1, '2020-01-15');\n" \
          "insert into type_date values (2, '1999-12-31');"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_date;")
    assert ret["data"] == [
        {"id": 1, "birth": "2020-01-15"},
        {"id": 2, "birth": "1999-12-31"},
    ]


def test_date_boundary_month_start():
    """First day of month."""
    ret = client.execute("insert into type_date values (3, '2020-01-01');")
    assert ret["success"] == True


def test_date_boundary_month_end():
    """Last day of month."""
    ret = client.execute("insert into type_date values (4, '2020-01-31');")
    assert ret["success"] == True


def test_date_boundary_leap_year():
    """Feb 29 on a leap year should be valid."""
    ret = client.execute("insert into type_date values (5, '2020-02-29');")
    assert ret["success"] == True


def test_date_invalid_format():
    """Invalid date format should fail."""
    ret = client.execute("insert into type_date values (6, '2020/01/15');")
    assert ret["success"] == False
    assert "Invalid input" in ret["message"]


def test_date_invalid_month():
    """Month > 12 should fail."""
    ret = client.execute("insert into type_date values (7, '2020-13-01');")
    assert ret["success"] == False
    assert "Invalid input" in ret["message"]


def test_date_invalid_day():
    """Day > 31 should fail."""
    ret = client.execute("insert into type_date values (8, '2020-01-32');")
    assert ret["success"] == False
    assert "Invalid input" in ret["message"]


def test_date_comparison():
    """Date comparison in WHERE clause."""
    ret = client.execute("select * from type_date where birth > '2000-01-01';")
    assert ret["success"] == True
    assert len(ret["data"]) >= 1


# ============================================================
# TIMESTAMP type tests
# ============================================================

def test_timestamp_create_and_insert():
    sql = "create table type_ts (id int primary key, created timestamp);\n" \
          "insert into type_ts values (1, '2025-01-09 10:30:01');\n" \
          "insert into type_ts values (2, '2025-06-15 08:00:00');"
    ret = client.execute(sql)
    assert_all(ret)
    ret = client.execute("select * from type_ts;")
    assert ret["data"] == [
        {"id": 1, "created": "2025-01-09 10:30:01"},
        {"id": 2, "created": "2025-06-15 08:00:00"},
    ]


def test_timestamp_with_milliseconds():
    """Timestamp with millisecond precision."""
    ret = client.execute("insert into type_ts values (3, '2025-01-09 10:30:01.092');")
    assert ret["success"] == True


def test_timestamp_invalid_format():
    """Invalid timestamp format should fail."""
    ret = client.execute("insert into type_ts values (4, '2025-02-17 12:00:0X');")
    assert ret["success"] == False
    assert "Invalid input" in ret["message"]


def test_timestamp_invalid_date_part():
    """Invalid date portion in timestamp should fail."""
    ret = client.execute("insert into type_ts values (5, '2025-13-01 10:00:00');")
    assert ret["success"] == False
    assert "Invalid input" in ret["message"]


def test_timestamp_comparison():
    """Timestamp comparison in WHERE clause."""
    ret = client.execute("select * from type_ts where created > '2025-01-01 00:00:00';")
    assert ret["success"] == True
    assert len(ret["data"]) >= 2


# ============================================================
# Cleanup
# ============================================================

def test_cleanup():
    sql = "drop table type_bool;\n" \
          "drop table type_char;\n" \
          "drop table type_varchar;\n" \
          "drop table type_varchar2;\n" \
          "drop table type_int;\n" \
          "drop table type_long;\n" \
          "drop table type_float;\n" \
          "drop table type_double;\n" \
          "drop table type_string;\n" \
          "drop table type_refer_child;\n" \
          "drop table type_refer_parent;\n" \
          "drop table type_date;\n" \
          "drop table type_ts;"
    ret = client.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client.close()
