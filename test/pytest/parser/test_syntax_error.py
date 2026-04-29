# test_syntax_error.py
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)
client.login("root", "Zc120130211")


# --- Missing keywords ---

def test_missing_table_name_in_create():
    ret = client.execute("create table (id int);")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_missing_table_keyword_in_drop():
    ret = client.execute("drop nonexistent;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_missing_from_in_select():
    ret = client.execute("select * nonexistent;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_missing_into_in_insert():
    ret = client.execute("insert nonexistent values (1);")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_missing_set_in_update():
    ret = client.execute("update nonexistent x = 1;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_missing_from_in_delete():
    ret = client.execute("delete nonexistent;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Malformed SQL ---

def test_empty_sql():
    ret = client.execute(";")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_only_whitespace_newlines():
    ret = client.execute("\n;\n")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


def test_random_garbage():
    ret = client.execute("xyz abc def;")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Unmatched parentheses ---

def test_unmatched_parentheses_insert():
    ret = client.execute("insert into nonexistent values (1, 'a';")
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Wrong keyword case combinations ---
# The lexer is case-insensitive, so these should parse fine

def test_case_insensitive_create_drop():
    sql = "CREATE TABLE case_test_t (ID INT, NAME VARCHAR(32));\n" \
          "DROP TABLE case_test_t;"
    ret = client.execute(sql)
    assert_all(ret)


def test_case_insensitive_mixed_case_explain():
    # Tests that the lexer is case-insensitive for keywords
    sql = "create table case_test_t2 (id int primary key, name varchar(32));\n" \
          "insert into case_test_t2 values (1, 'test');\n" \
          "SeLeCt * FrOm case_test_t2;"
    ret = client.execute(sql)
    assert_all(ret)
    # cleanup
    ret = client.execute("drop table case_test_t2;")
    assert ret["success"] == True

# --- Multi-statement error handling ---

def test_error_in_first_statement():
    sql = "badstatement;\nselect 1;"
    ret = client.execute(sql)
    # Errors on the first statement and stops parsing
    assert ret["success"] == False
    assert "syntax error" in ret["message"]


# --- Keywords as identifiers (should work with backticks) ---

def test_backtick_identifier():
    sql = "create table `table` (id int primary key, `select` varchar(32));\n" \
          "insert into `table` values (1, 'test');\n" \
          "select * from `table`;"
    ret = client.execute(sql)
    assert_all(ret)


def test_backtick_column_alias_is_not_supported():
    # Backticks in the lexer are for identifiers (IDENTIFIER token)
    # They are NOT treated as aliases - aliases are regular identifiers
    ret = client.execute("select `select` from `table`;")
    assert ret["success"] == True


# --- Cleanup ---

def test_cleanup():
    ret = client.execute("drop table `table`;")
    assert ret["success"] == True


def teardown_module(module):
    client.close()
