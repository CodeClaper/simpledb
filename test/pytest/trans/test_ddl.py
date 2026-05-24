# test_ddl.py - DDL transaction support tests (PostgreSQL-standard)
from support.db_cli import DbClient
from support.asserts import assert_all

client1 = DbClient("127.0.0.1", 4083)
client2 = DbClient("127.0.0.1", 4083)
client1.login("root", "Zc120130211")
client2.login("root", "Zc120130211")


# ==============================================================
# SETUP
# ==============================================================

def test_create_mock_tables():
    sql = "create table DDL_Alter1 (id varchar(32) primary key, name varchar(32), age int);\n" \
          "create table DDL_Alter2 (id varchar(32) primary key, name varchar(32), val float);"
    ret = client1.execute(sql)
    assert_all(ret)


def test_insert_mock_data():
    sql = "insert into DDL_Alter1 values ('A01', 'hello', 10);\n" \
          "insert into DDL_Alter2 values ('B01', 'world', 3.14);"
    ret = client1.execute(sql)
    assert_all(ret)


# ==============================================================
# CREATE TABLE transaction tests
# ==============================================================

def test_create_table_with_begin_commit():
    """CREATE TABLE inside BEGIN ... COMMIT should persist the table."""
    sql = "begin;\n" \
          "create table DDL_CT_Commit (id varchar(32) primary key, data varchar(64));\n" \
          "commit;"
    ret = client1.execute(sql)
    assert_all(ret)
    # Table visible to the creating session after commit
    ret1 = client1.execute("select * from DDL_CT_Commit;")
    assert ret1["success"] == True and ret1["rows"] == 0
    # Table visible to other sessions after commit
    ret2 = client2.execute("select * from DDL_CT_Commit;")
    assert ret2["success"] == True and ret2["rows"] == 0


def test_create_table_with_begin_rollback():
    """CREATE TABLE inside BEGIN ... ROLLBACK should be undone (PostgreSQL standard)."""
    sql = "begin;\n" \
          "create table DDL_CT_Rollback (id varchar(32) primary key, data varchar(64));\n" \
          "rollback;"
    ret = client1.execute(sql)
    assert_all(ret)
    # Table should NOT exist after rollback
    ret1 = client1.execute("select * from DDL_CT_Rollback;")
    assert ret1["success"] == False
    ret2 = client2.execute("select * from DDL_CT_Rollback;")
    assert ret2["success"] == False


def test_create_table_visibility_before_commit():
    """Table created inside a transaction should NOT be visible to other sessions before commit."""
    sql = "begin;\n" \
          "create table DDL_CT_Visible (id varchar(32) primary key, data varchar(64));\n"
    ret = client1.execute(sql)
    assert_all(ret)
    # Visible to the creating session within the same transaction
    ret1 = client1.execute("select * from DDL_CT_Visible;")
    assert ret1["success"] == True and ret1["rows"] == 0
    # NOT visible to other sessions before commit
    ret2 = client2.execute("select * from DDL_CT_Visible;")
    assert ret2["success"] == False
    # Commit
    ret = client1.execute("commit;")
    assert ret["success"] == True
    # Now visible to other sessions
    ret2 = client2.execute("select * from DDL_CT_Visible;")
    assert ret2["success"] == True and ret2["rows"] == 0


# ==============================================================
# ALTER TABLE ADD COLUMN transaction tests
# ==============================================================

# def test_alter_add_column_with_begin_commit():
#     """ALTER TABLE ADD COLUMN inside BEGIN ... COMMIT should persist."""
#     sql = "begin;\n" \
#           "alter table DDL_Alter1 add column email varchar(64) comment 'Email address' after name;\n" \
#           "commit;"
#     ret = client1.execute(sql)
#     assert_all(ret)
#     # Column should exist after commit
#     ret = client1.execute("desc DDL_Alter1;")
#     assert ret["success"] == True
#     fields = [col["field"] for col in ret["data"]]
#     assert "email" in fields
#     # Existing data should be preserved
#     ret = client1.execute("select * from DDL_Alter1 where id = 'A01';")
#     assert ret["success"] == True and ret["rows"] == 1
#
#
# def test_alter_add_column_with_begin_rollback():
#     """ALTER TABLE ADD COLUMN inside BEGIN ... ROLLBACK should be undone (PostgreSQL standard)."""
#     sql = "begin;\n" \
#           "alter table DDL_Alter2 add column phone varchar(16) comment 'Phone number' after name;\n" \
#           "rollback;"
#     ret = client1.execute(sql)
#     assert_all(ret)
#     # Column should NOT exist after rollback
#     ret = client1.execute("desc DDL_Alter2;")
#     assert ret["success"] == True
#     fields = [col["field"] for col in ret["data"]]
#     assert "phone" not in fields
#
#
# # ==============================================================
# # ALTER TABLE DROP COLUMN transaction tests
# # ==============================================================
#
# def test_alter_drop_column_with_begin_commit():
#     """ALTER TABLE DROP COLUMN inside BEGIN ... COMMIT should persist."""
#     sql = "begin;\n" \
#           "alter table DDL_Alter1 drop column email;\n" \
#           "commit;"
#     ret = client1.execute(sql)
#     assert_all(ret)
#     # Column should be removed after commit
#     ret = client1.execute("desc DDL_Alter1;")
#     assert ret["success"] == True
#     fields = [col["field"] for col in ret["data"]]
#     assert "email" not in fields
#
#
# def test_alter_drop_column_with_begin_rollback():
#     """ALTER TABLE DROP COLUMN inside BEGIN ... ROLLBACK should be undone (PostgreSQL standard)."""
#     # First add a column outside transaction (use unique name to avoid
#     # dependency on the add-column rollback test)
#     ret = client1.execute("alter table DDL_Alter2 add column label varchar(32) after name;")
#     assert ret["success"] == True
#     # Verify column exists before the rollback test
#     ret = client1.execute("desc DDL_Alter2;")
#     fields = [col["field"] for col in ret["data"]]
#     assert "label" in fields
#
#     # Drop column inside transaction, then rollback
#     sql = "begin;\n" \
#           "alter table DDL_Alter2 drop column label;\n" \
#           "rollback;"
#     ret = client1.execute(sql)
#     assert_all(ret)
#     # Column should still exist after rollback
#     ret = client1.execute("desc DDL_Alter2;")
#     assert ret["success"] == True
#     fields = [col["field"] for col in ret["data"]]
#     assert "label" in fields
#

# ==============================================================
# DROP TABLE transaction tests
# ==============================================================

def test_drop_table_with_begin_commit():
    """DROP TABLE inside BEGIN ... COMMIT should persist."""
    ret = client1.execute("create table DDL_DT_Commit (id varchar(32) primary key, val int);")
    assert ret["success"] == True

    sql = "begin;\n" \
          "drop table DDL_DT_Commit;\n" \
          "commit;"
    ret = client1.execute(sql)
    assert_all(ret)
    # Table should be gone after commit
    ret = client1.execute("select * from DDL_DT_Commit;")
    assert ret["success"] == False


def test_drop_table_with_begin_rollback():
    """DROP TABLE inside BEGIN ... ROLLBACK should be undone (PostgreSQL standard)."""
    ret = client1.execute(
        "create table DDL_DT_Rollback ("
        "id varchar(32) primary key, "
        "name varchar(32), "
        "val int, "
        "tags varchar(32)[], "
        "content string"
        ");"
    )
    assert ret["success"] == True

    # Insert data including array and string types
    ret = client1.execute(
        "insert into DDL_DT_Rollback values "
        "('A01', 'hello', 10, ['tag1', 'tag2'], 'some long text content');"
    )
    assert ret["success"] == True
    ret = client1.execute(
        "insert into DDL_DT_Rollback values "
        "('A02', 'world', 20, ['foo', 'bar', 'baz'], 'another string value');"
    )
    assert ret["success"] == True

    ret = client1.execute("select * from DDL_DT_Rollback;")
    assert ret["success"] == True and ret["rows"] == 2

    sql = "begin;\n" \
          "drop table DDL_DT_Rollback;\n" \
          "rollback;"
    ret = client1.execute(sql)
    assert_all(ret)
    # Table should still exist after rollback
    ret = client1.execute("select * from DDL_DT_Rollback;")
    assert ret["success"] == True and ret["rows"] == 2
    # Verify the data is intact
    ret = client1.execute("select * from DDL_DT_Rollback where id = 'A01';")
    assert ret["success"] == True and ret["rows"] == 1
    row = ret["data"][0]
    assert row["name"] == "hello"
    assert row["val"] == 10
    assert row["content"] == "some long text content"


def test_drop_table_visibility_before_commit():
    """Dropped table should still be visible to other sessions before commit."""
    ret = client1.execute("create table DDL_DT_Visible (id varchar(32) primary key, val int);")
    print(ret)
    assert ret["success"] == True

    # Begin transaction and drop table
    sql = "begin;\n" \
          "drop table DDL_DT_Visible;\n"
    ret = client1.execute(sql)
    assert_all(ret)
    # Still visible to other sessions before commit
    ret2 = client2.execute("select * from DDL_DT_Visible;")
    assert ret2["success"] == True
    # Commit
    ret = client1.execute("commit;")
    assert ret["success"] == True
    # Now invisible to other sessions
    ret2 = client2.execute("select * from DDL_DT_Visible;")
    assert ret2["success"] == False


# ==============================================================
# CLEANUP
# ==============================================================

def test_cleanup():
    sql = "drop table DDL_CT_Commit;\n" \
          "drop table DDL_CT_Visible;\n" \
          "drop table DDL_DT_Rollback;\n" \
          "drop table DDL_Alter1;\n" \
          "drop table DDL_Alter2;"
    ret = client1.execute(sql)
    assert_all(ret)


def teardown_module(module):
    client1.close()
    client2.close()
