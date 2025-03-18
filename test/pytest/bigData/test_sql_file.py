
from support.db_cli import DbClient
from support.asserts import assert_all

client = DbClient("127.0.0.1", 4083)    
client.login("root", "Zc120130211")

def test_read_sql_script():
    with open('test/pytest/bigData/sql/bo_qdbixlsx_sheet1.sql', encoding= 'utf-8') as file:
        for line in file:
            sql = line.strip()
            ret = client.execute(sql)
            assert ret["success"] == True
