# Parser Tests

基于 `src/parser/sql.y` 语法规则的 SQL 解析器 pytest 集成测试，共 **15 个文件，222 个测试函数**。

## 运行测试

```bash
# 先启动 simpledb 服务端，然后运行：
pytest test/pytest/parser/ -v

# 运行单个文件：
pytest test/pytest/parser/test_select.py -v
```

## 测试文件

| 文件 | 测试数 | 覆盖内容 |
|------|--------|----------|
| `test_transaction.py` | 6 | BEGIN, COMMIT, ROLLBACK, 多语句事务 |
| `test_create_table.py` | 28 | 全部数据类型 (INT/LONG/CHAR/VARCHAR/STRING/BOOL/FLOAT/DOUBLE/TIMESTAMP/DATE/RID), 列约束 (NOT NULL/UNIQUE/PRIMARY KEY/DEFAULT/DEFAULT NULL/COMMENT/CHECK/REFERENCES), 表约束 (PRIMARY KEY/UNIQUE/FOREIGN KEY/CHECK), 语法错误 |
| `test_create_index.py` | 7 | CREATE INDEX, CREATE UNIQUE INDEX, 单列/多列, 重复名/不存在表错误 |
| `test_drop.py` | 6 | DROP TABLE, DROP INDEX, 不存在表/索引错误, 语法错误 |
| `test_describe_show.py` | 8 | DESCRIBE / DESC, SHOW TABLES, SHOW INDEX FROM, 不存在表错误 |
| `test_explain_express.py` | 8 | EXPLAIN SELECT, EXPRESS SELECT (含 WHERE/alias/function/limit) |
| `test_alter_table.py` | 11 | ALTER TABLE ADD COLUMN (含 BEFORE/AFTER/DEFAULT/NOT NULL), DROP COLUMN, 不存在表/列错误 |
| `test_limit.py` | 11 | LIMIT, LIMIT offset,rows, LIMIT rows OFFSET offset, LIMIT 0, WHERE+LIMIT, 负值错误 |
| `test_select.py` | 31 | SELECT */column/多列, AS alias, table alias (隐式/AS), table.column, 聚合函数 (COUNT/MAX/MIN/SUM/AVG), 列算术 (+-*/), 复杂表达式, WHERE, LIMIT |
| `test_where_predicates.py` | 29 | 全部比较运算符 (=/!=/>/>=/</<=), LIKE (前缀/后缀/包含/精确), IN, AND/OR, NOT, IS TRUE/FALSE/IS NOT, 嵌套条件 |
| `test_insert.py` | 14 | INSERT VALUES (所有列/指定列/多行), 字符串/浮点/布尔/NULL/负数, INSERT FROM SELECT 子查询, 重复主键/不存在表错误 |
| `test_update_delete.py` | 14 | UPDATE SET (单列/多列/字符串/NULL), UPDATE 不带 WHERE, DELETE FROM WHERE, DELETE 复杂条件, 不存在表/语法错误 |
| `test_expressions.py` | 15 | 列/字面量算术 (+-*/), 括号表达式, 复杂表达式, expression AS alias, WHERE 中表达式, 聚合函数在表达式中 |
| `test_refer.py` | 9 | REF 直接值插入, REF(search_condition) 间接引用, (col).subcol 访问, WHERE 子列条件 |
| `test_array.py` | 10 | 一维 [] (varchar/int/float/bool), 二维 [][], 三维 [][][], 数组 NULL 元素 |
| `test_syntax_error.py` | 16 | 缺失关键字错误, 格式错误 SQL, 括号不匹配, 大小写不敏感, 多语句错误, 反引号标识符 |

## 测试模式

所有测试通过 `DbClient` 连接 simpledb 服务端（TCP socket, 127.0.0.1:4083），使用 `root / Zc120130211` 登录。

- 每个文件独立创建和清理测试表，表名使用文件前缀避免冲突
- 测试按函数定义顺序执行，后一个测试依赖前一个的建表/插入操作
- 使用 `setup_data()` + 模块级标志避免重复建表
- `teardown_module()` 关闭客户端连接
