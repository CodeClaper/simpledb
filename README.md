# An object-oriented database engine --SimpleDb

## Introduction

SimpleDb (can also be called simpledb as well) is a light, statble and effecive object-oriented relational server-side database engine. SimpleDb is intended to design and store more intuitive data model. When you use object-oriented language to build program like java, python etc,  the object  data structure in the code and the table structure in the SimpleDb database have direct one-to-one mapping relationship. By SimpleDb, you can easily save the data in memory to disk without unnecessary data structure conversion as well as reading data from disk.

## Requirements

### Linux OS

SimpleDb is a server-side database engine and only support Linux environment for now. 


### Lex & Yacc

SimpleDb uses Lex & Yacc to realize sql lexical analyzer and parser. If you want to compile the sql file, you must have theses tools or substitutes. 

**Debian** or **Ubuntu**

```sh
sudo apt-get install flex bison
```

**Centos**

```shell
yum install flex bison
```

### GNU Readline

SimpleDb cli bases the GNU Readline as command helper, so if you want use cli, you must install GNU Readline firstly.

**Debian** or **Ubuntu**

```shell
sudo apt install libreadline-dev
```

**Centos**

```shell
yum install readline-devel
```


## Compile

Compile all Makefile:

```shell
make
```

If you want debug:

```shell
make DEBUG=1
```

## RoadMap

- [x] B-Tree data page store
- [x] Based Lex&Yacc implement sql parser
- [x] Create a table using sql
- [x] Query data using sql
- [x] Insert data using sql
- [x] Update data using sql
- [x] Delete data using sql
- [x] Query special column data using sql
- [x] Query data under conditions using sql
- [x] Query data with aggregate function sum, avg, max, min, count
- [x] Multiple data type, bool, char, int, float, double, varchar string(dynamic scalable string), date, timestamp and reference
- [x] Data type valid check
- [x] Implement MVCC, support transaction and support the four types of transaction isolation levels
- [ ] ACID
- [ ] Refer function for manipulating Reference Data.
- [ ] Subquery
- [ ] Join query
- [ ] Support index, B+Tree index store
- [ ] Support view
- [ ] Bin Log 
- [ ] Distributed
