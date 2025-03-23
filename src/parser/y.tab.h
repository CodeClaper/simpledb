/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_YY_Y_TAB_H_INCLUDED
# define YY_YY_Y_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    OR = 258,                      /* OR  */
    AND = 259,                     /* AND  */
    NL = 260,                      /* NL  */
    BEGINN = 261,                  /* BEGINN  */
    COMMIT = 262,                  /* COMMIT  */
    ROLLBACK = 263,                /* ROLLBACK  */
    CREATE = 264,                  /* CREATE  */
    DROP = 265,                    /* DROP  */
    SELECT = 266,                  /* SELECT  */
    INSERT = 267,                  /* INSERT  */
    UPDATE = 268,                  /* UPDATE  */
    DELETE = 269,                  /* DELETE  */
    DESCRIBE = 270,                /* DESCRIBE  */
    FROM = 271,                    /* FROM  */
    WHERE = 272,                   /* WHERE  */
    INTO = 273,                    /* INTO  */
    SET = 274,                     /* SET  */
    VALUES = 275,                  /* VALUES  */
    TABLE = 276,                   /* TABLE  */
    LIMIT = 277,                   /* LIMIT  */
    OFFSET = 278,                  /* OFFSET  */
    SHOW = 279,                    /* SHOW  */
    TABLES = 280,                  /* TABLES  */
    PRIMARY = 281,                 /* PRIMARY  */
    KEY = 282,                     /* KEY  */
    UNIQUE = 283,                  /* UNIQUE  */
    DEFAULT = 284,                 /* DEFAULT  */
    CHECK = 285,                   /* CHECK  */
    REFERENCES = 286,              /* REFERENCES  */
    FOREIGN = 287,                 /* FOREIGN  */
    MAX = 288,                     /* MAX  */
    MIN = 289,                     /* MIN  */
    COUNT = 290,                   /* COUNT  */
    SUM = 291,                     /* SUM  */
    AVG = 292,                     /* AVG  */
    REF = 293,                     /* REF  */
    TRUE = 294,                    /* TRUE  */
    FALSE = 295,                   /* FALSE  */
    NULLX = 296,                   /* NULLX  */
    AS = 297,                      /* AS  */
    COMMENT = 298,                 /* COMMENT  */
    CHAR = 299,                    /* CHAR  */
    INT = 300,                     /* INT  */
    LONG = 301,                    /* LONG  */
    VARCHAR = 302,                 /* VARCHAR  */
    STRING = 303,                  /* STRING  */
    BOOL = 304,                    /* BOOL  */
    FLOAT = 305,                   /* FLOAT  */
    DOUBLE = 306,                  /* DOUBLE  */
    DATE = 307,                    /* DATE  */
    TIMESTAMP = 308,               /* TIMESTAMP  */
    EQ = 309,                      /* EQ  */
    NE = 310,                      /* NE  */
    GT = 311,                      /* GT  */
    GE = 312,                      /* GE  */
    LT = 313,                      /* LT  */
    LE = 314,                      /* LE  */
    IN = 315,                      /* IN  */
    LIKE = 316,                    /* LIKE  */
    NOT = 317,                     /* NOT  */
    ALTER = 318,                   /* ALTER  */
    COLUMN = 319,                  /* COLUMN  */
    ADD = 320,                     /* ADD  */
    RENAME = 321,                  /* RENAME  */
    BEFORE = 322,                  /* BEFORE  */
    AFTER = 323,                   /* AFTER  */
    SYSTEM = 324,                  /* SYSTEM  */
    CONFIG = 325,                  /* CONFIG  */
    MEMORY = 326,                  /* MEMORY  */
    IDENTIFIER = 327,              /* IDENTIFIER  */
    INTVALUE = 328,                /* INTVALUE  */
    FLOATVALUE = 329,              /* FLOATVALUE  */
    STRINGVALUE = 330              /* STRINGVALUE  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif
/* Token kinds.  */
#define YYEMPTY -2
#define YYEOF 0
#define YYerror 256
#define YYUNDEF 257
#define OR 258
#define AND 259
#define NL 260
#define BEGINN 261
#define COMMIT 262
#define ROLLBACK 263
#define CREATE 264
#define DROP 265
#define SELECT 266
#define INSERT 267
#define UPDATE 268
#define DELETE 269
#define DESCRIBE 270
#define FROM 271
#define WHERE 272
#define INTO 273
#define SET 274
#define VALUES 275
#define TABLE 276
#define LIMIT 277
#define OFFSET 278
#define SHOW 279
#define TABLES 280
#define PRIMARY 281
#define KEY 282
#define UNIQUE 283
#define DEFAULT 284
#define CHECK 285
#define REFERENCES 286
#define FOREIGN 287
#define MAX 288
#define MIN 289
#define COUNT 290
#define SUM 291
#define AVG 292
#define REF 293
#define TRUE 294
#define FALSE 295
#define NULLX 296
#define AS 297
#define COMMENT 298
#define CHAR 299
#define INT 300
#define LONG 301
#define VARCHAR 302
#define STRING 303
#define BOOL 304
#define FLOAT 305
#define DOUBLE 306
#define DATE 307
#define TIMESTAMP 308
#define EQ 309
#define NE 310
#define GT 311
#define GE 312
#define LT 313
#define LE 314
#define IN 315
#define LIKE 316
#define NOT 317
#define ALTER 318
#define COLUMN 319
#define ADD 320
#define RENAME 321
#define BEFORE 322
#define AFTER 323
#define SYSTEM 324
#define CONFIG 325
#define MEMORY 326
#define IDENTIFIER 327
#define INTVALUE 328
#define FLOATVALUE 329
#define STRINGVALUE 330

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 21 "sql.y"

   char                         *strVal;
   int64_t                      intVal;
   double                       floatVal;
   bool                         boolVal;
   char                         *keyword;
   ReferValue                   *referVal;
   CompareType                  compare_type;
   DataTypeNode                 *data_type_node;
   ColumnDefName                *column_def_name;
   ColumnDefNode                *column_def_node;
   BaseTableElementNode         *base_table_element;
   ColumnDefOptNode             *column_def_opt;
   TableContraintDefNode        *table_contraint_def;
   ColumnNode                   *column_node;
   AtomNode                     *atom_node;
   ValueItemNode                *value_item_node;
   SelectionNode                *selection_node;
   ScalarExpNode                *scalar_exp_node;
   FunctionValueNode            *function_value_node;
   FunctionNode                 *function_node;
   CalculateNode                *calculate_node;
   AssignmentNode               *assignment_node;
   ConditionNode                *condition_node;
   PredicateNode                *predicate_node;
   ComparisonNode               *comparison_node;
   LikeNode                     *like_node;
   InNode                       *in_node;
   LimitClauseNode              *limit_clause_node;
   TableRefNode                 *table_ref_node;
   QuerySpecNode                *query_spec_node;
   ValuesOrQuerySpecNode        *values_or_query_spec_node;
   FromClauseNode               *from_clause_node;
   WhereClauseNode              *where_clause_node;
   TableExpNode                 *table_exp_node; 
   AddColumnDef                 *add_column_def;
   DropColumnDef                *drop_column_def;
   AlterTableAction             *alter_table_action;
   ColumnPositionDef            *column_position_def;
   CreateTableNode              *create_table_node;
   DropTableNode                *drop_table_node;
   SelectNode                   *select_node;
   InsertNode                   *insert_node;
   UpdateNode                   *update_node;
   DeleteNode                   *delete_node;
   DescribeNode                 *describe_node;
   ShowNode                     *show_node;
   AlterTableNode               *alter_table_node;
   Statement                    *statement;
   List                         *list;

#line 269 "y.tab.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


extern YYSTYPE yylval;
extern YYLTYPE yylloc;

int yyparse (List *states);


#endif /* !YY_YY_Y_TAB_H_INCLUDED  */
