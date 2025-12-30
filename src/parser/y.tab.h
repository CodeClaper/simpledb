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
    SHOW = 271,                    /* SHOW  */
    EXPLAIN = 272,                 /* EXPLAIN  */
    FROM = 273,                    /* FROM  */
    WHERE = 274,                   /* WHERE  */
    INTO = 275,                    /* INTO  */
    SET = 276,                     /* SET  */
    VALUES = 277,                  /* VALUES  */
    TABLE = 278,                   /* TABLE  */
    INDEX = 279,                   /* INDEX  */
    LIMIT = 280,                   /* LIMIT  */
    OFFSET = 281,                  /* OFFSET  */
    TABLES = 282,                  /* TABLES  */
    PRIMARY = 283,                 /* PRIMARY  */
    KEY = 284,                     /* KEY  */
    UNIQUE = 285,                  /* UNIQUE  */
    DEFAULT = 286,                 /* DEFAULT  */
    CHECK = 287,                   /* CHECK  */
    REFERENCES = 288,              /* REFERENCES  */
    FOREIGN = 289,                 /* FOREIGN  */
    MAX = 290,                     /* MAX  */
    MIN = 291,                     /* MIN  */
    COUNT = 292,                   /* COUNT  */
    SUM = 293,                     /* SUM  */
    AVG = 294,                     /* AVG  */
    REF = 295,                     /* REF  */
    TRUE = 296,                    /* TRUE  */
    FALSE = 297,                   /* FALSE  */
    NULLX = 298,                   /* NULLX  */
    AS = 299,                      /* AS  */
    COMMENT = 300,                 /* COMMENT  */
    CHAR = 301,                    /* CHAR  */
    INT = 302,                     /* INT  */
    LONG = 303,                    /* LONG  */
    VARCHAR = 304,                 /* VARCHAR  */
    STRING = 305,                  /* STRING  */
    BOOL = 306,                    /* BOOL  */
    FLOAT = 307,                   /* FLOAT  */
    DOUBLE = 308,                  /* DOUBLE  */
    DATE = 309,                    /* DATE  */
    TIMESTAMP = 310,               /* TIMESTAMP  */
    EQ = 311,                      /* EQ  */
    NE = 312,                      /* NE  */
    GT = 313,                      /* GT  */
    GE = 314,                      /* GE  */
    LT = 315,                      /* LT  */
    LE = 316,                      /* LE  */
    IN = 317,                      /* IN  */
    LIKE = 318,                    /* LIKE  */
    IS = 319,                      /* IS  */
    NOT = 320,                     /* NOT  */
    ALTER = 321,                   /* ALTER  */
    COLUMN = 322,                  /* COLUMN  */
    ADD = 323,                     /* ADD  */
    RENAME = 324,                  /* RENAME  */
    ON = 325,                      /* ON  */
    BEFORE = 326,                  /* BEFORE  */
    AFTER = 327,                   /* AFTER  */
    SYSTEM = 328,                  /* SYSTEM  */
    CONFIG = 329,                  /* CONFIG  */
    MEMORY = 330,                  /* MEMORY  */
    IDENTIFIER = 331,              /* IDENTIFIER  */
    INTVALUE = 332,                /* INTVALUE  */
    FLOATVALUE = 333,              /* FLOATVALUE  */
    STRINGVALUE = 334              /* STRINGVALUE  */
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
#define SHOW 271
#define EXPLAIN 272
#define FROM 273
#define WHERE 274
#define INTO 275
#define SET 276
#define VALUES 277
#define TABLE 278
#define INDEX 279
#define LIMIT 280
#define OFFSET 281
#define TABLES 282
#define PRIMARY 283
#define KEY 284
#define UNIQUE 285
#define DEFAULT 286
#define CHECK 287
#define REFERENCES 288
#define FOREIGN 289
#define MAX 290
#define MIN 291
#define COUNT 292
#define SUM 293
#define AVG 294
#define REF 295
#define TRUE 296
#define FALSE 297
#define NULLX 298
#define AS 299
#define COMMENT 300
#define CHAR 301
#define INT 302
#define LONG 303
#define VARCHAR 304
#define STRING 305
#define BOOL 306
#define FLOAT 307
#define DOUBLE 308
#define DATE 309
#define TIMESTAMP 310
#define EQ 311
#define NE 312
#define GT 313
#define GE 314
#define LT 315
#define LE 316
#define IN 317
#define LIKE 318
#define IS 319
#define NOT 320
#define ALTER 321
#define COLUMN 322
#define ADD 323
#define RENAME 324
#define ON 325
#define BEFORE 326
#define AFTER 327
#define SYSTEM 328
#define CONFIG 329
#define MEMORY 330
#define IDENTIFIER 331
#define INTVALUE 332
#define FLOATVALUE 333
#define STRINGVALUE 334

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
   SearchConditionNode          *search_condition_node;
   BooleanTermNode              *boolean_term_node;
   BooleanFactorNode            *boolean_factor_node;
   BooleanTestNode              *boolean_test_node;
   BooleanPrimaryNode           *boolean_primary_node;
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
   CreateIndexNode              *create_index_node;
   DropTableNode                *drop_table_node;
   DropIndexNode                *drop_index_node;
   SelectNode                   *select_node;
   InsertNode                   *insert_node;
   UpdateNode                   *update_node;
   DeleteNode                   *delete_node;
   DescribeNode                 *describe_node;
   ShowNode                     *show_node;
   ExplainNode                  *explain_node;
   AlterTableNode               *alter_table_node;
   Statement                    *statement;
   List                         *list;

#line 284 "y.tab.h"

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
