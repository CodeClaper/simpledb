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
    INDEX = 277,                   /* INDEX  */
    LIMIT = 278,                   /* LIMIT  */
    OFFSET = 279,                  /* OFFSET  */
    SHOW = 280,                    /* SHOW  */
    TABLES = 281,                  /* TABLES  */
    PRIMARY = 282,                 /* PRIMARY  */
    KEY = 283,                     /* KEY  */
    UNIQUE = 284,                  /* UNIQUE  */
    DEFAULT = 285,                 /* DEFAULT  */
    CHECK = 286,                   /* CHECK  */
    REFERENCES = 287,              /* REFERENCES  */
    FOREIGN = 288,                 /* FOREIGN  */
    MAX = 289,                     /* MAX  */
    MIN = 290,                     /* MIN  */
    COUNT = 291,                   /* COUNT  */
    SUM = 292,                     /* SUM  */
    AVG = 293,                     /* AVG  */
    REF = 294,                     /* REF  */
    TRUE = 295,                    /* TRUE  */
    FALSE = 296,                   /* FALSE  */
    NULLX = 297,                   /* NULLX  */
    AS = 298,                      /* AS  */
    COMMENT = 299,                 /* COMMENT  */
    CHAR = 300,                    /* CHAR  */
    INT = 301,                     /* INT  */
    LONG = 302,                    /* LONG  */
    VARCHAR = 303,                 /* VARCHAR  */
    STRING = 304,                  /* STRING  */
    BOOL = 305,                    /* BOOL  */
    FLOAT = 306,                   /* FLOAT  */
    DOUBLE = 307,                  /* DOUBLE  */
    DATE = 308,                    /* DATE  */
    TIMESTAMP = 309,               /* TIMESTAMP  */
    EQ = 310,                      /* EQ  */
    NE = 311,                      /* NE  */
    GT = 312,                      /* GT  */
    GE = 313,                      /* GE  */
    LT = 314,                      /* LT  */
    LE = 315,                      /* LE  */
    IN = 316,                      /* IN  */
    LIKE = 317,                    /* LIKE  */
    IS = 318,                      /* IS  */
    NOT = 319,                     /* NOT  */
    ALTER = 320,                   /* ALTER  */
    COLUMN = 321,                  /* COLUMN  */
    ADD = 322,                     /* ADD  */
    RENAME = 323,                  /* RENAME  */
    ON = 324,                      /* ON  */
    BEFORE = 325,                  /* BEFORE  */
    AFTER = 326,                   /* AFTER  */
    SYSTEM = 327,                  /* SYSTEM  */
    CONFIG = 328,                  /* CONFIG  */
    MEMORY = 329,                  /* MEMORY  */
    IDENTIFIER = 330,              /* IDENTIFIER  */
    INTVALUE = 331,                /* INTVALUE  */
    FLOATVALUE = 332,              /* FLOATVALUE  */
    STRINGVALUE = 333              /* STRINGVALUE  */
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
#define INDEX 277
#define LIMIT 278
#define OFFSET 279
#define SHOW 280
#define TABLES 281
#define PRIMARY 282
#define KEY 283
#define UNIQUE 284
#define DEFAULT 285
#define CHECK 286
#define REFERENCES 287
#define FOREIGN 288
#define MAX 289
#define MIN 290
#define COUNT 291
#define SUM 292
#define AVG 293
#define REF 294
#define TRUE 295
#define FALSE 296
#define NULLX 297
#define AS 298
#define COMMENT 299
#define CHAR 300
#define INT 301
#define LONG 302
#define VARCHAR 303
#define STRING 304
#define BOOL 305
#define FLOAT 306
#define DOUBLE 307
#define DATE 308
#define TIMESTAMP 309
#define EQ 310
#define NE 311
#define GT 312
#define GE 313
#define LT 314
#define LE 315
#define IN 316
#define LIKE 317
#define IS 318
#define NOT 319
#define ALTER 320
#define COLUMN 321
#define ADD 322
#define RENAME 323
#define ON 324
#define BEFORE 325
#define AFTER 326
#define SYSTEM 327
#define CONFIG 328
#define MEMORY 329
#define IDENTIFIER 330
#define INTVALUE 331
#define FLOATVALUE 332
#define STRINGVALUE 333

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
   SelectNode                   *select_node;
   InsertNode                   *insert_node;
   UpdateNode                   *update_node;
   DeleteNode                   *delete_node;
   DescribeNode                 *describe_node;
   ShowNode                     *show_node;
   AlterTableNode               *alter_table_node;
   Statement                    *statement;
   List                         *list;

#line 280 "y.tab.h"

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
