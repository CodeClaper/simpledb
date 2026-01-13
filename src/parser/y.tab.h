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
    EXPRESS = 273,                 /* EXPRESS  */
    FROM = 274,                    /* FROM  */
    WHERE = 275,                   /* WHERE  */
    INTO = 276,                    /* INTO  */
    SET = 277,                     /* SET  */
    VALUES = 278,                  /* VALUES  */
    TABLE = 279,                   /* TABLE  */
    INDEX = 280,                   /* INDEX  */
    LIMIT = 281,                   /* LIMIT  */
    OFFSET = 282,                  /* OFFSET  */
    TABLES = 283,                  /* TABLES  */
    PRIMARY = 284,                 /* PRIMARY  */
    KEY = 285,                     /* KEY  */
    UNIQUE = 286,                  /* UNIQUE  */
    DEFAULT = 287,                 /* DEFAULT  */
    CHECK = 288,                   /* CHECK  */
    REFERENCES = 289,              /* REFERENCES  */
    FOREIGN = 290,                 /* FOREIGN  */
    MAX = 291,                     /* MAX  */
    MIN = 292,                     /* MIN  */
    COUNT = 293,                   /* COUNT  */
    SUM = 294,                     /* SUM  */
    AVG = 295,                     /* AVG  */
    REF = 296,                     /* REF  */
    TRUE = 297,                    /* TRUE  */
    FALSE = 298,                   /* FALSE  */
    NULLX = 299,                   /* NULLX  */
    AS = 300,                      /* AS  */
    COMMENT = 301,                 /* COMMENT  */
    CHAR = 302,                    /* CHAR  */
    INT = 303,                     /* INT  */
    LONG = 304,                    /* LONG  */
    VARCHAR = 305,                 /* VARCHAR  */
    STRING = 306,                  /* STRING  */
    BOOL = 307,                    /* BOOL  */
    FLOAT = 308,                   /* FLOAT  */
    DOUBLE = 309,                  /* DOUBLE  */
    DATE = 310,                    /* DATE  */
    TIMESTAMP = 311,               /* TIMESTAMP  */
    EQ = 312,                      /* EQ  */
    NE = 313,                      /* NE  */
    GT = 314,                      /* GT  */
    GE = 315,                      /* GE  */
    LT = 316,                      /* LT  */
    LE = 317,                      /* LE  */
    IN = 318,                      /* IN  */
    LIKE = 319,                    /* LIKE  */
    IS = 320,                      /* IS  */
    NOT = 321,                     /* NOT  */
    ALTER = 322,                   /* ALTER  */
    COLUMN = 323,                  /* COLUMN  */
    ADD = 324,                     /* ADD  */
    RENAME = 325,                  /* RENAME  */
    ON = 326,                      /* ON  */
    BEFORE = 327,                  /* BEFORE  */
    AFTER = 328,                   /* AFTER  */
    SYSTEM = 329,                  /* SYSTEM  */
    CONFIG = 330,                  /* CONFIG  */
    MEMORY = 331,                  /* MEMORY  */
    IDENTIFIER = 332,              /* IDENTIFIER  */
    INTVALUE = 333,                /* INTVALUE  */
    FLOATVALUE = 334,              /* FLOATVALUE  */
    STRINGVALUE = 335              /* STRINGVALUE  */
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
#define EXPRESS 273
#define FROM 274
#define WHERE 275
#define INTO 276
#define SET 277
#define VALUES 278
#define TABLE 279
#define INDEX 280
#define LIMIT 281
#define OFFSET 282
#define TABLES 283
#define PRIMARY 284
#define KEY 285
#define UNIQUE 286
#define DEFAULT 287
#define CHECK 288
#define REFERENCES 289
#define FOREIGN 290
#define MAX 291
#define MIN 292
#define COUNT 293
#define SUM 294
#define AVG 295
#define REF 296
#define TRUE 297
#define FALSE 298
#define NULLX 299
#define AS 300
#define COMMENT 301
#define CHAR 302
#define INT 303
#define LONG 304
#define VARCHAR 305
#define STRING 306
#define BOOL 307
#define FLOAT 308
#define DOUBLE 309
#define DATE 310
#define TIMESTAMP 311
#define EQ 312
#define NE 313
#define GT 314
#define GE 315
#define LT 316
#define LE 317
#define IN 318
#define LIKE 319
#define IS 320
#define NOT 321
#define ALTER 322
#define COLUMN 323
#define ADD 324
#define RENAME 325
#define ON 326
#define BEFORE 327
#define AFTER 328
#define SYSTEM 329
#define CONFIG 330
#define MEMORY 331
#define IDENTIFIER 332
#define INTVALUE 333
#define FLOATVALUE 334
#define STRINGVALUE 335

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
   ExpressNode                  *express_node;
   AlterTableNode               *alter_table_node;
   Statement                    *statement;
   List                         *list;

#line 287 "y.tab.h"

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
