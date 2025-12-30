/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 1 "sql.y"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "list.h"
#include "intpr.h"
#include "mmgr.h"
#include "log.h"
#include "utils.h"
#include "y.tab.h"

int yywrap() {
    return 1;
}
int yylex(List *states);
int yyerror(List *states, const char *s);
extern char *current_token;

#line 91 "y.tab.c"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Use api.header.include to #include this header
   instead of duplicating it here.  */
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

#line 361 "y.tab.c"

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
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_OR = 3,                         /* OR  */
  YYSYMBOL_AND = 4,                        /* AND  */
  YYSYMBOL_5_ = 5,                         /* '+'  */
  YYSYMBOL_6_ = 6,                         /* '-'  */
  YYSYMBOL_7_ = 7,                         /* '*'  */
  YYSYMBOL_8_ = 8,                         /* '/'  */
  YYSYMBOL_9_ = 9,                         /* '('  */
  YYSYMBOL_10_ = 10,                       /* ')'  */
  YYSYMBOL_11_ = 11,                       /* '['  */
  YYSYMBOL_12_ = 12,                       /* ']'  */
  YYSYMBOL_NL = 13,                        /* NL  */
  YYSYMBOL_BEGINN = 14,                    /* BEGINN  */
  YYSYMBOL_COMMIT = 15,                    /* COMMIT  */
  YYSYMBOL_ROLLBACK = 16,                  /* ROLLBACK  */
  YYSYMBOL_CREATE = 17,                    /* CREATE  */
  YYSYMBOL_DROP = 18,                      /* DROP  */
  YYSYMBOL_SELECT = 19,                    /* SELECT  */
  YYSYMBOL_INSERT = 20,                    /* INSERT  */
  YYSYMBOL_UPDATE = 21,                    /* UPDATE  */
  YYSYMBOL_DELETE = 22,                    /* DELETE  */
  YYSYMBOL_DESCRIBE = 23,                  /* DESCRIBE  */
  YYSYMBOL_SHOW = 24,                      /* SHOW  */
  YYSYMBOL_EXPLAIN = 25,                   /* EXPLAIN  */
  YYSYMBOL_FROM = 26,                      /* FROM  */
  YYSYMBOL_WHERE = 27,                     /* WHERE  */
  YYSYMBOL_INTO = 28,                      /* INTO  */
  YYSYMBOL_SET = 29,                       /* SET  */
  YYSYMBOL_VALUES = 30,                    /* VALUES  */
  YYSYMBOL_TABLE = 31,                     /* TABLE  */
  YYSYMBOL_INDEX = 32,                     /* INDEX  */
  YYSYMBOL_LIMIT = 33,                     /* LIMIT  */
  YYSYMBOL_OFFSET = 34,                    /* OFFSET  */
  YYSYMBOL_TABLES = 35,                    /* TABLES  */
  YYSYMBOL_PRIMARY = 36,                   /* PRIMARY  */
  YYSYMBOL_KEY = 37,                       /* KEY  */
  YYSYMBOL_UNIQUE = 38,                    /* UNIQUE  */
  YYSYMBOL_DEFAULT = 39,                   /* DEFAULT  */
  YYSYMBOL_CHECK = 40,                     /* CHECK  */
  YYSYMBOL_REFERENCES = 41,                /* REFERENCES  */
  YYSYMBOL_FOREIGN = 42,                   /* FOREIGN  */
  YYSYMBOL_MAX = 43,                       /* MAX  */
  YYSYMBOL_MIN = 44,                       /* MIN  */
  YYSYMBOL_COUNT = 45,                     /* COUNT  */
  YYSYMBOL_SUM = 46,                       /* SUM  */
  YYSYMBOL_AVG = 47,                       /* AVG  */
  YYSYMBOL_REF = 48,                       /* REF  */
  YYSYMBOL_TRUE = 49,                      /* TRUE  */
  YYSYMBOL_FALSE = 50,                     /* FALSE  */
  YYSYMBOL_NULLX = 51,                     /* NULLX  */
  YYSYMBOL_AS = 52,                        /* AS  */
  YYSYMBOL_COMMENT = 53,                   /* COMMENT  */
  YYSYMBOL_CHAR = 54,                      /* CHAR  */
  YYSYMBOL_INT = 55,                       /* INT  */
  YYSYMBOL_LONG = 56,                      /* LONG  */
  YYSYMBOL_VARCHAR = 57,                   /* VARCHAR  */
  YYSYMBOL_STRING = 58,                    /* STRING  */
  YYSYMBOL_BOOL = 59,                      /* BOOL  */
  YYSYMBOL_FLOAT = 60,                     /* FLOAT  */
  YYSYMBOL_DOUBLE = 61,                    /* DOUBLE  */
  YYSYMBOL_DATE = 62,                      /* DATE  */
  YYSYMBOL_TIMESTAMP = 63,                 /* TIMESTAMP  */
  YYSYMBOL_EQ = 64,                        /* EQ  */
  YYSYMBOL_NE = 65,                        /* NE  */
  YYSYMBOL_GT = 66,                        /* GT  */
  YYSYMBOL_GE = 67,                        /* GE  */
  YYSYMBOL_LT = 68,                        /* LT  */
  YYSYMBOL_LE = 69,                        /* LE  */
  YYSYMBOL_IN = 70,                        /* IN  */
  YYSYMBOL_LIKE = 71,                      /* LIKE  */
  YYSYMBOL_IS = 72,                        /* IS  */
  YYSYMBOL_NOT = 73,                       /* NOT  */
  YYSYMBOL_ALTER = 74,                     /* ALTER  */
  YYSYMBOL_COLUMN = 75,                    /* COLUMN  */
  YYSYMBOL_ADD = 76,                       /* ADD  */
  YYSYMBOL_RENAME = 77,                    /* RENAME  */
  YYSYMBOL_ON = 78,                        /* ON  */
  YYSYMBOL_BEFORE = 79,                    /* BEFORE  */
  YYSYMBOL_AFTER = 80,                     /* AFTER  */
  YYSYMBOL_SYSTEM = 81,                    /* SYSTEM  */
  YYSYMBOL_CONFIG = 82,                    /* CONFIG  */
  YYSYMBOL_MEMORY = 83,                    /* MEMORY  */
  YYSYMBOL_IDENTIFIER = 84,                /* IDENTIFIER  */
  YYSYMBOL_INTVALUE = 85,                  /* INTVALUE  */
  YYSYMBOL_FLOATVALUE = 86,                /* FLOATVALUE  */
  YYSYMBOL_STRINGVALUE = 87,               /* STRINGVALUE  */
  YYSYMBOL_88_ = 88,                       /* ','  */
  YYSYMBOL_89_ = 89,                       /* '.'  */
  YYSYMBOL_90_ = 90,                       /* '{'  */
  YYSYMBOL_91_ = 91,                       /* '}'  */
  YYSYMBOL_92_ = 92,                       /* ';'  */
  YYSYMBOL_YYACCEPT = 93,                  /* $accept  */
  YYSYMBOL_statements = 94,                /* statements  */
  YYSYMBOL_statement = 95,                 /* statement  */
  YYSYMBOL_begin_transaction_statement = 96, /* begin_transaction_statement  */
  YYSYMBOL_commit_transaction_statement = 97, /* commit_transaction_statement  */
  YYSYMBOL_rollback_transaction_statement = 98, /* rollback_transaction_statement  */
  YYSYMBOL_create_table_statement = 99,    /* create_table_statement  */
  YYSYMBOL_create_index_statement = 100,   /* create_index_statement  */
  YYSYMBOL_drop_table_statement = 101,     /* drop_table_statement  */
  YYSYMBOL_drop_index_statement = 102,     /* drop_index_statement  */
  YYSYMBOL_select_statement = 103,         /* select_statement  */
  YYSYMBOL_insert_statement = 104,         /* insert_statement  */
  YYSYMBOL_update_statement = 105,         /* update_statement  */
  YYSYMBOL_delete_statement = 106,         /* delete_statement  */
  YYSYMBOL_describe_statement = 107,       /* describe_statement  */
  YYSYMBOL_show_statement = 108,           /* show_statement  */
  YYSYMBOL_explain_statement = 109,        /* explain_statement  */
  YYSYMBOL_alter_table_statement = 110,    /* alter_table_statement  */
  YYSYMBOL_alter_table_action = 111,       /* alter_table_action  */
  YYSYMBOL_add_column_def = 112,           /* add_column_def  */
  YYSYMBOL_drop_column_def = 113,          /* drop_column_def  */
  YYSYMBOL_column_position_def = 114,      /* column_position_def  */
  YYSYMBOL_selection = 115,                /* selection  */
  YYSYMBOL_table_exp = 116,                /* table_exp  */
  YYSYMBOL_from_clause = 117,              /* from_clause  */
  YYSYMBOL_table_ref_commalist = 118,      /* table_ref_commalist  */
  YYSYMBOL_table_ref = 119,                /* table_ref  */
  YYSYMBOL_table = 120,                    /* table  */
  YYSYMBOL_index_name = 121,               /* index_name  */
  YYSYMBOL_range_variable = 122,           /* range_variable  */
  YYSYMBOL_opt_where_clause = 123,         /* opt_where_clause  */
  YYSYMBOL_where_clause = 124,             /* where_clause  */
  YYSYMBOL_values_or_query_spec = 125,     /* values_or_query_spec  */
  YYSYMBOL_opt_values = 126,               /* opt_values  */
  YYSYMBOL_query_spec = 127,               /* query_spec  */
  YYSYMBOL_scalar_exp_commalist = 128,     /* scalar_exp_commalist  */
  YYSYMBOL_scalar_exp = 129,               /* scalar_exp  */
  YYSYMBOL_calculate = 130,                /* calculate  */
  YYSYMBOL_columns = 131,                  /* columns  */
  YYSYMBOL_base_table_element_commalist = 132, /* base_table_element_commalist  */
  YYSYMBOL_base_table_element = 133,       /* base_table_element  */
  YYSYMBOL_column_def = 134,               /* column_def  */
  YYSYMBOL_column_def_name_commalist = 135, /* column_def_name_commalist  */
  YYSYMBOL_column_def_name = 136,          /* column_def_name  */
  YYSYMBOL_data_type = 137,                /* data_type  */
  YYSYMBOL_array_dim_clause = 138,         /* array_dim_clause  */
  YYSYMBOL_column_def_opt_list = 139,      /* column_def_opt_list  */
  YYSYMBOL_column_def_opt = 140,           /* column_def_opt  */
  YYSYMBOL_table_contraint_def = 141,      /* table_contraint_def  */
  YYSYMBOL_column = 142,                   /* column  */
  YYSYMBOL_value_items = 143,              /* value_items  */
  YYSYMBOL_value_item = 144,               /* value_item  */
  YYSYMBOL_atom = 145,                     /* atom  */
  YYSYMBOL_REFERVALUE = 146,               /* REFERVALUE  */
  YYSYMBOL_BOOLVALUE = 147,                /* BOOLVALUE  */
  YYSYMBOL_assignments = 148,              /* assignments  */
  YYSYMBOL_assignment = 149,               /* assignment  */
  YYSYMBOL_search_condition = 150,         /* search_condition  */
  YYSYMBOL_boolean_term = 151,             /* boolean_term  */
  YYSYMBOL_boolean_factor = 152,           /* boolean_factor  */
  YYSYMBOL_boolean_test = 153,             /* boolean_test  */
  YYSYMBOL_boolean_primary = 154,          /* boolean_primary  */
  YYSYMBOL_predicate = 155,                /* predicate  */
  YYSYMBOL_comparison_predicate = 156,     /* comparison_predicate  */
  YYSYMBOL_like_predicate = 157,           /* like_predicate  */
  YYSYMBOL_in_predicate = 158,             /* in_predicate  */
  YYSYMBOL_limit_clause = 159,             /* limit_clause  */
  YYSYMBOL_compare = 160,                  /* compare  */
  YYSYMBOL_function = 161,                 /* function  */
  YYSYMBOL_function_value = 162,           /* function_value  */
  YYSYMBOL_non_all_function_value = 163,   /* non_all_function_value  */
  YYSYMBOL_end = 164                       /* end  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_int16 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
             && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE) \
             + YYSIZEOF (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  75
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   500

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  93
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  72
/* YYNRULES -- Number of rules.  */
#define YYNRULES  176
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  351

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   334


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       9,    10,     7,     5,    88,     6,    89,     8,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    92,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    11,     2,    12,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    90,     2,    91,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
      13,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,    39,    40,    41,    42,
      43,    44,    45,    46,    47,    48,    49,    50,    51,    52,
      53,    54,    55,    56,    57,    58,    59,    60,    61,    62,
      63,    64,    65,    66,    67,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   191,   191,   196,   203,   209,   215,   221,   228,   235,
     242,   249,   256,   263,   270,   277,   284,   291,   298,   307,
     310,   313,   317,   327,   337,   350,   359,   368,   378,   386,
     398,   409,   415,   425,   434,   440,   450,   459,   467,   474,
     483,   492,   501,   504,   511,   520,   527,   535,   546,   549,
     557,   563,   570,   576,   583,   592,   598,   604,   611,   614,
     620,   628,   635,   644,   649,   656,   665,   671,   678,   685,
     692,   699,   706,   710,   717,   725,   733,   741,   751,   757,
     764,   770,   777,   785,   808,   819,   825,   832,   840,   846,
     852,   858,   865,   871,   877,   883,   889,   895,   901,   911,
     914,   918,   925,   928,   933,   940,   946,   952,   958,   965,
     971,   978,   985,   994,  1001,  1008,  1016,  1025,  1032,  1040,
    1048,  1055,  1061,  1068,  1075,  1081,  1090,  1097,  1104,  1111,
    1118,  1128,  1136,  1145,  1149,  1155,  1161,  1168,  1177,  1183,
    1192,  1198,  1207,  1214,  1223,  1230,  1238,  1248,  1256,  1265,
    1272,  1279,  1288,  1298,  1307,  1317,  1320,  1327,  1334,  1343,
    1344,  1345,  1346,  1347,  1348,  1351,  1358,  1365,  1372,  1379,
    1388,  1395,  1402,  1410,  1417,  1426,  1427
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  static const char *const yy_sname[] =
  {
  "end of file", "error", "invalid token", "OR", "AND", "'+'", "'-'",
  "'*'", "'/'", "'('", "')'", "'['", "']'", "NL", "BEGINN", "COMMIT",
  "ROLLBACK", "CREATE", "DROP", "SELECT", "INSERT", "UPDATE", "DELETE",
  "DESCRIBE", "SHOW", "EXPLAIN", "FROM", "WHERE", "INTO", "SET", "VALUES",
  "TABLE", "INDEX", "LIMIT", "OFFSET", "TABLES", "PRIMARY", "KEY",
  "UNIQUE", "DEFAULT", "CHECK", "REFERENCES", "FOREIGN", "MAX", "MIN",
  "COUNT", "SUM", "AVG", "REF", "TRUE", "FALSE", "NULLX", "AS", "COMMENT",
  "CHAR", "INT", "LONG", "VARCHAR", "STRING", "BOOL", "FLOAT", "DOUBLE",
  "DATE", "TIMESTAMP", "EQ", "NE", "GT", "GE", "LT", "LE", "IN", "LIKE",
  "IS", "NOT", "ALTER", "COLUMN", "ADD", "RENAME", "ON", "BEFORE", "AFTER",
  "SYSTEM", "CONFIG", "MEMORY", "IDENTIFIER", "INTVALUE", "FLOATVALUE",
  "STRINGVALUE", "','", "'.'", "'{'", "'}'", "';'", "$accept",
  "statements", "statement", "begin_transaction_statement",
  "commit_transaction_statement", "rollback_transaction_statement",
  "create_table_statement", "create_index_statement",
  "drop_table_statement", "drop_index_statement", "select_statement",
  "insert_statement", "update_statement", "delete_statement",
  "describe_statement", "show_statement", "explain_statement",
  "alter_table_statement", "alter_table_action", "add_column_def",
  "drop_column_def", "column_position_def", "selection", "table_exp",
  "from_clause", "table_ref_commalist", "table_ref", "table", "index_name",
  "range_variable", "opt_where_clause", "where_clause",
  "values_or_query_spec", "opt_values", "query_spec",
  "scalar_exp_commalist", "scalar_exp", "calculate", "columns",
  "base_table_element_commalist", "base_table_element", "column_def",
  "column_def_name_commalist", "column_def_name", "data_type",
  "array_dim_clause", "column_def_opt_list", "column_def_opt",
  "table_contraint_def", "column", "value_items", "value_item", "atom",
  "REFERVALUE", "BOOLVALUE", "assignments", "assignment",
  "search_condition", "boolean_term", "boolean_factor", "boolean_test",
  "boolean_primary", "predicate", "comparison_predicate", "like_predicate",
  "in_predicate", "limit_clause", "compare", "function", "function_value",
  "non_all_function_value", "end", YY_NULLPTR
  };
  return yy_sname[yysymbol];
}
#endif

#define YYPACT_NINF (-246)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-122)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     426,   -66,   -66,   -66,   150,   183,   157,    13,   -38,    46,
     -38,   115,    75,    35,   386,  -246,  -246,  -246,  -246,  -246,
    -246,  -246,  -246,  -246,  -246,  -246,  -246,  -246,  -246,  -246,
    -246,    63,  -246,  -246,  -246,   -38,    58,   125,   -38,    58,
    -246,   281,    54,   153,   162,   168,   170,   176,   187,  -246,
    -246,  -246,   128,  -246,  -246,  -246,   194,   141,   147,  -246,
    -246,  -246,  -246,  -246,  -246,  -246,   -38,  -246,   204,   -38,
     -66,   213,   -66,  -246,   -38,  -246,  -246,  -246,   245,  -246,
     179,    58,   -66,   -66,     6,   138,     9,   171,    54,    -4,
    -246,    53,    53,    30,    30,    30,   202,     8,   303,   -38,
     -66,   231,   303,   303,   303,   303,   303,   177,   154,     8,
      -3,  -246,   -38,  -246,    11,     7,   -38,   184,  -246,  -246,
     174,  -246,  -246,    54,  -246,   180,  -246,  -246,   266,   267,
    -246,  -246,  -246,   268,   271,   283,   223,   312,   370,   152,
      57,   287,  -246,  -246,   222,  -246,  -246,  -246,  -246,  -246,
     106,   211,  -246,   -32,  -246,   202,   272,  -246,   147,   126,
     126,   252,   252,  -246,     8,   157,   297,   -66,  -246,   249,
      -9,  -246,   202,  -246,   -66,   241,   243,   -66,  -246,  -246,
     285,   311,   324,   299,  -246,    12,  -246,  -246,   407,  -246,
     325,   -38,     8,  -246,   327,  -246,  -246,  -246,  -246,  -246,
     364,   188,  -246,  -246,  -246,  -246,  -246,  -246,  -246,   303,
     329,    54,   202,  -246,   202,   120,  -246,   -38,   255,  -246,
    -246,   337,   258,  -246,    18,  -246,   194,    54,   256,  -246,
      54,     8,   -66,     1,  -246,   261,   280,  -246,   371,   280,
     202,   372,   -66,     7,  -246,  -246,  -246,   373,  -246,  -246,
    -246,  -246,  -246,  -246,  -246,   362,     8,   374,  -246,  -246,
     147,    54,  -246,   287,  -246,   178,  -246,  -246,  -246,    43,
     137,     8,  -246,    21,   375,  -246,  -246,  -246,  -246,  -246,
     156,   280,    24,  -246,   206,   280,  -246,  -246,   294,   379,
     244,    28,     8,    38,  -246,   300,   307,   -66,  -246,  -246,
      54,   309,   310,  -246,    40,  -246,   280,  -246,    41,   385,
    -246,   400,   376,  -246,    74,   405,   -38,   328,   366,   262,
    -246,   -66,    47,  -246,  -246,  -246,  -246,    48,  -246,  -246,
    -246,  -246,   377,  -246,  -246,  -246,  -246,  -246,   202,  -246,
    -246,  -246,  -246,  -246,   -66,  -246,   -38,   209,  -246,  -246,
    -246
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     2,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,   175,    19,    20,    21,     0,     0,     0,     0,     0,
      46,     0,     0,     0,     0,     0,     0,     0,     0,   133,
     134,   124,   117,   126,   129,   128,    48,    45,    66,    68,
      69,    71,   123,   130,   127,    70,     0,    55,     0,     0,
       0,     0,     0,    36,     0,     1,     3,   176,     0,    56,
       0,     0,     0,     0,   117,     0,     0,    71,     0,     0,
     121,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    58,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    33,     0,    34,     0,     0,     0,     0,    25,    26,
       0,    72,   131,     0,   125,     0,   173,   174,     0,     0,
     172,   170,   171,     0,     0,     0,     0,     0,     0,    69,
       0,   138,   140,   142,   144,   147,   149,   150,   151,   120,
       0,    49,    50,    52,    27,     0,   155,    59,    67,    74,
      75,    76,    77,    73,     0,     0,     0,     0,    62,     0,
      58,   135,     0,    31,     0,     0,     0,     0,    38,    39,
       0,     0,     0,     0,    87,     0,    80,    82,     0,    83,
       0,     0,     0,   122,     0,   165,   166,   167,   168,   169,
       0,     0,   143,   159,   160,   161,   162,   163,   164,     0,
       0,     0,     0,   132,     0,     0,   119,     0,     0,    57,
      53,    60,     0,    47,     0,    78,    48,     0,    61,    28,
       0,     0,     0,     0,    35,     0,     0,    37,     0,     0,
       0,     0,     0,     0,    90,    88,    89,     0,    92,    93,
      94,    95,    97,    96,    98,    99,     0,     0,   118,   148,
     152,     0,   153,   139,   141,     0,   145,    51,    54,   156,
       0,     0,    65,     0,     0,   137,   136,    30,    32,    41,
      42,     0,     0,    85,     0,     0,    22,    81,     0,     0,
     102,     0,     0,     0,   146,     0,     0,     0,    79,    63,
       0,     0,     0,    40,     0,   113,     0,   116,     0,     0,
     100,     0,     0,   106,     0,     0,     0,     0,     0,    84,
     103,     0,     0,   154,   158,   157,    29,     0,    43,    44,
     114,    86,     0,    91,   101,   107,   109,   108,     0,   112,
     110,   105,   104,    23,     0,    64,     0,     0,    24,   115,
     111
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -246,  -246,   406,  -246,  -246,  -246,  -246,  -246,  -246,  -246,
     409,  -246,  -246,  -246,  -246,  -246,  -246,  -246,  -246,  -246,
    -246,  -246,   254,   197,  -246,  -246,   207,    -5,     5,   208,
     257,  -246,   155,  -246,  -246,   354,    15,  -246,  -221,  -246,
     210,   218,  -245,  -207,  -246,  -246,  -246,   136,  -246,   -82,
     -40,   -35,  -246,  -246,  -190,  -246,   225,  -113,   246,   259,
     320,  -246,  -246,  -246,  -246,  -246,  -246,  -246,  -246,   143,
     367,    -2
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,   177,   178,
     179,   303,    56,   100,   101,   151,   152,   153,    80,   220,
     156,   157,   167,   228,   168,    57,   138,    59,   224,   185,
     186,   187,   282,   188,   255,   290,   319,   320,   189,    60,
      86,    61,    62,    63,    64,   170,   171,   140,   141,   142,
     143,   144,   145,   146,   147,   148,   223,   209,    65,   133,
     128,    32
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      33,    34,    89,    68,   212,    70,    87,    90,   124,   127,
     127,   132,   132,   132,   139,   149,   120,   125,   155,   122,
     218,    58,   242,   201,   172,   266,    31,   169,   270,   175,
      78,   299,   283,    82,   305,   291,   304,   130,   321,   125,
     308,    66,   221,   180,    83,   181,    67,   182,   323,   183,
     330,   332,   219,    90,   139,   139,    85,   344,   345,   233,
     212,   108,   125,    88,   110,    42,    74,   213,   111,   114,
     113,   322,    69,   139,   283,   294,    77,   295,   283,   231,
     118,   119,   225,    88,   123,    42,   117,   176,   193,    31,
     139,   184,    52,    31,     6,    97,    98,   123,   154,   331,
     243,    87,    48,    49,    50,    51,   271,   174,   173,   123,
     258,   190,   306,    58,    52,   131,   271,   158,   159,   160,
     161,   162,    48,    49,    50,   336,   123,   284,   306,   306,
     139,   296,   139,   105,   106,   271,   123,    52,   126,    53,
      54,    55,    79,   103,   104,   105,   106,    71,   121,   169,
      72,   200,   103,   104,   105,   106,   165,    81,   139,    53,
      54,    55,    91,   164,    40,   229,    41,   166,    42,    49,
      50,    92,   234,   165,   225,   237,   262,    93,   107,    94,
      58,    35,    36,   254,   166,    95,   257,   273,    37,   298,
     107,   212,    90,   265,   102,   275,    96,   216,   259,   107,
      43,    44,    45,    46,    47,    48,    49,    50,    51,   212,
     225,   136,   212,    42,    38,    39,   307,    97,    98,   350,
      99,   293,   210,   211,   260,   347,    90,    49,    50,   102,
     277,   278,   136,   109,    42,   301,   302,   134,   135,   112,
     286,    52,    53,    54,    55,    43,    44,    45,    46,    47,
      48,    49,    50,    51,   115,   311,   139,   116,   155,  -121,
     327,   163,   191,   192,   194,    90,    43,    44,    45,    46,
      47,    48,    49,    50,    51,   137,   195,   196,   197,   337,
     312,   198,   313,   314,   315,   316,    52,    53,    54,    55,
      41,   214,    42,   199,   215,   326,   137,   317,   312,   217,
     313,   314,   315,   316,   107,   222,   227,    84,    53,    54,
      55,   339,    41,   230,    42,   317,   235,   318,   236,   343,
     239,   136,   238,    42,    43,    44,    45,    46,    47,    48,
      49,    50,    51,   240,   256,   318,   241,   120,   261,   219,
     212,   349,   348,   269,   274,   279,    43,    44,    45,    46,
      47,    48,    49,    50,    51,    43,    44,    45,    46,    47,
      48,    49,    50,    51,   184,    84,    53,    54,    55,   103,
     104,   105,   106,   289,   121,   103,   104,   105,   106,   309,
     281,   285,   288,   292,   300,   324,    75,    52,    53,    54,
      55,   310,   325,   328,   329,   333,    52,    53,    54,    55,
       1,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,   334,   335,   338,   340,   107,   341,   346,   226,
      76,    73,   107,   272,   267,   297,   268,   232,   203,   204,
     205,   206,   207,   208,   203,   204,   205,   206,   207,   208,
       1,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,   150,   287,   280,   342,   276,   202,   263,   129,
      13,   244,   245,   246,   247,   248,   249,   250,   251,   252,
     253,     0,     0,   264,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    67,     0,     0,     0,     0,     0,     0,     0,     0,
      13
};

static const yytype_int16 yycheck[] =
{
       2,     3,    42,     8,     3,    10,    41,    42,    12,    91,
      92,    93,    94,    95,    96,    97,    10,     9,    27,    10,
      52,     6,    10,   136,    27,   215,    92,   109,    10,    18,
      35,    10,   239,    38,    10,   256,   281,     7,    10,     9,
     285,    28,   155,    36,    39,    38,    84,    40,    10,    42,
      10,    10,    84,    88,   136,   137,    41,    10,    10,   172,
       3,    66,     9,     9,    69,    11,    31,    10,    70,    74,
      72,   292,    26,   155,   281,   265,    13,    34,   285,    88,
      82,    83,   164,     9,    88,    11,    81,    76,   123,    92,
     172,    84,    84,    92,    19,    89,    90,    88,   100,   306,
      88,   136,    48,    49,    50,    51,    88,   112,   110,    88,
     192,   116,    88,    98,    84,    85,    88,   102,   103,   104,
     105,   106,    48,    49,    50,    51,    88,   240,    88,    88,
     212,    88,   214,     7,     8,    88,    88,    84,    85,    85,
      86,    87,    84,     5,     6,     7,     8,    32,    10,   231,
      35,   136,     5,     6,     7,     8,    19,    32,   240,    85,
      86,    87,     9,     9,     7,   167,     9,    30,    11,    49,
      50,     9,   174,    19,   256,   177,   211,     9,    52,     9,
     165,    31,    32,   188,    30,     9,   191,   227,    38,   271,
      52,     3,   227,    73,    88,   230,     9,    91,    10,    52,
      43,    44,    45,    46,    47,    48,    49,    50,    51,     3,
     292,     9,     3,    11,    31,    32,    10,    89,    90,    10,
      26,   261,    70,    71,   209,   338,   261,    49,    50,    88,
     232,   233,     9,    29,    11,    79,    80,    94,    95,    26,
     242,    84,    85,    86,    87,    43,    44,    45,    46,    47,
      48,    49,    50,    51,     9,    11,   338,    78,    27,    88,
     300,    84,    78,    89,    84,   300,    43,    44,    45,    46,
      47,    48,    49,    50,    51,    73,    10,    10,    10,   314,
      36,    10,    38,    39,    40,    41,    84,    85,    86,    87,
       9,     4,    11,    10,    72,   297,    73,    53,    36,    88,
      38,    39,    40,    41,    52,    33,     9,    84,    85,    86,
      87,   316,     9,    64,    11,    53,    75,    73,    75,   321,
       9,     9,    37,    11,    43,    44,    45,    46,    47,    48,
      49,    50,    51,     9,     9,    73,    37,    10,     9,    84,
       3,   346,   344,    85,    88,    84,    43,    44,    45,    46,
      47,    48,    49,    50,    51,    43,    44,    45,    46,    47,
      48,    49,    50,    51,    84,    84,    85,    86,    87,     5,
       6,     7,     8,    11,    10,     5,     6,     7,     8,    85,
       9,     9,     9,     9,     9,    85,     0,    84,    85,    86,
      87,    12,    85,    84,    84,    10,    84,    85,    86,    87,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    12,    37,     9,    87,    52,    51,    41,   165,
      14,    12,    52,   226,   217,   270,   218,   170,    64,    65,
      66,    67,    68,    69,    64,    65,    66,    67,    68,    69,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    98,   243,   236,   319,   231,   137,   212,    92,
      74,    54,    55,    56,    57,    58,    59,    60,    61,    62,
      63,    -1,    -1,   214,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    84,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      74
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    74,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,    92,   164,   164,   164,    31,    32,    38,    31,    32,
       7,     9,    11,    43,    44,    45,    46,    47,    48,    49,
      50,    51,    84,    85,    86,    87,   115,   128,   129,   130,
     142,   144,   145,   146,   147,   161,    28,    84,   120,    26,
     120,    32,    35,   103,    31,     0,    95,    13,   120,    84,
     121,    32,   120,   121,    84,   129,   143,   144,     9,   143,
     144,     9,     9,     9,     9,     9,     9,    89,    90,    26,
     116,   117,    88,     5,     6,     7,     8,    52,   120,    29,
     120,   164,    26,   164,   120,     9,    78,   121,   164,   164,
      10,    10,    10,    88,    12,     9,    85,   142,   163,   163,
       7,    85,   142,   162,   162,   162,     9,    73,   129,   142,
     150,   151,   152,   153,   154,   155,   156,   157,   158,   142,
     128,   118,   119,   120,   164,    27,   123,   124,   129,   129,
     129,   129,   129,    84,     9,    19,    30,   125,   127,   142,
     148,   149,    27,   164,   120,    18,    76,   111,   112,   113,
      36,    38,    40,    42,    84,   132,   133,   134,   136,   141,
     120,    78,    89,   144,    84,    10,    10,    10,    10,    10,
     129,   150,   153,    64,    65,    66,    67,    68,    69,   160,
      70,    71,     3,    10,     4,    72,    91,    88,    52,    84,
     122,   150,    33,   159,   131,   142,   115,     9,   126,   164,
      64,    88,   123,   150,   164,    75,    75,   164,    37,     9,
       9,    37,    10,    88,    54,    55,    56,    57,    58,    59,
      60,    61,    62,    63,   120,   137,     9,   120,   142,    10,
     129,     9,   144,   151,   152,    73,   147,   119,   122,    85,
      10,    88,   116,   143,    88,   144,   149,   164,   164,    84,
     134,     9,   135,   136,   150,     9,   164,   133,     9,    11,
     138,   131,     9,   143,   147,    34,    88,   125,   142,    10,
       9,    79,    80,   114,   135,    10,    88,    10,   135,    85,
      12,    11,    36,    38,    39,    40,    41,    53,    73,   139,
     140,    10,   131,    10,    85,    85,   164,   143,    84,    84,
      10,   136,    10,    10,    12,    37,    51,   144,     9,   120,
      87,    51,   140,   164,    10,    10,    41,   150,   164,   120,
      10
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,    93,    94,    94,    95,    95,    95,    95,    95,    95,
      95,    95,    95,    95,    95,    95,    95,    95,    95,    96,
      97,    98,    99,   100,   100,   101,   102,   103,   104,   104,
     105,   106,   106,   107,   108,   108,   109,   110,   111,   111,
     112,   113,   114,   114,   114,   115,   115,   116,   117,   117,
     118,   118,   119,   119,   119,   120,   121,   122,   123,   123,
     124,   125,   125,   126,   126,   127,   128,   128,   129,   129,
     129,   129,   129,   129,   130,   130,   130,   130,   131,   131,
     132,   132,   133,   133,   134,   135,   135,   136,   137,   137,
     137,   137,   137,   137,   137,   137,   137,   137,   137,   138,
     138,   138,   139,   139,   139,   140,   140,   140,   140,   140,
     140,   140,   140,   141,   141,   141,   141,   142,   142,   142,
     142,   143,   143,   144,   144,   144,   145,   145,   145,   145,
     145,   146,   146,   147,   147,   148,   148,   149,   150,   150,
     151,   151,   152,   152,   153,   153,   153,   154,   154,   155,
     155,   155,   156,   157,   158,   159,   159,   159,   159,   160,
     160,   160,   160,   160,   160,   161,   161,   161,   161,   161,
     162,   162,   162,   163,   163,   164,   164
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     2,
       2,     2,     7,     9,    10,     4,     4,     4,     5,     8,
       6,     4,     6,     3,     3,     5,     2,     5,     1,     1,
       4,     3,     0,     2,     2,     1,     1,     3,     0,     2,
       1,     3,     1,     2,     3,     1,     1,     1,     0,     1,
       2,     2,     1,     3,     5,     3,     1,     3,     1,     1,
       1,     1,     3,     3,     3,     3,     3,     3,     1,     3,
       1,     3,     1,     1,     4,     1,     3,     1,     1,     1,
       1,     4,     1,     1,     1,     1,     1,     1,     1,     0,
       2,     3,     0,     1,     2,     2,     1,     2,     2,     2,
       2,     4,     2,     4,     5,     7,     4,     1,     5,     4,
       3,     1,     3,     1,     1,     3,     1,     1,     1,     1,
       1,     3,     4,     1,     1,     1,     3,     3,     1,     3,
       1,     3,     1,     2,     1,     3,     4,     1,     3,     1,
       1,     1,     3,     3,     5,     0,     2,     4,     4,     1,
       1,     1,     1,     1,     1,     4,     4,     4,     4,     4,
       1,     1,     1,     1,     1,     1,     2
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (states, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF

/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YYLOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

# ifndef YYLOCATION_PRINT

#  if defined YY_LOCATION_PRINT

   /* Temporary convenience wrapper in case some people defined the
      undocumented and private YY_LOCATION_PRINT macros.  */
#   define YYLOCATION_PRINT(File, Loc)  YY_LOCATION_PRINT(File, *(Loc))

#  elif defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static int
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  int res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
}

#   define YYLOCATION_PRINT  yy_location_print_

    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT(File, Loc)  YYLOCATION_PRINT(File, &(Loc))

#  else

#   define YYLOCATION_PRINT(File, Loc) ((void) 0)
    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT  YYLOCATION_PRINT

#  endif
# endif /* !defined YYLOCATION_PRINT */


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, Location, states); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, List *states)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  YY_USE (states);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, List *states)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YYLOCATION_PRINT (yyo, yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp, states);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp,
                 int yyrule, List *states)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)],
                       &(yylsp[(yyi + 1) - (yynrhs)]), states);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule, states); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
  YYLTYPE *yylloc;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif



static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yystrlen (yysymbol_name (yyarg[yyi]));
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp = yystpcpy (yyp, yysymbol_name (yyarg[yyi++]));
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, List *states)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  YY_USE (states);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/* Lookahead token kind.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Location data for the lookahead symbol.  */
YYLTYPE yylloc
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
/* Number of syntax errors so far.  */
int yynerrs;




/*----------.
| yyparse.  |
`----------*/

int
yyparse (List *states)
{
    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

    /* The location stack: array, bottom, top.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls = yylsa;
    YYLTYPE *yylsp = yyls;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

  /* The locations where the error started and ended.  */
  YYLTYPE yyerror_range[3];

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  yylsp[0] = yylloc;
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yyls1, yysize * YYSIZEOF (*yylsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
        yyls = yyls1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (states);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      yyerror_range[1] = yylloc;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location. */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  yyerror_range[1] = yyloc;
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* statements: statement  */
#line 192 "sql.y"
        {
            append_list(states, (yyvsp[0].statement));
            (yyval.list) = states;
        }
#line 2153 "y.tab.c"
    break;

  case 3: /* statements: statements statement  */
#line 197 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].statement));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 2162 "y.tab.c"
    break;

  case 4: /* statement: begin_transaction_statement  */
#line 204 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = BEGIN_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2172 "y.tab.c"
    break;

  case 5: /* statement: commit_transaction_statement  */
#line 210 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = COMMIT_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2182 "y.tab.c"
    break;

  case 6: /* statement: rollback_transaction_statement  */
#line 216 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ROLLBACK_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2192 "y.tab.c"
    break;

  case 7: /* statement: create_table_statement  */
#line 222 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_TABLE_STMT;
            statement->create_table_node = (yyvsp[0].create_table_node);
            (yyval.statement) = statement;
        }
#line 2203 "y.tab.c"
    break;

  case 8: /* statement: create_index_statement  */
#line 229 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_INDEX_STMT;
            statement->create_index_node = (yyvsp[0].create_index_node);
            (yyval.statement) = statement;
        }
#line 2214 "y.tab.c"
    break;

  case 9: /* statement: drop_table_statement  */
#line 236 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_TABLE_STMT;
            statement->drop_table_node = (yyvsp[0].drop_table_node);
            (yyval.statement) = statement;
        }
#line 2225 "y.tab.c"
    break;

  case 10: /* statement: drop_index_statement  */
#line 243 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_INDEX_STMT;
            statement->drop_index_node = (yyvsp[0].drop_index_node);
            (yyval.statement) = statement;
        }
#line 2236 "y.tab.c"
    break;

  case 11: /* statement: select_statement  */
#line 250 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SELECT_STMT;
            statement->select_node = (yyvsp[0].select_node);
            (yyval.statement) = statement;
        }
#line 2247 "y.tab.c"
    break;

  case 12: /* statement: insert_statement  */
#line 257 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = INSERT_STMT;
            statement->insert_node = (yyvsp[0].insert_node);
            (yyval.statement) = statement;
        }
#line 2258 "y.tab.c"
    break;

  case 13: /* statement: update_statement  */
#line 264 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = UPDATE_STMT;
            statement->update_node = (yyvsp[0].update_node);
            (yyval.statement) = statement;
        }
#line 2269 "y.tab.c"
    break;

  case 14: /* statement: delete_statement  */
#line 271 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DELETE_STMT;
            statement->delete_node = (yyvsp[0].delete_node);
            (yyval.statement) = statement;
        }
#line 2280 "y.tab.c"
    break;

  case 15: /* statement: describe_statement  */
#line 278 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DESCRIBE_STMT;
            statement->describe_node = (yyvsp[0].describe_node);
            (yyval.statement) = statement;
        }
#line 2291 "y.tab.c"
    break;

  case 16: /* statement: show_statement  */
#line 285 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SHOW_STMT;
            statement->show_node = (yyvsp[0].show_node);
            (yyval.statement) = statement;
        }
#line 2302 "y.tab.c"
    break;

  case 17: /* statement: explain_statement  */
#line 292 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = EXPLAIN_STMT;
            statement->explain_node = (yyvsp[0].explain_node);
            (yyval.statement) = statement;
        }
#line 2313 "y.tab.c"
    break;

  case 18: /* statement: alter_table_statement  */
#line 299 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ALTER_TABLE_STMT;
            statement->alter_table_node = (yyvsp[0].alter_table_node);
            (yyval.statement) = statement;
        }
#line 2324 "y.tab.c"
    break;

  case 22: /* create_table_statement: CREATE TABLE table '(' base_table_element_commalist ')' end  */
#line 318 "sql.y"
        {
            CreateTableNode *create_table_node = instance(CreateTableNode);
            create_table_node->table_name = (yyvsp[-4].strVal);
            create_table_node->base_table_element_commalist = (yyvsp[-2].list);
            (yyval.create_table_node) = create_table_node;
        }
#line 2335 "y.tab.c"
    break;

  case 23: /* create_index_statement: CREATE INDEX index_name ON table '(' columns ')' end  */
#line 328 "sql.y"
        {
            CreateIndexNode *create_index_node = instance(CreateIndexNode);
            create_index_node->index_name = (yyvsp[-6].strVal);
            create_index_node->table_name = (yyvsp[-4].strVal);
            create_index_node->is_unique = false;
            create_index_node->type = BTREE_INDEX;
            create_index_node->columns = (yyvsp[-2].list);
            (yyval.create_index_node) = create_index_node;
        }
#line 2349 "y.tab.c"
    break;

  case 24: /* create_index_statement: CREATE UNIQUE INDEX index_name ON table '(' columns ')' end  */
#line 338 "sql.y"
        {
            CreateIndexNode *create_index_node = instance(CreateIndexNode);
            create_index_node->index_name = (yyvsp[-6].strVal);
            create_index_node->table_name = (yyvsp[-4].strVal);
            create_index_node->is_unique = true;
            create_index_node->type = BTREE_INDEX;
            create_index_node->columns = (yyvsp[-2].list);
            (yyval.create_index_node) = create_index_node;
        }
#line 2363 "y.tab.c"
    break;

  case 25: /* drop_table_statement: DROP TABLE table end  */
#line 351 "sql.y"
        {
            DropTableNode *drop_table_node = instance(DropTableNode);
            drop_table_node->table_name = (yyvsp[-1].strVal);
            (yyval.drop_table_node) = drop_table_node;
        }
#line 2373 "y.tab.c"
    break;

  case 26: /* drop_index_statement: DROP INDEX index_name end  */
#line 360 "sql.y"
        {
            DropIndexNode *drop_index_node = instance(DropIndexNode);
            drop_index_node->index_name = (yyvsp[-1].strVal);
            (yyval.drop_index_node) = drop_index_node;
        }
#line 2383 "y.tab.c"
    break;

  case 27: /* select_statement: SELECT selection table_exp end  */
#line 369 "sql.y"
        {
            SelectNode *select_node = instance(SelectNode);
            select_node->selection = (yyvsp[-2].selection_node);
            select_node->table_exp = (yyvsp[-1].table_exp_node);
            (yyval.select_node) = select_node;
        }
#line 2394 "y.tab.c"
    break;

  case 28: /* insert_statement: INSERT INTO table values_or_query_spec end  */
#line 379 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = true;
            node->table_name = (yyvsp[-2].strVal);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2406 "y.tab.c"
    break;

  case 29: /* insert_statement: INSERT INTO table '(' columns ')' values_or_query_spec end  */
#line 387 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = false;
            node->table_name = (yyvsp[-5].strVal);
            node->column_list = (yyvsp[-3].list);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2419 "y.tab.c"
    break;

  case 30: /* update_statement: UPDATE table SET assignments opt_where_clause end  */
#line 399 "sql.y"
        {
            UpdateNode *node = instance(UpdateNode);
            node->table_name = (yyvsp[-4].strVal);
            node->assignment_list = (yyvsp[-2].list);
            node->where_clause = (yyvsp[-1].where_clause_node);
            (yyval.update_node) = node;
        }
#line 2431 "y.tab.c"
    break;

  case 31: /* delete_statement: DELETE FROM table end  */
#line 410 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.delete_node) = node;
        }
#line 2441 "y.tab.c"
    break;

  case 32: /* delete_statement: DELETE FROM table WHERE search_condition end  */
#line 416 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-3].strVal);
            node->condition_node = (yyvsp[-1].search_condition_node);
            (yyval.delete_node) = node;
        }
#line 2452 "y.tab.c"
    break;

  case 33: /* describe_statement: DESCRIBE table end  */
#line 426 "sql.y"
        {
            DescribeNode *node = instance(DescribeNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.describe_node) = node;
        }
#line 2462 "y.tab.c"
    break;

  case 34: /* show_statement: SHOW TABLES end  */
#line 435 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_TABLES;
            (yyval.show_node) = node;
        }
#line 2472 "y.tab.c"
    break;

  case 35: /* show_statement: SHOW INDEX FROM table end  */
#line 441 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_IDNEXS;
            node->table_name = (yyvsp[-1].strVal);
            (yyval.show_node) = node;
        }
#line 2483 "y.tab.c"
    break;

  case 36: /* explain_statement: EXPLAIN select_statement  */
#line 451 "sql.y"
        {
            ExplainNode *node = instance(ExplainNode);
            node->select_node = (yyvsp[0].select_node);
            (yyval.explain_node) = node;
        }
#line 2493 "y.tab.c"
    break;

  case 37: /* alter_table_statement: ALTER TABLE table alter_table_action end  */
#line 460 "sql.y"
        {
            (yyval.alter_table_node) = instance(AlterTableNode);
            (yyval.alter_table_node)->table_name = (yyvsp[-2].strVal);
            (yyval.alter_table_node)->action = (yyvsp[-1].alter_table_action);
        }
#line 2503 "y.tab.c"
    break;

  case 38: /* alter_table_action: add_column_def  */
#line 468 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_ADD_COLUMN;
            action->action.add_column = (yyvsp[0].add_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2514 "y.tab.c"
    break;

  case 39: /* alter_table_action: drop_column_def  */
#line 475 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_DROP_COLUMN;
            action->action.drop_column = (yyvsp[0].drop_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2525 "y.tab.c"
    break;

  case 40: /* add_column_def: ADD COLUMN column_def column_position_def  */
#line 484 "sql.y"
        {
            AddColumnDef *node = instance(AddColumnDef);
            node->column_def = (yyvsp[-1].column_def_node);
            node->position_def = (yyvsp[0].column_position_def);
            (yyval.add_column_def) = node;
        }
#line 2536 "y.tab.c"
    break;

  case 41: /* drop_column_def: DROP COLUMN IDENTIFIER  */
#line 493 "sql.y"
        {
            DropColumnDef *node = instance(DropColumnDef);
            node->column_name = (yyvsp[0].strVal);
            (yyval.drop_column_def) = node;
        }
#line 2546 "y.tab.c"
    break;

  case 42: /* column_position_def: %empty  */
#line 501 "sql.y"
    {
        (yyval.column_position_def) = NULL;
    }
#line 2554 "y.tab.c"
    break;

  case 43: /* column_position_def: BEFORE IDENTIFIER  */
#line 505 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_BEFORE;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2565 "y.tab.c"
    break;

  case 44: /* column_position_def: AFTER IDENTIFIER  */
#line 512 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_AFTER;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2576 "y.tab.c"
    break;

  case 45: /* selection: scalar_exp_commalist  */
#line 521 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = false;
            selection_node->scalar_exp_list = (yyvsp[0].list);
            (yyval.selection_node) = selection_node;
        }
#line 2587 "y.tab.c"
    break;

  case 46: /* selection: '*'  */
#line 528 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = true;
            (yyval.selection_node) = selection_node;
        }
#line 2597 "y.tab.c"
    break;

  case 47: /* table_exp: from_clause opt_where_clause limit_clause  */
#line 536 "sql.y"
        {
            TableExpNode *table_exp = instance(TableExpNode);
            table_exp->from_clause = (yyvsp[-2].from_clause_node);
            table_exp->where_clause = (yyvsp[-1].where_clause_node);
            table_exp->limit_clause = (yyvsp[0].limit_clause_node);
            (yyval.table_exp_node) = table_exp;
        }
#line 2609 "y.tab.c"
    break;

  case 48: /* from_clause: %empty  */
#line 546 "sql.y"
        {
            (yyval.from_clause_node) = NULL;
        }
#line 2617 "y.tab.c"
    break;

  case 49: /* from_clause: FROM table_ref_commalist  */
#line 550 "sql.y"
        {
            FromClauseNode *from_clause = instance(FromClauseNode);
            from_clause->from = (yyvsp[0].list);
            (yyval.from_clause_node) = from_clause;
        }
#line 2627 "y.tab.c"
    break;

  case 50: /* table_ref_commalist: table_ref  */
#line 558 "sql.y"
        {
            List *list = create_list(NODE_TABLE_REFER);
            append_list(list, (yyvsp[0].table_ref_node));
            (yyval.list) = list;
        }
#line 2637 "y.tab.c"
    break;

  case 51: /* table_ref_commalist: table_ref_commalist ',' table_ref  */
#line 564 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].table_ref_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2646 "y.tab.c"
    break;

  case 52: /* table_ref: table  */
#line 571 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2656 "y.tab.c"
    break;

  case 53: /* table_ref: table range_variable  */
#line 577 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-1].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2667 "y.tab.c"
    break;

  case 54: /* table_ref: table AS range_variable  */
#line 584 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-2].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2678 "y.tab.c"
    break;

  case 55: /* table: IDENTIFIER  */
#line 593 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2686 "y.tab.c"
    break;

  case 56: /* index_name: IDENTIFIER  */
#line 599 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2694 "y.tab.c"
    break;

  case 57: /* range_variable: IDENTIFIER  */
#line 605 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2702 "y.tab.c"
    break;

  case 58: /* opt_where_clause: %empty  */
#line 611 "sql.y"
        {
            (yyval.where_clause_node) = NULL;
        }
#line 2710 "y.tab.c"
    break;

  case 59: /* opt_where_clause: where_clause  */
#line 615 "sql.y"
        {
            (yyval.where_clause_node) = (yyvsp[0].where_clause_node);
        }
#line 2718 "y.tab.c"
    break;

  case 60: /* where_clause: WHERE search_condition  */
#line 621 "sql.y"
        {
            WhereClauseNode *where_clause_node = instance(WhereClauseNode);
            where_clause_node->condition = (yyvsp[0].search_condition_node);
            (yyval.where_clause_node) = where_clause_node;
        }
#line 2728 "y.tab.c"
    break;

  case 61: /* values_or_query_spec: VALUES opt_values  */
#line 629 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_VALUES;
            values_or_query_spec->values = (yyvsp[0].list);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2739 "y.tab.c"
    break;

  case 62: /* values_or_query_spec: query_spec  */
#line 636 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_QUERY_SPEC;
            values_or_query_spec->query_spec = (yyvsp[0].query_spec_node);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2750 "y.tab.c"
    break;

  case 63: /* opt_values: '(' value_items ')'  */
#line 645 "sql.y"
        {
            (yyval.list) = create_list(NODE_LIST);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2759 "y.tab.c"
    break;

  case 64: /* opt_values: opt_values ',' '(' value_items ')'  */
#line 650 "sql.y"
        {
            (yyval.list) = (yyvsp[-4].list);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2768 "y.tab.c"
    break;

  case 65: /* query_spec: SELECT selection table_exp  */
#line 657 "sql.y"
        {
            QuerySpecNode *query_spec = instance(QuerySpecNode);
            query_spec->selection = (yyvsp[-1].selection_node);
            query_spec->table_exp = (yyvsp[0].table_exp_node);
            (yyval.query_spec_node) = query_spec;
        }
#line 2779 "y.tab.c"
    break;

  case 66: /* scalar_exp_commalist: scalar_exp  */
#line 666 "sql.y"
        {
            List *scalar_exp_list = create_list(NODE_SCALAR_EXP);
            append_list(scalar_exp_list, (yyvsp[0].scalar_exp_node));
            (yyval.list) = scalar_exp_list;
        }
#line 2789 "y.tab.c"
    break;

  case 67: /* scalar_exp_commalist: scalar_exp_commalist ',' scalar_exp  */
#line 672 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].scalar_exp_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2798 "y.tab.c"
    break;

  case 68: /* scalar_exp: calculate  */
#line 679 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_CALCULATE;
            scalar_exp_node->calculate = (yyvsp[0].calculate_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2809 "y.tab.c"
    break;

  case 69: /* scalar_exp: column  */
#line 686 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_COLUMN;
            scalar_exp_node->column = (yyvsp[0].column_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2820 "y.tab.c"
    break;

  case 70: /* scalar_exp: function  */
#line 693 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_FUNCTION;
            scalar_exp_node->function = (yyvsp[0].function_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2831 "y.tab.c"
    break;

  case 71: /* scalar_exp: value_item  */
#line 700 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_VALUE;
            scalar_exp_node->value = (yyvsp[0].value_item_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2842 "y.tab.c"
    break;

  case 72: /* scalar_exp: '(' scalar_exp ')'  */
#line 707 "sql.y"
        {
            (yyval.scalar_exp_node) = (yyvsp[-1].scalar_exp_node);
        }
#line 2850 "y.tab.c"
    break;

  case 73: /* scalar_exp: scalar_exp AS IDENTIFIER  */
#line 711 "sql.y"
        {
            (yyvsp[-2].scalar_exp_node)->alias = (yyvsp[0].strVal);
            (yyval.scalar_exp_node) = (yyvsp[-2].scalar_exp_node);
        }
#line 2859 "y.tab.c"
    break;

  case 74: /* calculate: scalar_exp '+' scalar_exp  */
#line 718 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_ADD;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2871 "y.tab.c"
    break;

  case 75: /* calculate: scalar_exp '-' scalar_exp  */
#line 726 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_SUB;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2883 "y.tab.c"
    break;

  case 76: /* calculate: scalar_exp '*' scalar_exp  */
#line 734 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_MUL;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2895 "y.tab.c"
    break;

  case 77: /* calculate: scalar_exp '/' scalar_exp  */
#line 742 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_DIV;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2907 "y.tab.c"
    break;

  case 78: /* columns: column  */
#line 752 "sql.y"
        {
            List *column_set_node = create_list(NODE_COLUMN);
            append_list(column_set_node, (yyvsp[0].column_node));
            (yyval.list) = column_set_node;
        }
#line 2917 "y.tab.c"
    break;

  case 79: /* columns: columns ',' column  */
#line 758 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].column_node));
        }
#line 2926 "y.tab.c"
    break;

  case 80: /* base_table_element_commalist: base_table_element  */
#line 765 "sql.y"
        {
            List *base_table_element_commalist = create_list(NODE_BASE_TABLE_ELEMENT);
            append_list(base_table_element_commalist, (yyvsp[0].base_table_element));
            (yyval.list) = base_table_element_commalist;
        }
#line 2936 "y.tab.c"
    break;

  case 81: /* base_table_element_commalist: base_table_element_commalist ',' base_table_element  */
#line 771 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].base_table_element));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2945 "y.tab.c"
    break;

  case 82: /* base_table_element: column_def  */
#line 778 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = (yyvsp[0].column_def_node);
            node->table_contraint_def = NULL;
            node->type = TELE_COLUMN_DEF;
            (yyval.base_table_element) = node;
        }
#line 2957 "y.tab.c"
    break;

  case 83: /* base_table_element: table_contraint_def  */
#line 786 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = NULL;
            node->table_contraint_def = (yyvsp[0].table_contraint_def);
            node->type = TELE_TABLE_CONTRAINT_DEF;
            (yyval.base_table_element) = node;
        }
#line 2969 "y.tab.c"
    break;

  case 84: /* column_def: column_def_name data_type array_dim_clause column_def_opt_list  */
#line 809 "sql.y"
        {
            ColumnDefNode *column_def = instance(ColumnDefNode);
            column_def->column = (yyvsp[-3].column_def_name);
            column_def->data_type = (yyvsp[-2].data_type_node);
            column_def->array_dim = (yyvsp[-1].intVal);
            column_def->column_def_opt_list = (yyvsp[0].list);
            (yyval.column_def_node) = column_def;
        }
#line 2982 "y.tab.c"
    break;

  case 85: /* column_def_name_commalist: column_def_name  */
#line 820 "sql.y"
        {
            List *list = create_list(NODE_COLUMN_DEF_NAME);
            append_list(list, (yyvsp[0].column_def_name));
            (yyval.list) = list;
        }
#line 2992 "y.tab.c"
    break;

  case 86: /* column_def_name_commalist: column_def_name_commalist ',' column_def_name  */
#line 826 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].column_def_name));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 3001 "y.tab.c"
    break;

  case 87: /* column_def_name: IDENTIFIER  */
#line 833 "sql.y"
        {
            ColumnDefName *column_def_name = instance(ColumnDefName);
            column_def_name->column = (yyvsp[0].strVal);
            (yyval.column_def_name) = column_def_name;
        }
#line 3011 "y.tab.c"
    break;

  case 88: /* data_type: INT  */
#line 841 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_INT; 
            (yyval.data_type_node) = node;
        }
#line 3021 "y.tab.c"
    break;

  case 89: /* data_type: LONG  */
#line 847 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_LONG;  
            (yyval.data_type_node) = node;
        }
#line 3031 "y.tab.c"
    break;

  case 90: /* data_type: CHAR  */
#line 853 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_CHAR; 
            (yyval.data_type_node) = node;
        }
#line 3041 "y.tab.c"
    break;

  case 91: /* data_type: VARCHAR '(' INTVALUE ')'  */
#line 859 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_VARCHAR; 
            node->len = (yyvsp[-1].intVal);
            (yyval.data_type_node) = node;
        }
#line 3052 "y.tab.c"
    break;

  case 92: /* data_type: STRING  */
#line 866 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_STRING; 
            (yyval.data_type_node) = node;
        }
#line 3062 "y.tab.c"
    break;

  case 93: /* data_type: BOOL  */
#line 872 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_BOOL; 
            (yyval.data_type_node) = node;
        }
#line 3072 "y.tab.c"
    break;

  case 94: /* data_type: FLOAT  */
#line 878 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_FLOAT; 
            (yyval.data_type_node) = node;
        }
#line 3082 "y.tab.c"
    break;

  case 95: /* data_type: DOUBLE  */
#line 884 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DOUBLE; 
            (yyval.data_type_node) = node;
        }
#line 3092 "y.tab.c"
    break;

  case 96: /* data_type: TIMESTAMP  */
#line 890 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_TIMESTAMP; 
            (yyval.data_type_node) = node;
        }
#line 3102 "y.tab.c"
    break;

  case 97: /* data_type: DATE  */
#line 896 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DATE; 
            (yyval.data_type_node) = node;
        }
#line 3112 "y.tab.c"
    break;

  case 98: /* data_type: table  */
#line 902 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_RID;
            node->table_name = (yyvsp[0].strVal);
            (yyval.data_type_node) = node;
        }
#line 3123 "y.tab.c"
    break;

  case 99: /* array_dim_clause: %empty  */
#line 911 "sql.y"
        {
            (yyval.intVal) = 0;
        }
#line 3131 "y.tab.c"
    break;

  case 100: /* array_dim_clause: '[' ']'  */
#line 915 "sql.y"
        {
            (yyval.intVal) = 1;
        }
#line 3139 "y.tab.c"
    break;

  case 101: /* array_dim_clause: array_dim_clause '[' ']'  */
#line 919 "sql.y"
        {
            (yyval.intVal)++;
        }
#line 3147 "y.tab.c"
    break;

  case 102: /* column_def_opt_list: %empty  */
#line 925 "sql.y"
        {
            (yyval.list) = NULL;
        }
#line 3155 "y.tab.c"
    break;

  case 103: /* column_def_opt_list: column_def_opt  */
#line 929 "sql.y"
        {
            (yyval.list) = create_list(NODE_COLUMN_DEF_OPT);
            append_list((yyval.list), (yyvsp[0].column_def_opt));
        }
#line 3164 "y.tab.c"
    break;

  case 104: /* column_def_opt_list: column_def_opt_list column_def_opt  */
#line 934 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].column_def_opt));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 3173 "y.tab.c"
    break;

  case 105: /* column_def_opt: NOT NULLX  */
#line 941 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_NOT_NULL; 
            (yyval.column_def_opt) = node;
        }
#line 3183 "y.tab.c"
    break;

  case 106: /* column_def_opt: UNIQUE  */
#line 947 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_UNIQUE; 
            (yyval.column_def_opt) = node;
        }
#line 3193 "y.tab.c"
    break;

  case 107: /* column_def_opt: PRIMARY KEY  */
#line 953 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_PRIMARY_KEY; 
            (yyval.column_def_opt) = node;
        }
#line 3203 "y.tab.c"
    break;

  case 108: /* column_def_opt: DEFAULT value_item  */
#line 959 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_VALUE;
            node->value = (yyvsp[0].value_item_node);
            (yyval.column_def_opt) = node;
        }
#line 3214 "y.tab.c"
    break;

  case 109: /* column_def_opt: DEFAULT NULLX  */
#line 966 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_NULL;
            (yyval.column_def_opt) = node;
        }
#line 3224 "y.tab.c"
    break;

  case 110: /* column_def_opt: COMMENT STRINGVALUE  */
#line 972 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_COMMENT;
            node->comment = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3235 "y.tab.c"
    break;

  case 111: /* column_def_opt: CHECK '(' search_condition ')'  */
#line 979 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_CHECK_CONDITION;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.column_def_opt) = node;
        }
#line 3246 "y.tab.c"
    break;

  case 112: /* column_def_opt: REFERENCES table  */
#line 986 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_REFERENECS;
            node->refer_table = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3257 "y.tab.c"
    break;

  case 113: /* table_contraint_def: UNIQUE '(' column_def_name_commalist ')'  */
#line 995 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_UNIQUE;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3268 "y.tab.c"
    break;

  case 114: /* table_contraint_def: PRIMARY KEY '(' column_def_name_commalist ')'  */
#line 1002 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_PRIMARY_KEY;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3279 "y.tab.c"
    break;

  case 115: /* table_contraint_def: FOREIGN KEY '(' column_def_name_commalist ')' REFERENCES table  */
#line 1009 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_FOREIGN_KEY;
            node->column_commalist = (yyvsp[-3].list);
            node->table = (yyvsp[0].strVal);
            (yyval.table_contraint_def) = node;
        }
#line 3291 "y.tab.c"
    break;

  case 116: /* table_contraint_def: CHECK '(' search_condition ')'  */
#line 1017 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_CHECK;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.table_contraint_def) = node;
        }
#line 3302 "y.tab.c"
    break;

  case 117: /* column: IDENTIFIER  */
#line 1026 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[0].strVal);
            column_node->has_sub_column = false;
            (yyval.column_node) = column_node;
        }
#line 3313 "y.tab.c"
    break;

  case 118: /* column: '(' IDENTIFIER ')' '.' column  */
#line 1033 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->sub_column = (yyvsp[0].column_node);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3325 "y.tab.c"
    break;

  case 119: /* column: IDENTIFIER '{' scalar_exp_commalist '}'  */
#line 1041 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->scalar_exp_list = (yyvsp[-1].list);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3337 "y.tab.c"
    break;

  case 120: /* column: IDENTIFIER '.' column  */
#line 1049 "sql.y"
        {
            (yyval.column_node) = (yyvsp[0].column_node);
            (yyval.column_node)->range_variable = (yyvsp[-2].strVal);
        }
#line 3346 "y.tab.c"
    break;

  case 121: /* value_items: value_item  */
#line 1056 "sql.y"
        {
            List *value_list = create_list(NODE_VALUE_ITEM);
            append_list(value_list, (yyvsp[0].value_item_node));
            (yyval.list) = value_list;
        }
#line 3356 "y.tab.c"
    break;

  case 122: /* value_items: value_items ',' value_item  */
#line 1062 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].value_item_node));
        }
#line 3365 "y.tab.c"
    break;

  case 123: /* value_item: atom  */
#line 1069 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ATOM;
            node->value.atom = (yyvsp[0].atom_node);
            (yyval.value_item_node) = node;
        }
#line 3376 "y.tab.c"
    break;

  case 124: /* value_item: NULLX  */
#line 1076 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_NULL;
            (yyval.value_item_node) = node;
        }
#line 3386 "y.tab.c"
    break;

  case 125: /* value_item: '[' value_items ']'  */
#line 1082 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ARRAY;
            node->value.value_list = (yyvsp[-1].list);
            (yyval.value_item_node) = node;
        }
#line 3397 "y.tab.c"
    break;

  case 126: /* atom: INTVALUE  */
#line 1091 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.intval = (yyvsp[0].intVal);
            node->type = A_INT;
            (yyval.atom_node) = node;
        }
#line 3408 "y.tab.c"
    break;

  case 127: /* atom: BOOLVALUE  */
#line 1098 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.boolval = (yyvsp[0].boolVal);
            node->type = A_BOOL;
            (yyval.atom_node) = node;
        }
#line 3419 "y.tab.c"
    break;

  case 128: /* atom: STRINGVALUE  */
#line 1105 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.strval = (yyvsp[0].strVal);
            node->type = A_STRING;
            (yyval.atom_node) = node;
        }
#line 3430 "y.tab.c"
    break;

  case 129: /* atom: FLOATVALUE  */
#line 1112 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.floatval = (yyvsp[0].floatVal);
            node->type = A_FLOAT;
            (yyval.atom_node) = node;
        }
#line 3441 "y.tab.c"
    break;

  case 130: /* atom: REFERVALUE  */
#line 1119 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.referval = (yyvsp[0].referVal);
            node->type = A_REFERENCE;
            (yyval.atom_node) = node;
        }
#line 3452 "y.tab.c"
    break;

  case 131: /* REFERVALUE: '(' value_items ')'  */
#line 1129 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = DIRECTLY;
            refer->nest_value_list = (yyvsp[-1].list);
            (yyval.referVal) = refer;
        }
#line 3463 "y.tab.c"
    break;

  case 132: /* REFERVALUE: REF '(' search_condition ')'  */
#line 1137 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = INDIRECTLY;
            refer->condition = (yyvsp[-1].search_condition_node);
            (yyval.referVal) = refer;
        }
#line 3474 "y.tab.c"
    break;

  case 133: /* BOOLVALUE: TRUE  */
#line 1146 "sql.y"
        {
            (yyval.boolVal) = true;
        }
#line 3482 "y.tab.c"
    break;

  case 134: /* BOOLVALUE: FALSE  */
#line 1150 "sql.y"
        {
            (yyval.boolVal) = false;
        }
#line 3490 "y.tab.c"
    break;

  case 135: /* assignments: assignment  */
#line 1156 "sql.y"
        {
            List *list = create_list(NODE_ASSIGNMENT);
            append_list(list, (yyvsp[0].assignment_node));
            (yyval.list) = list;
        }
#line 3500 "y.tab.c"
    break;

  case 136: /* assignments: assignments ',' assignment  */
#line 1162 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].assignment_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 3509 "y.tab.c"
    break;

  case 137: /* assignment: column EQ value_item  */
#line 1169 "sql.y"
        {
            AssignmentNode *node = instance(AssignmentNode);
            node->column = (yyvsp[-2].column_node);
            node->value = (yyvsp[0].value_item_node);
            (yyval.assignment_node) = node;
        }
#line 3520 "y.tab.c"
    break;

  case 138: /* search_condition: boolean_term  */
#line 1178 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3530 "y.tab.c"
    break;

  case 139: /* search_condition: search_condition OR boolean_term  */
#line 1184 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->or_search_condition = (yyvsp[-2].search_condition_node);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3541 "y.tab.c"
    break;

  case 140: /* boolean_term: boolean_factor  */
#line 1193 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3551 "y.tab.c"
    break;

  case 141: /* boolean_term: boolean_term AND boolean_factor  */
#line 1199 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->and_boolean_term = (yyvsp[-2].boolean_term_node);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3562 "y.tab.c"
    break;

  case 142: /* boolean_factor: boolean_test  */
#line 1208 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = false;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3573 "y.tab.c"
    break;

  case 143: /* boolean_factor: NOT boolean_test  */
#line 1215 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = true;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3584 "y.tab.c"
    break;

  case 144: /* boolean_test: boolean_primary  */
#line 1224 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[0].boolean_primary_node);
            test_node->type = NONE_TRUE_VALUE;
            (yyval.boolean_test_node) = test_node;
        }
#line 3595 "y.tab.c"
    break;

  case 145: /* boolean_test: boolean_primary IS BOOLVALUE  */
#line 1231 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-2].boolean_primary_node);
            test_node->type = IS_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[0].boolVal);
            (yyval.boolean_test_node) = test_node;
        }
#line 3607 "y.tab.c"
    break;

  case 146: /* boolean_test: boolean_primary IS NOT BOOLVALUE  */
#line 1239 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-3].boolean_primary_node);
            test_node->type = IS_NOT_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[-1].keyword);
            (yyval.boolean_test_node) = test_node;
        }
#line 3619 "y.tab.c"
    break;

  case 147: /* boolean_primary: predicate  */
#line 1249 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = PREDICATE_BOOLEAN_PRIMAYR;
            primary_node->predicate = (yyvsp[0].predicate_node);
            primary_node->search_condition = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3631 "y.tab.c"
    break;

  case 148: /* boolean_primary: '(' search_condition ')'  */
#line 1257 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = SEARCH_CONDITION_BOOLEAN_PRIMAYR;
            primary_node->search_condition = (yyvsp[-1].search_condition_node);
            primary_node->predicate = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3643 "y.tab.c"
    break;

  case 149: /* predicate: comparison_predicate  */
#line 1266 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_COMPARISON;
            predicate->comparison = (yyvsp[0].comparison_node);
            (yyval.predicate_node) = predicate;
        }
#line 3654 "y.tab.c"
    break;

  case 150: /* predicate: like_predicate  */
#line 1273 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_LIKE;
            predicate->like = (yyvsp[0].like_node);
            (yyval.predicate_node) = predicate;
        }
#line 3665 "y.tab.c"
    break;

  case 151: /* predicate: in_predicate  */
#line 1280 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_IN;
            predicate->in = (yyvsp[0].in_node);
            (yyval.predicate_node) = predicate;
        }
#line 3676 "y.tab.c"
    break;

  case 152: /* comparison_predicate: scalar_exp compare scalar_exp  */
#line 1289 "sql.y"
        {
            ComparisonNode *comparison_node = instance(ComparisonNode);
            comparison_node->left = (yyvsp[-2].scalar_exp_node);
            comparison_node->type = (yyvsp[-1].compare_type);
            comparison_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.comparison_node) = comparison_node;
        }
#line 3688 "y.tab.c"
    break;

  case 153: /* like_predicate: column LIKE value_item  */
#line 1299 "sql.y"
        {
            LikeNode *like_node = instance(LikeNode);
            like_node->column = (yyvsp[-2].column_node);
            like_node->value = (yyvsp[0].value_item_node);
            (yyval.like_node) = like_node;
        }
#line 3699 "y.tab.c"
    break;

  case 154: /* in_predicate: column IN '(' value_items ')'  */
#line 1308 "sql.y"
        {
            InNode *in_node = instance(InNode);
            in_node->column = (yyvsp[-4].column_node);
            in_node->value_list = (yyvsp[-1].list);
            (yyval.in_node) = in_node;
        }
#line 3710 "y.tab.c"
    break;

  case 155: /* limit_clause: %empty  */
#line 1317 "sql.y"
        {
            (yyval.limit_clause_node) = NULL;
        }
#line 3718 "y.tab.c"
    break;

  case 156: /* limit_clause: LIMIT INTVALUE  */
#line 1321 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = 0;
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3729 "y.tab.c"
    break;

  case 157: /* limit_clause: LIMIT INTVALUE ',' INTVALUE  */
#line 1328 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = (yyvsp[-2].intVal);
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3740 "y.tab.c"
    break;

  case 158: /* limit_clause: LIMIT INTVALUE OFFSET INTVALUE  */
#line 1335 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->rows = (yyvsp[-2].intVal);
            node->offset = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3751 "y.tab.c"
    break;

  case 159: /* compare: EQ  */
#line 1343 "sql.y"
            { (yyval.compare_type) = O_EQ; }
#line 3757 "y.tab.c"
    break;

  case 160: /* compare: NE  */
#line 1344 "sql.y"
            { (yyval.compare_type) = O_NE; }
#line 3763 "y.tab.c"
    break;

  case 161: /* compare: GT  */
#line 1345 "sql.y"
            { (yyval.compare_type) = O_GT; }
#line 3769 "y.tab.c"
    break;

  case 162: /* compare: GE  */
#line 1346 "sql.y"
            { (yyval.compare_type) = O_GE; }
#line 3775 "y.tab.c"
    break;

  case 163: /* compare: LT  */
#line 1347 "sql.y"
            { (yyval.compare_type) = O_LT; }
#line 3781 "y.tab.c"
    break;

  case 164: /* compare: LE  */
#line 1348 "sql.y"
            { (yyval.compare_type) = O_LE; }
#line 3787 "y.tab.c"
    break;

  case 165: /* function: MAX '(' non_all_function_value ')'  */
#line 1352 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MAX;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3798 "y.tab.c"
    break;

  case 166: /* function: MIN '(' non_all_function_value ')'  */
#line 1359 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MIN;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3809 "y.tab.c"
    break;

  case 167: /* function: COUNT '(' function_value ')'  */
#line 1366 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_COUNT;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3820 "y.tab.c"
    break;

  case 168: /* function: SUM '(' function_value ')'  */
#line 1373 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_SUM;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3831 "y.tab.c"
    break;

  case 169: /* function: AVG '(' function_value ')'  */
#line 1380 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_AVG;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3842 "y.tab.c"
    break;

  case 170: /* function_value: INTVALUE  */
#line 1389 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3853 "y.tab.c"
    break;

  case 171: /* function_value: column  */
#line 1396 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3864 "y.tab.c"
    break;

  case 172: /* function_value: '*'  */
#line 1403 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->value_type = V_ALL;
            (yyval.function_value_node) = node;
        }
#line 3874 "y.tab.c"
    break;

  case 173: /* non_all_function_value: INTVALUE  */
#line 1411 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3885 "y.tab.c"
    break;

  case 174: /* non_all_function_value: column  */
#line 1418 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3896 "y.tab.c"
    break;


#line 3900 "y.tab.c"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken, &yylloc};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (states, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc, states);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp, states);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  ++yylsp;
  YYLLOC_DEFAULT (*yylsp, yyerror_range, 2);

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (states, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, states);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp, states);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 1429 "sql.y"


int yyerror(List *states, const char *s) {
    if (current_token != NULL) {
	    db_log(ERROR, "%s at or near [%s].", s, current_token);
        free(current_token);
        current_token = NULL;
    }
    else
	    db_log(ERROR, "%s.", s);
    return 0;
}
