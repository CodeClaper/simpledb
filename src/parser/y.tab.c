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
    IS = 317,                      /* IS  */
    NOT = 318,                     /* NOT  */
    ALTER = 319,                   /* ALTER  */
    COLUMN = 320,                  /* COLUMN  */
    ADD = 321,                     /* ADD  */
    RENAME = 322,                  /* RENAME  */
    BEFORE = 323,                  /* BEFORE  */
    AFTER = 324,                   /* AFTER  */
    SYSTEM = 325,                  /* SYSTEM  */
    CONFIG = 326,                  /* CONFIG  */
    MEMORY = 327,                  /* MEMORY  */
    IDENTIFIER = 328,              /* IDENTIFIER  */
    INTVALUE = 329,                /* INTVALUE  */
    FLOATVALUE = 330,              /* FLOATVALUE  */
    STRINGVALUE = 331              /* STRINGVALUE  */
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
#define IS 317
#define NOT 318
#define ALTER 319
#define COLUMN 320
#define ADD 321
#define RENAME 322
#define BEFORE 323
#define AFTER 324
#define SYSTEM 325
#define CONFIG 326
#define MEMORY 327
#define IDENTIFIER 328
#define INTVALUE 329
#define FLOATVALUE 330
#define STRINGVALUE 331

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

#line 352 "y.tab.c"

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
  YYSYMBOL_FROM = 24,                      /* FROM  */
  YYSYMBOL_WHERE = 25,                     /* WHERE  */
  YYSYMBOL_INTO = 26,                      /* INTO  */
  YYSYMBOL_SET = 27,                       /* SET  */
  YYSYMBOL_VALUES = 28,                    /* VALUES  */
  YYSYMBOL_TABLE = 29,                     /* TABLE  */
  YYSYMBOL_LIMIT = 30,                     /* LIMIT  */
  YYSYMBOL_OFFSET = 31,                    /* OFFSET  */
  YYSYMBOL_SHOW = 32,                      /* SHOW  */
  YYSYMBOL_TABLES = 33,                    /* TABLES  */
  YYSYMBOL_PRIMARY = 34,                   /* PRIMARY  */
  YYSYMBOL_KEY = 35,                       /* KEY  */
  YYSYMBOL_UNIQUE = 36,                    /* UNIQUE  */
  YYSYMBOL_DEFAULT = 37,                   /* DEFAULT  */
  YYSYMBOL_CHECK = 38,                     /* CHECK  */
  YYSYMBOL_REFERENCES = 39,                /* REFERENCES  */
  YYSYMBOL_FOREIGN = 40,                   /* FOREIGN  */
  YYSYMBOL_MAX = 41,                       /* MAX  */
  YYSYMBOL_MIN = 42,                       /* MIN  */
  YYSYMBOL_COUNT = 43,                     /* COUNT  */
  YYSYMBOL_SUM = 44,                       /* SUM  */
  YYSYMBOL_AVG = 45,                       /* AVG  */
  YYSYMBOL_REF = 46,                       /* REF  */
  YYSYMBOL_TRUE = 47,                      /* TRUE  */
  YYSYMBOL_FALSE = 48,                     /* FALSE  */
  YYSYMBOL_NULLX = 49,                     /* NULLX  */
  YYSYMBOL_AS = 50,                        /* AS  */
  YYSYMBOL_COMMENT = 51,                   /* COMMENT  */
  YYSYMBOL_CHAR = 52,                      /* CHAR  */
  YYSYMBOL_INT = 53,                       /* INT  */
  YYSYMBOL_LONG = 54,                      /* LONG  */
  YYSYMBOL_VARCHAR = 55,                   /* VARCHAR  */
  YYSYMBOL_STRING = 56,                    /* STRING  */
  YYSYMBOL_BOOL = 57,                      /* BOOL  */
  YYSYMBOL_FLOAT = 58,                     /* FLOAT  */
  YYSYMBOL_DOUBLE = 59,                    /* DOUBLE  */
  YYSYMBOL_DATE = 60,                      /* DATE  */
  YYSYMBOL_TIMESTAMP = 61,                 /* TIMESTAMP  */
  YYSYMBOL_EQ = 62,                        /* EQ  */
  YYSYMBOL_NE = 63,                        /* NE  */
  YYSYMBOL_GT = 64,                        /* GT  */
  YYSYMBOL_GE = 65,                        /* GE  */
  YYSYMBOL_LT = 66,                        /* LT  */
  YYSYMBOL_LE = 67,                        /* LE  */
  YYSYMBOL_IN = 68,                        /* IN  */
  YYSYMBOL_LIKE = 69,                      /* LIKE  */
  YYSYMBOL_IS = 70,                        /* IS  */
  YYSYMBOL_NOT = 71,                       /* NOT  */
  YYSYMBOL_ALTER = 72,                     /* ALTER  */
  YYSYMBOL_COLUMN = 73,                    /* COLUMN  */
  YYSYMBOL_ADD = 74,                       /* ADD  */
  YYSYMBOL_RENAME = 75,                    /* RENAME  */
  YYSYMBOL_BEFORE = 76,                    /* BEFORE  */
  YYSYMBOL_AFTER = 77,                     /* AFTER  */
  YYSYMBOL_SYSTEM = 78,                    /* SYSTEM  */
  YYSYMBOL_CONFIG = 79,                    /* CONFIG  */
  YYSYMBOL_MEMORY = 80,                    /* MEMORY  */
  YYSYMBOL_IDENTIFIER = 81,                /* IDENTIFIER  */
  YYSYMBOL_INTVALUE = 82,                  /* INTVALUE  */
  YYSYMBOL_FLOATVALUE = 83,                /* FLOATVALUE  */
  YYSYMBOL_STRINGVALUE = 84,               /* STRINGVALUE  */
  YYSYMBOL_85_ = 85,                       /* ','  */
  YYSYMBOL_86_ = 86,                       /* '.'  */
  YYSYMBOL_87_ = 87,                       /* '{'  */
  YYSYMBOL_88_ = 88,                       /* '}'  */
  YYSYMBOL_89_ = 89,                       /* ';'  */
  YYSYMBOL_YYACCEPT = 90,                  /* $accept  */
  YYSYMBOL_statements = 91,                /* statements  */
  YYSYMBOL_statement = 92,                 /* statement  */
  YYSYMBOL_begin_transaction_statement = 93, /* begin_transaction_statement  */
  YYSYMBOL_commit_transaction_statement = 94, /* commit_transaction_statement  */
  YYSYMBOL_rollback_transaction_statement = 95, /* rollback_transaction_statement  */
  YYSYMBOL_create_table_statement = 96,    /* create_table_statement  */
  YYSYMBOL_drop_table_statement = 97,      /* drop_table_statement  */
  YYSYMBOL_select_statement = 98,          /* select_statement  */
  YYSYMBOL_insert_statement = 99,          /* insert_statement  */
  YYSYMBOL_update_statement = 100,         /* update_statement  */
  YYSYMBOL_delete_statement = 101,         /* delete_statement  */
  YYSYMBOL_describe_statement = 102,       /* describe_statement  */
  YYSYMBOL_show_statement = 103,           /* show_statement  */
  YYSYMBOL_alter_table_statement = 104,    /* alter_table_statement  */
  YYSYMBOL_alter_table_action = 105,       /* alter_table_action  */
  YYSYMBOL_add_column_def = 106,           /* add_column_def  */
  YYSYMBOL_drop_column_def = 107,          /* drop_column_def  */
  YYSYMBOL_column_position_def = 108,      /* column_position_def  */
  YYSYMBOL_selection = 109,                /* selection  */
  YYSYMBOL_table_exp = 110,                /* table_exp  */
  YYSYMBOL_from_clause = 111,              /* from_clause  */
  YYSYMBOL_table_ref_commalist = 112,      /* table_ref_commalist  */
  YYSYMBOL_table_ref = 113,                /* table_ref  */
  YYSYMBOL_table = 114,                    /* table  */
  YYSYMBOL_range_variable = 115,           /* range_variable  */
  YYSYMBOL_opt_where_clause = 116,         /* opt_where_clause  */
  YYSYMBOL_where_clause = 117,             /* where_clause  */
  YYSYMBOL_values_or_query_spec = 118,     /* values_or_query_spec  */
  YYSYMBOL_opt_values = 119,               /* opt_values  */
  YYSYMBOL_query_spec = 120,               /* query_spec  */
  YYSYMBOL_scalar_exp_commalist = 121,     /* scalar_exp_commalist  */
  YYSYMBOL_scalar_exp = 122,               /* scalar_exp  */
  YYSYMBOL_calculate = 123,                /* calculate  */
  YYSYMBOL_columns = 124,                  /* columns  */
  YYSYMBOL_base_table_element_commalist = 125, /* base_table_element_commalist  */
  YYSYMBOL_base_table_element = 126,       /* base_table_element  */
  YYSYMBOL_column_def = 127,               /* column_def  */
  YYSYMBOL_column_def_name_commalist = 128, /* column_def_name_commalist  */
  YYSYMBOL_column_def_name = 129,          /* column_def_name  */
  YYSYMBOL_data_type = 130,                /* data_type  */
  YYSYMBOL_array_dim_clause = 131,         /* array_dim_clause  */
  YYSYMBOL_column_def_opt_list = 132,      /* column_def_opt_list  */
  YYSYMBOL_column_def_opt = 133,           /* column_def_opt  */
  YYSYMBOL_table_contraint_def = 134,      /* table_contraint_def  */
  YYSYMBOL_column = 135,                   /* column  */
  YYSYMBOL_value_items = 136,              /* value_items  */
  YYSYMBOL_value_item = 137,               /* value_item  */
  YYSYMBOL_atom = 138,                     /* atom  */
  YYSYMBOL_REFERVALUE = 139,               /* REFERVALUE  */
  YYSYMBOL_BOOLVALUE = 140,                /* BOOLVALUE  */
  YYSYMBOL_assignments = 141,              /* assignments  */
  YYSYMBOL_assignment = 142,               /* assignment  */
  YYSYMBOL_search_condition = 143,         /* search_condition  */
  YYSYMBOL_boolean_term = 144,             /* boolean_term  */
  YYSYMBOL_boolean_factor = 145,           /* boolean_factor  */
  YYSYMBOL_boolean_test = 146,             /* boolean_test  */
  YYSYMBOL_boolean_primary = 147,          /* boolean_primary  */
  YYSYMBOL_predicate = 148,                /* predicate  */
  YYSYMBOL_comparison_predicate = 149,     /* comparison_predicate  */
  YYSYMBOL_like_predicate = 150,           /* like_predicate  */
  YYSYMBOL_in_predicate = 151,             /* in_predicate  */
  YYSYMBOL_limit_clause = 152,             /* limit_clause  */
  YYSYMBOL_compare = 153,                  /* compare  */
  YYSYMBOL_function = 154,                 /* function  */
  YYSYMBOL_function_value = 155,           /* function_value  */
  YYSYMBOL_non_all_function_value = 156,   /* non_all_function_value  */
  YYSYMBOL_end = 157                       /* end  */
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
#define YYFINAL  66
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   441

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  90
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  68
/* YYNRULES -- Number of rules.  */
#define YYNRULES  167
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  321

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   331


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
       9,    10,     7,     5,    85,     6,    86,     8,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    89,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    11,     2,    12,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    87,     2,    88,     2,     2,     2,     2,
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
      83,    84
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   185,   185,   190,   197,   203,   209,   215,   222,   229,
     236,   243,   250,   257,   264,   271,   280,   283,   286,   290,
     300,   309,   319,   327,   339,   350,   356,   366,   375,   384,
     392,   399,   408,   417,   426,   429,   436,   445,   452,   460,
     471,   474,   482,   488,   495,   501,   508,   517,   523,   530,
     533,   539,   547,   554,   563,   568,   575,   584,   590,   597,
     604,   611,   618,   625,   629,   636,   644,   652,   660,   670,
     676,   683,   689,   696,   704,   727,   738,   744,   751,   759,
     765,   771,   777,   784,   790,   796,   802,   808,   814,   820,
     830,   833,   837,   844,   847,   852,   859,   865,   871,   877,
     884,   890,   897,   904,   913,   920,   927,   935,   944,   951,
     959,   967,   974,   980,   987,   994,  1000,  1009,  1016,  1023,
    1030,  1037,  1047,  1055,  1064,  1068,  1074,  1080,  1087,  1096,
    1102,  1111,  1117,  1126,  1133,  1142,  1149,  1157,  1167,  1175,
    1184,  1191,  1198,  1207,  1217,  1226,  1236,  1239,  1246,  1253,
    1262,  1263,  1264,  1265,  1266,  1267,  1270,  1277,  1284,  1291,
    1298,  1307,  1314,  1321,  1329,  1336,  1345,  1346
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
  "DESCRIBE", "FROM", "WHERE", "INTO", "SET", "VALUES", "TABLE", "LIMIT",
  "OFFSET", "SHOW", "TABLES", "PRIMARY", "KEY", "UNIQUE", "DEFAULT",
  "CHECK", "REFERENCES", "FOREIGN", "MAX", "MIN", "COUNT", "SUM", "AVG",
  "REF", "TRUE", "FALSE", "NULLX", "AS", "COMMENT", "CHAR", "INT", "LONG",
  "VARCHAR", "STRING", "BOOL", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP",
  "EQ", "NE", "GT", "GE", "LT", "LE", "IN", "LIKE", "IS", "NOT", "ALTER",
  "COLUMN", "ADD", "RENAME", "BEFORE", "AFTER", "SYSTEM", "CONFIG",
  "MEMORY", "IDENTIFIER", "INTVALUE", "FLOATVALUE", "STRINGVALUE", "','",
  "'.'", "'{'", "'}'", "';'", "$accept", "statements", "statement",
  "begin_transaction_statement", "commit_transaction_statement",
  "rollback_transaction_statement", "create_table_statement",
  "drop_table_statement", "select_statement", "insert_statement",
  "update_statement", "delete_statement", "describe_statement",
  "show_statement", "alter_table_statement", "alter_table_action",
  "add_column_def", "drop_column_def", "column_position_def", "selection",
  "table_exp", "from_clause", "table_ref_commalist", "table_ref", "table",
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

#define YYPACT_NINF (-180)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-113)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     331,   -79,   -79,   -79,    -9,    -7,    63,    12,   -37,    51,
     -37,    47,    61,   306,  -180,  -180,  -180,  -180,  -180,  -180,
    -180,  -180,  -180,  -180,  -180,  -180,  -180,   105,  -180,  -180,
    -180,   -37,   -37,  -180,   188,   248,   129,   147,   172,   185,
     197,   203,  -180,  -180,  -180,    14,  -180,  -180,  -180,   193,
     136,    53,  -180,  -180,  -180,  -180,  -180,  -180,  -180,   -37,
    -180,   196,   -37,   -79,   -79,   -37,  -180,  -180,  -180,   229,
     -79,    -5,   369,    -2,   155,   248,    -1,  -180,    67,    67,
       5,     5,     5,    88,    41,   207,   -37,   -79,   214,   207,
     207,   207,   207,   207,   165,   133,    41,   -10,  -180,  -180,
      15,   162,  -180,   159,  -180,  -180,   248,  -180,   179,  -180,
    -180,   255,   256,  -180,  -180,  -180,   257,   258,   274,   144,
     233,   305,   -16,    52,   259,  -180,  -180,   215,  -180,  -180,
    -180,  -180,  -180,    80,   201,  -180,   -13,  -180,    88,   262,
    -180,    53,    27,    27,   237,   237,  -180,    41,    63,   284,
     -79,  -180,   236,    31,  -180,    88,  -180,   230,   231,   -79,
    -180,  -180,   270,   298,   299,   283,  -180,     7,  -180,  -180,
     352,  -180,    41,  -180,   309,  -180,  -180,  -180,  -180,  -180,
     334,   140,  -180,  -180,  -180,  -180,  -180,  -180,  -180,   207,
     325,   248,    88,  -180,    88,   130,  -180,   -37,   276,  -180,
    -180,   340,   280,  -180,     8,  -180,   193,   248,   279,  -180,
     248,    41,   -79,     6,   285,   300,  -180,   356,   300,    88,
     371,   -79,   162,  -180,  -180,  -180,   373,  -180,  -180,  -180,
    -180,  -180,  -180,  -180,   372,  -180,  -180,    53,   248,  -180,
     259,  -180,   128,  -180,  -180,  -180,   -12,    17,    41,  -180,
      11,   377,  -180,  -180,  -180,  -180,  -180,   132,   300,    29,
    -180,   154,   300,  -180,  -180,   310,   379,   322,    30,  -180,
     312,   313,   -79,  -180,  -180,   248,   333,   335,  -180,    32,
    -180,   300,  -180,    36,   405,  -180,   406,   382,  -180,   253,
     411,   -37,   337,   374,   351,  -180,  -180,  -180,  -180,  -180,
      38,  -180,  -180,  -180,  -180,   385,  -180,  -180,  -180,  -180,
    -180,    88,  -180,  -180,  -180,  -180,  -180,   -37,   164,  -180,
    -180
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     2,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,   166,    16,    17,
      18,     0,     0,    38,     0,     0,     0,     0,     0,     0,
       0,     0,   124,   125,   115,   108,   117,   120,   119,    40,
      37,    57,    59,    60,    62,   114,   121,   118,    61,     0,
      47,     0,     0,     0,     0,     0,     1,     3,   167,     0,
       0,   108,     0,     0,    62,     0,     0,   112,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    49,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    27,    28,
       0,     0,    20,     0,    63,   122,     0,   116,     0,   164,
     165,     0,     0,   163,   161,   162,     0,     0,     0,     0,
       0,     0,    60,     0,   129,   131,   133,   135,   138,   140,
     141,   142,   111,     0,    41,    42,    44,    21,     0,   146,
      50,    58,    65,    66,    67,    68,    64,     0,     0,     0,
       0,    53,     0,    49,   126,     0,    25,     0,     0,     0,
      30,    31,     0,     0,     0,     0,    78,     0,    71,    73,
       0,    74,     0,   113,     0,   156,   157,   158,   159,   160,
       0,     0,   134,   150,   151,   152,   153,   154,   155,     0,
       0,     0,     0,   123,     0,     0,   110,     0,     0,    48,
      45,    51,     0,    39,     0,    69,    40,     0,    52,    22,
       0,     0,     0,     0,     0,     0,    29,     0,     0,     0,
       0,     0,     0,    81,    79,    80,     0,    83,    84,    85,
      86,    88,    87,    89,    90,   109,   139,   143,     0,   144,
     130,   132,     0,   136,    43,    46,   147,     0,     0,    56,
       0,     0,   128,   127,    24,    26,    33,    34,     0,     0,
      76,     0,     0,    19,    72,     0,     0,    93,     0,   137,
       0,     0,     0,    70,    54,     0,     0,     0,    32,     0,
     104,     0,   107,     0,     0,    91,     0,     0,    97,     0,
       0,     0,     0,     0,    75,    94,   145,   149,   148,    23,
       0,    35,    36,   105,    77,     0,    82,    92,    98,   100,
      99,     0,   103,   101,    96,    95,    55,     0,     0,   106,
     102
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -180,  -180,   412,  -180,  -180,  -180,  -180,  -180,  -180,  -180,
    -180,  -180,  -180,  -180,  -180,  -180,  -180,  -180,  -180,   278,
     221,  -180,  -180,   232,    -8,   234,   275,  -180,   183,  -180,
    -180,   346,    35,  -180,  -180,  -180,   212,   220,   -51,   -78,
    -180,  -180,  -180,   142,  -180,   -53,   -34,   -28,  -180,  -180,
    -179,  -180,   226,  -106,   246,   245,   320,  -180,  -180,  -180,
    -180,  -180,  -180,  -180,  -180,   138,   362,     1
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,   159,   160,   161,   278,    49,
      87,    88,   134,   135,   136,   200,   139,   140,   150,   208,
     151,    50,   121,    52,   204,   167,   168,   169,   259,   170,
     234,   267,   294,   295,   171,    53,    73,    54,    55,    56,
      57,   153,   154,   123,   124,   125,   126,   127,   128,   129,
     130,   131,   203,   189,    58,   116,   111,    28
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      61,    76,    63,    29,    30,   103,    74,    77,   105,   192,
      27,   107,   113,   181,   108,   155,   243,   221,   247,   270,
      31,   274,    32,    69,    70,   110,   110,   115,   115,   115,
     122,   132,   201,   157,    92,    93,   148,   198,    59,   280,
     296,    51,   303,   152,    60,   149,   305,    77,   316,   213,
     108,    95,   190,   191,    97,   192,   138,   100,    90,    91,
      92,    93,   193,   269,    98,    99,   122,   122,   199,    72,
      33,   102,    34,   271,    35,    62,   108,    94,   173,    27,
      64,    84,    85,   106,   106,   122,    45,   114,   137,   158,
      65,    74,   222,   248,   205,    27,   106,   119,   156,    35,
      84,    85,   122,    94,    36,    37,    38,    39,    40,    41,
      42,    43,    44,   261,   281,   106,   211,   281,    68,   235,
      51,   281,    45,   106,   141,   142,   143,   144,   145,    36,
      37,    38,    39,    40,    41,    42,    43,    44,    78,   122,
     260,   122,   147,   192,    45,    46,    47,    48,    45,   109,
     236,   209,   148,   119,   180,    35,    79,   192,   152,   120,
     216,   149,   233,   239,   282,    89,   122,   192,   196,    45,
      46,    47,    48,   250,   320,    42,    43,    42,    43,    77,
     260,    80,   252,    51,   260,    36,    37,    38,    39,    40,
      41,    42,    43,    44,    81,   273,   162,    34,   163,    35,
     164,   242,   165,   304,   268,   318,    82,   279,   276,   277,
      77,   283,    83,   254,   255,   120,    34,    86,    35,   117,
     118,    89,   263,    96,   237,    71,    46,    47,    48,    36,
      37,    38,    39,    40,    41,    42,    43,    44,   101,   138,
    -112,   300,   119,   166,    35,   172,   146,    77,    36,    37,
      38,    39,    40,    41,    42,    43,    44,    75,   122,    35,
     174,   310,    75,   194,    35,   175,   176,   177,   178,    71,
      46,    47,    48,   299,    36,    37,    38,    39,    40,    41,
      42,    43,    44,   312,   179,   195,   197,    94,    45,    46,
      47,    48,   202,   207,    41,    42,    43,    44,   210,    41,
      42,    43,   309,   214,   215,   217,    66,   218,   219,   319,
      90,    91,    92,    93,    45,    46,    47,    48,   220,   103,
       1,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      46,    47,    48,   286,   238,    46,    47,    48,    11,    90,
      91,    92,    93,   192,   104,     1,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    94,   287,   199,   288,   289,
     290,   291,   246,    11,   251,   258,   256,   183,   184,   185,
     186,   187,   188,   292,    90,    91,    92,    93,    12,   104,
     262,   166,   265,   266,    94,   287,   275,   288,   289,   290,
     291,   285,   284,   293,   297,   298,   183,   184,   185,   186,
     187,   188,   292,    12,   223,   224,   225,   226,   227,   228,
     229,   230,   231,   232,   301,   306,   302,   308,   307,    94,
     311,   313,   293,   314,   317,    67,   206,   249,   212,   244,
     272,   133,   245,    60,   264,   257,   315,   253,   240,   241,
     182,   112
};

static const yytype_int16 yycheck[] =
{
       8,    35,    10,     2,     3,    10,    34,    35,    10,     3,
      89,    12,     7,   119,     9,    25,   195,    10,    10,    31,
      29,    10,    29,    31,    32,    78,    79,    80,    81,    82,
      83,    84,   138,    18,     7,     8,    19,    50,    26,    10,
      10,     6,    10,    96,    81,    28,    10,    75,    10,   155,
       9,    59,    68,    69,    62,     3,    25,    65,     5,     6,
       7,     8,    10,   242,    63,    64,   119,   120,    81,    34,
       7,    70,     9,    85,    11,    24,     9,    50,   106,    89,
      33,    86,    87,    85,    85,   138,    81,    82,    87,    74,
      29,   119,    85,    85,   147,    89,    85,     9,    97,    11,
      86,    87,   155,    50,    41,    42,    43,    44,    45,    46,
      47,    48,    49,   219,    85,    85,    85,    85,    13,   172,
      85,    85,    81,    85,    89,    90,    91,    92,    93,    41,
      42,    43,    44,    45,    46,    47,    48,    49,     9,   192,
     218,   194,     9,     3,    81,    82,    83,    84,    81,    82,
      10,   150,    19,     9,   119,    11,     9,     3,   211,    71,
     159,    28,   170,   191,    10,    85,   219,     3,    88,    81,
      82,    83,    84,   207,    10,    47,    48,    47,    48,   207,
     258,     9,   210,   148,   262,    41,    42,    43,    44,    45,
      46,    47,    48,    49,     9,   248,    34,     9,    36,    11,
      38,    71,    40,   281,   238,   311,     9,   258,    76,    77,
     238,   262,     9,   212,   213,    71,     9,    24,    11,    81,
      82,    85,   221,    27,   189,    81,    82,    83,    84,    41,
      42,    43,    44,    45,    46,    47,    48,    49,     9,    25,
      85,   275,     9,    81,    11,    86,    81,   275,    41,    42,
      43,    44,    45,    46,    47,    48,    49,     9,   311,    11,
      81,   289,     9,     4,    11,    10,    10,    10,    10,    81,
      82,    83,    84,   272,    41,    42,    43,    44,    45,    46,
      47,    48,    49,   291,    10,    70,    85,    50,    81,    82,
      83,    84,    30,     9,    46,    47,    48,    49,    62,    46,
      47,    48,    49,    73,    73,    35,     0,     9,     9,   317,
       5,     6,     7,     8,    81,    82,    83,    84,    35,    10,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      82,    83,    84,    11,     9,    82,    83,    84,    32,     5,
       6,     7,     8,     3,    10,    14,    15,    16,    17,    18,
      19,    20,    21,    22,    23,    50,    34,    81,    36,    37,
      38,    39,    82,    32,    85,     9,    81,    62,    63,    64,
      65,    66,    67,    51,     5,     6,     7,     8,    72,    10,
       9,    81,     9,    11,    50,    34,     9,    36,    37,    38,
      39,    12,    82,    71,    82,    82,    62,    63,    64,    65,
      66,    67,    51,    72,    52,    53,    54,    55,    56,    57,
      58,    59,    60,    61,    81,    10,    81,    35,    12,    50,
       9,    84,    71,    49,    39,    13,   148,   206,   153,   197,
     247,    85,   198,    81,   222,   215,   294,   211,   192,   194,
     120,    79
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    32,    72,    91,    92,    93,    94,    95,    96,    97,
      98,    99,   100,   101,   102,   103,   104,    89,   157,   157,
     157,    29,    29,     7,     9,    11,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    81,    82,    83,    84,   109,
     121,   122,   123,   135,   137,   138,   139,   140,   154,    26,
      81,   114,    24,   114,    33,    29,     0,    92,    13,   114,
     114,    81,   122,   136,   137,     9,   136,   137,     9,     9,
       9,     9,     9,     9,    86,    87,    24,   110,   111,    85,
       5,     6,     7,     8,    50,   114,    27,   114,   157,   157,
     114,     9,   157,    10,    10,    10,    85,    12,     9,    82,
     135,   156,   156,     7,    82,   135,   155,   155,   155,     9,
      71,   122,   135,   143,   144,   145,   146,   147,   148,   149,
     150,   151,   135,   121,   112,   113,   114,   157,    25,   116,
     117,   122,   122,   122,   122,   122,    81,     9,    19,    28,
     118,   120,   135,   141,   142,    25,   157,    18,    74,   105,
     106,   107,    34,    36,    38,    40,    81,   125,   126,   127,
     129,   134,    86,   137,    81,    10,    10,    10,    10,    10,
     122,   143,   146,    62,    63,    64,    65,    66,    67,   153,
      68,    69,     3,    10,     4,    70,    88,    85,    50,    81,
     115,   143,    30,   152,   124,   135,   109,     9,   119,   157,
      62,    85,   116,   143,    73,    73,   157,    35,     9,     9,
      35,    10,    85,    52,    53,    54,    55,    56,    57,    58,
      59,    60,    61,   114,   130,   135,    10,   122,     9,   137,
     144,   145,    71,   140,   113,   115,    82,    10,    85,   110,
     136,    85,   137,   142,   157,   157,    81,   127,     9,   128,
     129,   143,     9,   157,   126,     9,    11,   131,   136,   140,
      31,    85,   118,   135,    10,     9,    76,    77,   108,   128,
      10,    85,    10,   128,    82,    12,    11,    34,    36,    37,
      38,    39,    51,    71,   132,   133,    10,    82,    82,   157,
     136,    81,    81,    10,   129,    10,    10,    12,    35,    49,
     137,     9,   114,    84,    49,   133,    10,    39,   143,   114,
      10
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,    90,    91,    91,    92,    92,    92,    92,    92,    92,
      92,    92,    92,    92,    92,    92,    93,    94,    95,    96,
      97,    98,    99,    99,   100,   101,   101,   102,   103,   104,
     105,   105,   106,   107,   108,   108,   108,   109,   109,   110,
     111,   111,   112,   112,   113,   113,   113,   114,   115,   116,
     116,   117,   118,   118,   119,   119,   120,   121,   121,   122,
     122,   122,   122,   122,   122,   123,   123,   123,   123,   124,
     124,   125,   125,   126,   126,   127,   128,   128,   129,   130,
     130,   130,   130,   130,   130,   130,   130,   130,   130,   130,
     131,   131,   131,   132,   132,   132,   133,   133,   133,   133,
     133,   133,   133,   133,   134,   134,   134,   134,   135,   135,
     135,   135,   136,   136,   137,   137,   137,   138,   138,   138,
     138,   138,   139,   139,   140,   140,   141,   141,   142,   143,
     143,   144,   144,   145,   145,   146,   146,   146,   147,   147,
     148,   148,   148,   149,   150,   151,   152,   152,   152,   152,
     153,   153,   153,   153,   153,   153,   154,   154,   154,   154,
     154,   155,   155,   155,   156,   156,   157,   157
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     2,     2,     2,     7,
       4,     4,     5,     8,     6,     4,     6,     3,     3,     5,
       1,     1,     4,     3,     0,     2,     2,     1,     1,     3,
       0,     2,     1,     3,     1,     2,     3,     1,     1,     0,
       1,     2,     2,     1,     3,     5,     3,     1,     3,     1,
       1,     1,     1,     3,     3,     3,     3,     3,     3,     1,
       3,     1,     3,     1,     1,     4,     1,     3,     1,     1,
       1,     1,     4,     1,     1,     1,     1,     1,     1,     1,
       0,     2,     3,     0,     1,     2,     2,     1,     2,     2,
       2,     2,     4,     2,     4,     5,     7,     4,     1,     5,
       4,     3,     1,     3,     1,     1,     3,     1,     1,     1,
       1,     1,     3,     4,     1,     1,     1,     3,     3,     1,
       3,     1,     3,     1,     2,     1,     3,     4,     1,     3,
       1,     1,     1,     3,     3,     5,     0,     2,     4,     4,
       1,     1,     1,     1,     1,     1,     4,     4,     4,     4,
       4,     1,     1,     1,     1,     1,     1,     2
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
#line 186 "sql.y"
        {
            append_list(states, (yyvsp[0].statement));
            (yyval.list) = states;
        }
#line 2109 "y.tab.c"
    break;

  case 3: /* statements: statements statement  */
#line 191 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].statement));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 2118 "y.tab.c"
    break;

  case 4: /* statement: begin_transaction_statement  */
#line 198 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = BEGIN_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2128 "y.tab.c"
    break;

  case 5: /* statement: commit_transaction_statement  */
#line 204 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = COMMIT_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2138 "y.tab.c"
    break;

  case 6: /* statement: rollback_transaction_statement  */
#line 210 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ROLLBACK_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2148 "y.tab.c"
    break;

  case 7: /* statement: create_table_statement  */
#line 216 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_TABLE_STMT;
            statement->create_table_node = (yyvsp[0].create_table_node);
            (yyval.statement) = statement;
        }
#line 2159 "y.tab.c"
    break;

  case 8: /* statement: drop_table_statement  */
#line 223 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_TABLE_STMT;
            statement->drop_table_node = (yyvsp[0].drop_table_node);
            (yyval.statement) = statement;
        }
#line 2170 "y.tab.c"
    break;

  case 9: /* statement: select_statement  */
#line 230 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SELECT_STMT;
            statement->select_node = (yyvsp[0].select_node);
            (yyval.statement) = statement;
        }
#line 2181 "y.tab.c"
    break;

  case 10: /* statement: insert_statement  */
#line 237 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = INSERT_STMT;
            statement->insert_node = (yyvsp[0].insert_node);
            (yyval.statement) = statement;
        }
#line 2192 "y.tab.c"
    break;

  case 11: /* statement: update_statement  */
#line 244 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = UPDATE_STMT;
            statement->update_node = (yyvsp[0].update_node);
            (yyval.statement) = statement;
        }
#line 2203 "y.tab.c"
    break;

  case 12: /* statement: delete_statement  */
#line 251 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DELETE_STMT;
            statement->delete_node = (yyvsp[0].delete_node);
            (yyval.statement) = statement;
        }
#line 2214 "y.tab.c"
    break;

  case 13: /* statement: describe_statement  */
#line 258 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DESCRIBE_STMT;
            statement->describe_node = (yyvsp[0].describe_node);
            (yyval.statement) = statement;
        }
#line 2225 "y.tab.c"
    break;

  case 14: /* statement: show_statement  */
#line 265 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SHOW_STMT;
            statement->show_node = (yyvsp[0].show_node);
            (yyval.statement) = statement;
        }
#line 2236 "y.tab.c"
    break;

  case 15: /* statement: alter_table_statement  */
#line 272 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ALTER_TABLE_STMT;
            statement->alter_table_node = (yyvsp[0].alter_table_node);
            (yyval.statement) = statement;
        }
#line 2247 "y.tab.c"
    break;

  case 19: /* create_table_statement: CREATE TABLE table '(' base_table_element_commalist ')' end  */
#line 291 "sql.y"
        {
            CreateTableNode *create_table_node = instance(CreateTableNode);
            create_table_node->table_name = (yyvsp[-4].strVal);
            create_table_node->base_table_element_commalist = (yyvsp[-2].list);
            (yyval.create_table_node) = create_table_node;
        }
#line 2258 "y.tab.c"
    break;

  case 20: /* drop_table_statement: DROP TABLE table end  */
#line 301 "sql.y"
        {
            DropTableNode *drop_table_node = instance(DropTableNode);
            drop_table_node->table_name = (yyvsp[-1].strVal);
            (yyval.drop_table_node) = drop_table_node;
        }
#line 2268 "y.tab.c"
    break;

  case 21: /* select_statement: SELECT selection table_exp end  */
#line 310 "sql.y"
        {
            SelectNode *select_node = instance(SelectNode);
            select_node->selection = (yyvsp[-2].selection_node);
            select_node->table_exp = (yyvsp[-1].table_exp_node);
            (yyval.select_node) = select_node;
        }
#line 2279 "y.tab.c"
    break;

  case 22: /* insert_statement: INSERT INTO table values_or_query_spec end  */
#line 320 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = true;
            node->table_name = (yyvsp[-2].strVal);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2291 "y.tab.c"
    break;

  case 23: /* insert_statement: INSERT INTO table '(' columns ')' values_or_query_spec end  */
#line 328 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = false;
            node->table_name = (yyvsp[-5].strVal);
            node->column_list = (yyvsp[-3].list);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2304 "y.tab.c"
    break;

  case 24: /* update_statement: UPDATE table SET assignments opt_where_clause end  */
#line 340 "sql.y"
        {
            UpdateNode *node = instance(UpdateNode);
            node->table_name = (yyvsp[-4].strVal);
            node->assignment_list = (yyvsp[-2].list);
            node->where_clause = (yyvsp[-1].where_clause_node);
            (yyval.update_node) = node;
        }
#line 2316 "y.tab.c"
    break;

  case 25: /* delete_statement: DELETE FROM table end  */
#line 351 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.delete_node) = node;
        }
#line 2326 "y.tab.c"
    break;

  case 26: /* delete_statement: DELETE FROM table WHERE search_condition end  */
#line 357 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-3].strVal);
            node->condition_node = (yyvsp[-1].search_condition_node);
            (yyval.delete_node) = node;
        }
#line 2337 "y.tab.c"
    break;

  case 27: /* describe_statement: DESCRIBE table end  */
#line 367 "sql.y"
        {
            DescribeNode *node = instance(DescribeNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.describe_node) = node;
        }
#line 2347 "y.tab.c"
    break;

  case 28: /* show_statement: SHOW TABLES end  */
#line 376 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_TABLES;
            (yyval.show_node) = node;
        }
#line 2357 "y.tab.c"
    break;

  case 29: /* alter_table_statement: ALTER TABLE table alter_table_action end  */
#line 385 "sql.y"
        {
            (yyval.alter_table_node) = instance(AlterTableNode);
            (yyval.alter_table_node)->table_name = (yyvsp[-2].strVal);
            (yyval.alter_table_node)->action = (yyvsp[-1].alter_table_action);
        }
#line 2367 "y.tab.c"
    break;

  case 30: /* alter_table_action: add_column_def  */
#line 393 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_ADD_COLUMN;
            action->action.add_column = (yyvsp[0].add_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2378 "y.tab.c"
    break;

  case 31: /* alter_table_action: drop_column_def  */
#line 400 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_DROP_COLUMN;
            action->action.drop_column = (yyvsp[0].drop_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2389 "y.tab.c"
    break;

  case 32: /* add_column_def: ADD COLUMN column_def column_position_def  */
#line 409 "sql.y"
        {
            AddColumnDef *node = instance(AddColumnDef);
            node->column_def = (yyvsp[-1].column_def_node);
            node->position_def = (yyvsp[0].column_position_def);
            (yyval.add_column_def) = node;
        }
#line 2400 "y.tab.c"
    break;

  case 33: /* drop_column_def: DROP COLUMN IDENTIFIER  */
#line 418 "sql.y"
        {
            DropColumnDef *node = instance(DropColumnDef);
            node->column_name = (yyvsp[0].strVal);
            (yyval.drop_column_def) = node;
        }
#line 2410 "y.tab.c"
    break;

  case 34: /* column_position_def: %empty  */
#line 426 "sql.y"
    {
        (yyval.column_position_def) = NULL;
    }
#line 2418 "y.tab.c"
    break;

  case 35: /* column_position_def: BEFORE IDENTIFIER  */
#line 430 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_BEFORE;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2429 "y.tab.c"
    break;

  case 36: /* column_position_def: AFTER IDENTIFIER  */
#line 437 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_AFTER;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2440 "y.tab.c"
    break;

  case 37: /* selection: scalar_exp_commalist  */
#line 446 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = false;
            selection_node->scalar_exp_list = (yyvsp[0].list);
            (yyval.selection_node) = selection_node;
        }
#line 2451 "y.tab.c"
    break;

  case 38: /* selection: '*'  */
#line 453 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = true;
            (yyval.selection_node) = selection_node;
        }
#line 2461 "y.tab.c"
    break;

  case 39: /* table_exp: from_clause opt_where_clause limit_clause  */
#line 461 "sql.y"
        {
            TableExpNode *table_exp = instance(TableExpNode);
            table_exp->from_clause = (yyvsp[-2].from_clause_node);
            table_exp->where_clause = (yyvsp[-1].where_clause_node);
            table_exp->limit_clause = (yyvsp[0].limit_clause_node);
            (yyval.table_exp_node) = table_exp;
        }
#line 2473 "y.tab.c"
    break;

  case 40: /* from_clause: %empty  */
#line 471 "sql.y"
        {
            (yyval.from_clause_node) = NULL;
        }
#line 2481 "y.tab.c"
    break;

  case 41: /* from_clause: FROM table_ref_commalist  */
#line 475 "sql.y"
        {
            FromClauseNode *from_clause = instance(FromClauseNode);
            from_clause->from = (yyvsp[0].list);
            (yyval.from_clause_node) = from_clause;
        }
#line 2491 "y.tab.c"
    break;

  case 42: /* table_ref_commalist: table_ref  */
#line 483 "sql.y"
        {
            List *list = create_list(NODE_TABLE_REFER);
            append_list(list, (yyvsp[0].table_ref_node));
            (yyval.list) = list;
        }
#line 2501 "y.tab.c"
    break;

  case 43: /* table_ref_commalist: table_ref_commalist ',' table_ref  */
#line 489 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].table_ref_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2510 "y.tab.c"
    break;

  case 44: /* table_ref: table  */
#line 496 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2520 "y.tab.c"
    break;

  case 45: /* table_ref: table range_variable  */
#line 502 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-1].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2531 "y.tab.c"
    break;

  case 46: /* table_ref: table AS range_variable  */
#line 509 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-2].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2542 "y.tab.c"
    break;

  case 47: /* table: IDENTIFIER  */
#line 518 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2550 "y.tab.c"
    break;

  case 48: /* range_variable: IDENTIFIER  */
#line 524 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2558 "y.tab.c"
    break;

  case 49: /* opt_where_clause: %empty  */
#line 530 "sql.y"
        {
            (yyval.where_clause_node) = NULL;
        }
#line 2566 "y.tab.c"
    break;

  case 50: /* opt_where_clause: where_clause  */
#line 534 "sql.y"
        {
            (yyval.where_clause_node) = (yyvsp[0].where_clause_node);
        }
#line 2574 "y.tab.c"
    break;

  case 51: /* where_clause: WHERE search_condition  */
#line 540 "sql.y"
        {
            WhereClauseNode *where_clause_node = instance(WhereClauseNode);
            where_clause_node->condition = (yyvsp[0].search_condition_node);
            (yyval.where_clause_node) = where_clause_node;
        }
#line 2584 "y.tab.c"
    break;

  case 52: /* values_or_query_spec: VALUES opt_values  */
#line 548 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_VALUES;
            values_or_query_spec->values = (yyvsp[0].list);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2595 "y.tab.c"
    break;

  case 53: /* values_or_query_spec: query_spec  */
#line 555 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_QUERY_SPEC;
            values_or_query_spec->query_spec = (yyvsp[0].query_spec_node);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2606 "y.tab.c"
    break;

  case 54: /* opt_values: '(' value_items ')'  */
#line 564 "sql.y"
        {
            (yyval.list) = create_list(NODE_LIST);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2615 "y.tab.c"
    break;

  case 55: /* opt_values: opt_values ',' '(' value_items ')'  */
#line 569 "sql.y"
        {
            (yyval.list) = (yyvsp[-4].list);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2624 "y.tab.c"
    break;

  case 56: /* query_spec: SELECT selection table_exp  */
#line 576 "sql.y"
        {
            QuerySpecNode *query_spec = instance(QuerySpecNode);
            query_spec->selection = (yyvsp[-1].selection_node);
            query_spec->table_exp = (yyvsp[0].table_exp_node);
            (yyval.query_spec_node) = query_spec;
        }
#line 2635 "y.tab.c"
    break;

  case 57: /* scalar_exp_commalist: scalar_exp  */
#line 585 "sql.y"
        {
            List *scalar_exp_list = create_list(NODE_SCALAR_EXP);
            append_list(scalar_exp_list, (yyvsp[0].scalar_exp_node));
            (yyval.list) = scalar_exp_list;
        }
#line 2645 "y.tab.c"
    break;

  case 58: /* scalar_exp_commalist: scalar_exp_commalist ',' scalar_exp  */
#line 591 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].scalar_exp_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2654 "y.tab.c"
    break;

  case 59: /* scalar_exp: calculate  */
#line 598 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_CALCULATE;
            scalar_exp_node->calculate = (yyvsp[0].calculate_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2665 "y.tab.c"
    break;

  case 60: /* scalar_exp: column  */
#line 605 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_COLUMN;
            scalar_exp_node->column = (yyvsp[0].column_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2676 "y.tab.c"
    break;

  case 61: /* scalar_exp: function  */
#line 612 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_FUNCTION;
            scalar_exp_node->function = (yyvsp[0].function_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2687 "y.tab.c"
    break;

  case 62: /* scalar_exp: value_item  */
#line 619 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_VALUE;
            scalar_exp_node->value = (yyvsp[0].value_item_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2698 "y.tab.c"
    break;

  case 63: /* scalar_exp: '(' scalar_exp ')'  */
#line 626 "sql.y"
        {
            (yyval.scalar_exp_node) = (yyvsp[-1].scalar_exp_node);
        }
#line 2706 "y.tab.c"
    break;

  case 64: /* scalar_exp: scalar_exp AS IDENTIFIER  */
#line 630 "sql.y"
        {
            (yyvsp[-2].scalar_exp_node)->alias = (yyvsp[0].strVal);
            (yyval.scalar_exp_node) = (yyvsp[-2].scalar_exp_node);
        }
#line 2715 "y.tab.c"
    break;

  case 65: /* calculate: scalar_exp '+' scalar_exp  */
#line 637 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_ADD;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2727 "y.tab.c"
    break;

  case 66: /* calculate: scalar_exp '-' scalar_exp  */
#line 645 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_SUB;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2739 "y.tab.c"
    break;

  case 67: /* calculate: scalar_exp '*' scalar_exp  */
#line 653 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_MUL;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2751 "y.tab.c"
    break;

  case 68: /* calculate: scalar_exp '/' scalar_exp  */
#line 661 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_DIV;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2763 "y.tab.c"
    break;

  case 69: /* columns: column  */
#line 671 "sql.y"
        {
            List *column_set_node = create_list(NODE_COLUMN);
            append_list(column_set_node, (yyvsp[0].column_node));
            (yyval.list) = column_set_node;
        }
#line 2773 "y.tab.c"
    break;

  case 70: /* columns: columns ',' column  */
#line 677 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].column_node));
        }
#line 2782 "y.tab.c"
    break;

  case 71: /* base_table_element_commalist: base_table_element  */
#line 684 "sql.y"
        {
            List *base_table_element_commalist = create_list(NODE_BASE_TABLE_ELEMENT);
            append_list(base_table_element_commalist, (yyvsp[0].base_table_element));
            (yyval.list) = base_table_element_commalist;
        }
#line 2792 "y.tab.c"
    break;

  case 72: /* base_table_element_commalist: base_table_element_commalist ',' base_table_element  */
#line 690 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].base_table_element));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2801 "y.tab.c"
    break;

  case 73: /* base_table_element: column_def  */
#line 697 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = (yyvsp[0].column_def_node);
            node->table_contraint_def = NULL;
            node->type = TELE_COLUMN_DEF;
            (yyval.base_table_element) = node;
        }
#line 2813 "y.tab.c"
    break;

  case 74: /* base_table_element: table_contraint_def  */
#line 705 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = NULL;
            node->table_contraint_def = (yyvsp[0].table_contraint_def);
            node->type = TELE_TABLE_CONTRAINT_DEF;
            (yyval.base_table_element) = node;
        }
#line 2825 "y.tab.c"
    break;

  case 75: /* column_def: column_def_name data_type array_dim_clause column_def_opt_list  */
#line 728 "sql.y"
        {
            ColumnDefNode *column_def = instance(ColumnDefNode);
            column_def->column = (yyvsp[-3].column_def_name);
            column_def->data_type = (yyvsp[-2].data_type_node);
            column_def->array_dim = (yyvsp[-1].intVal);
            column_def->column_def_opt_list = (yyvsp[0].list);
            (yyval.column_def_node) = column_def;
        }
#line 2838 "y.tab.c"
    break;

  case 76: /* column_def_name_commalist: column_def_name  */
#line 739 "sql.y"
        {
            List *list = create_list(NODE_COLUMN_DEF_NAME);
            append_list(list, (yyvsp[0].column_def_name));
            (yyval.list) = list;
        }
#line 2848 "y.tab.c"
    break;

  case 77: /* column_def_name_commalist: column_def_name_commalist ',' column_def_name  */
#line 745 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].column_def_name));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2857 "y.tab.c"
    break;

  case 78: /* column_def_name: IDENTIFIER  */
#line 752 "sql.y"
        {
            ColumnDefName *column_def_name = instance(ColumnDefName);
            column_def_name->column = (yyvsp[0].strVal);
            (yyval.column_def_name) = column_def_name;
        }
#line 2867 "y.tab.c"
    break;

  case 79: /* data_type: INT  */
#line 760 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_INT; 
            (yyval.data_type_node) = node;
        }
#line 2877 "y.tab.c"
    break;

  case 80: /* data_type: LONG  */
#line 766 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_LONG;  
            (yyval.data_type_node) = node;
        }
#line 2887 "y.tab.c"
    break;

  case 81: /* data_type: CHAR  */
#line 772 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_CHAR; 
            (yyval.data_type_node) = node;
        }
#line 2897 "y.tab.c"
    break;

  case 82: /* data_type: VARCHAR '(' INTVALUE ')'  */
#line 778 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_VARCHAR; 
            node->len = (yyvsp[-1].intVal);
            (yyval.data_type_node) = node;
        }
#line 2908 "y.tab.c"
    break;

  case 83: /* data_type: STRING  */
#line 785 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_STRING; 
            (yyval.data_type_node) = node;
        }
#line 2918 "y.tab.c"
    break;

  case 84: /* data_type: BOOL  */
#line 791 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_BOOL; 
            (yyval.data_type_node) = node;
        }
#line 2928 "y.tab.c"
    break;

  case 85: /* data_type: FLOAT  */
#line 797 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_FLOAT; 
            (yyval.data_type_node) = node;
        }
#line 2938 "y.tab.c"
    break;

  case 86: /* data_type: DOUBLE  */
#line 803 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DOUBLE; 
            (yyval.data_type_node) = node;
        }
#line 2948 "y.tab.c"
    break;

  case 87: /* data_type: TIMESTAMP  */
#line 809 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_TIMESTAMP; 
            (yyval.data_type_node) = node;
        }
#line 2958 "y.tab.c"
    break;

  case 88: /* data_type: DATE  */
#line 815 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DATE; 
            (yyval.data_type_node) = node;
        }
#line 2968 "y.tab.c"
    break;

  case 89: /* data_type: table  */
#line 821 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_REFERENCE;
            node->table_name = (yyvsp[0].strVal);
            (yyval.data_type_node) = node;
        }
#line 2979 "y.tab.c"
    break;

  case 90: /* array_dim_clause: %empty  */
#line 830 "sql.y"
        {
            (yyval.intVal) = 0;
        }
#line 2987 "y.tab.c"
    break;

  case 91: /* array_dim_clause: '[' ']'  */
#line 834 "sql.y"
        {
            (yyval.intVal) = 1;
        }
#line 2995 "y.tab.c"
    break;

  case 92: /* array_dim_clause: array_dim_clause '[' ']'  */
#line 838 "sql.y"
        {
            (yyval.intVal)++;
        }
#line 3003 "y.tab.c"
    break;

  case 93: /* column_def_opt_list: %empty  */
#line 844 "sql.y"
        {
            (yyval.list) = NULL;
        }
#line 3011 "y.tab.c"
    break;

  case 94: /* column_def_opt_list: column_def_opt  */
#line 848 "sql.y"
        {
            (yyval.list) = create_list(NODE_COLUMN_DEF_OPT);
            append_list((yyval.list), (yyvsp[0].column_def_opt));
        }
#line 3020 "y.tab.c"
    break;

  case 95: /* column_def_opt_list: column_def_opt_list column_def_opt  */
#line 853 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].column_def_opt));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 3029 "y.tab.c"
    break;

  case 96: /* column_def_opt: NOT NULLX  */
#line 860 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_NOT_NULL; 
            (yyval.column_def_opt) = node;
        }
#line 3039 "y.tab.c"
    break;

  case 97: /* column_def_opt: UNIQUE  */
#line 866 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_UNIQUE; 
            (yyval.column_def_opt) = node;
        }
#line 3049 "y.tab.c"
    break;

  case 98: /* column_def_opt: PRIMARY KEY  */
#line 872 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_PRIMARY_KEY; 
            (yyval.column_def_opt) = node;
        }
#line 3059 "y.tab.c"
    break;

  case 99: /* column_def_opt: DEFAULT value_item  */
#line 878 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_VALUE;
            node->value = (yyvsp[0].value_item_node);
            (yyval.column_def_opt) = node;
        }
#line 3070 "y.tab.c"
    break;

  case 100: /* column_def_opt: DEFAULT NULLX  */
#line 885 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_NULL;
            (yyval.column_def_opt) = node;
        }
#line 3080 "y.tab.c"
    break;

  case 101: /* column_def_opt: COMMENT STRINGVALUE  */
#line 891 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_COMMENT;
            node->comment = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3091 "y.tab.c"
    break;

  case 102: /* column_def_opt: CHECK '(' search_condition ')'  */
#line 898 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_CHECK_CONDITION;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.column_def_opt) = node;
        }
#line 3102 "y.tab.c"
    break;

  case 103: /* column_def_opt: REFERENCES table  */
#line 905 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_REFERENECS;
            node->refer_table = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3113 "y.tab.c"
    break;

  case 104: /* table_contraint_def: UNIQUE '(' column_def_name_commalist ')'  */
#line 914 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_UNIQUE;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3124 "y.tab.c"
    break;

  case 105: /* table_contraint_def: PRIMARY KEY '(' column_def_name_commalist ')'  */
#line 921 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_PRIMARY_KEY;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3135 "y.tab.c"
    break;

  case 106: /* table_contraint_def: FOREIGN KEY '(' column_def_name_commalist ')' REFERENCES table  */
#line 928 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_FOREIGN_KEY;
            node->column_commalist = (yyvsp[-3].list);
            node->table = (yyvsp[0].strVal);
            (yyval.table_contraint_def) = node;
        }
#line 3147 "y.tab.c"
    break;

  case 107: /* table_contraint_def: CHECK '(' search_condition ')'  */
#line 936 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_CHECK;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.table_contraint_def) = node;
        }
#line 3158 "y.tab.c"
    break;

  case 108: /* column: IDENTIFIER  */
#line 945 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[0].strVal);
            column_node->has_sub_column = false;
            (yyval.column_node) = column_node;
        }
#line 3169 "y.tab.c"
    break;

  case 109: /* column: '(' IDENTIFIER ')' '.' column  */
#line 952 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->sub_column = (yyvsp[0].column_node);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3181 "y.tab.c"
    break;

  case 110: /* column: IDENTIFIER '{' scalar_exp_commalist '}'  */
#line 960 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->scalar_exp_list = (yyvsp[-1].list);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3193 "y.tab.c"
    break;

  case 111: /* column: IDENTIFIER '.' column  */
#line 968 "sql.y"
        {
            (yyval.column_node) = (yyvsp[0].column_node);
            (yyval.column_node)->range_variable = (yyvsp[-2].strVal);
        }
#line 3202 "y.tab.c"
    break;

  case 112: /* value_items: value_item  */
#line 975 "sql.y"
        {
            List *value_list = create_list(NODE_VALUE_ITEM);
            append_list(value_list, (yyvsp[0].value_item_node));
            (yyval.list) = value_list;
        }
#line 3212 "y.tab.c"
    break;

  case 113: /* value_items: value_items ',' value_item  */
#line 981 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].value_item_node));
        }
#line 3221 "y.tab.c"
    break;

  case 114: /* value_item: atom  */
#line 988 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ATOM;
            node->value.atom = (yyvsp[0].atom_node);
            (yyval.value_item_node) = node;
        }
#line 3232 "y.tab.c"
    break;

  case 115: /* value_item: NULLX  */
#line 995 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_NULL;
            (yyval.value_item_node) = node;
        }
#line 3242 "y.tab.c"
    break;

  case 116: /* value_item: '[' value_items ']'  */
#line 1001 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ARRAY;
            node->value.value_list = (yyvsp[-1].list);
            (yyval.value_item_node) = node;
        }
#line 3253 "y.tab.c"
    break;

  case 117: /* atom: INTVALUE  */
#line 1010 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.intval = (yyvsp[0].intVal);
            node->type = A_INT;
            (yyval.atom_node) = node;
        }
#line 3264 "y.tab.c"
    break;

  case 118: /* atom: BOOLVALUE  */
#line 1017 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.boolval = (yyvsp[0].boolVal);
            node->type = A_BOOL;
            (yyval.atom_node) = node;
        }
#line 3275 "y.tab.c"
    break;

  case 119: /* atom: STRINGVALUE  */
#line 1024 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.strval = (yyvsp[0].strVal);
            node->type = A_STRING;
            (yyval.atom_node) = node;
        }
#line 3286 "y.tab.c"
    break;

  case 120: /* atom: FLOATVALUE  */
#line 1031 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.floatval = (yyvsp[0].floatVal);
            node->type = A_FLOAT;
            (yyval.atom_node) = node;
        }
#line 3297 "y.tab.c"
    break;

  case 121: /* atom: REFERVALUE  */
#line 1038 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.referval = (yyvsp[0].referVal);
            node->type = A_REFERENCE;
            (yyval.atom_node) = node;
        }
#line 3308 "y.tab.c"
    break;

  case 122: /* REFERVALUE: '(' value_items ')'  */
#line 1048 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = DIRECTLY;
            refer->nest_value_list = (yyvsp[-1].list);
            (yyval.referVal) = refer;
        }
#line 3319 "y.tab.c"
    break;

  case 123: /* REFERVALUE: REF '(' search_condition ')'  */
#line 1056 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = INDIRECTLY;
            refer->condition = (yyvsp[-1].search_condition_node);
            (yyval.referVal) = refer;
        }
#line 3330 "y.tab.c"
    break;

  case 124: /* BOOLVALUE: TRUE  */
#line 1065 "sql.y"
        {
            (yyval.boolVal) = true;
        }
#line 3338 "y.tab.c"
    break;

  case 125: /* BOOLVALUE: FALSE  */
#line 1069 "sql.y"
        {
            (yyval.boolVal) = false;
        }
#line 3346 "y.tab.c"
    break;

  case 126: /* assignments: assignment  */
#line 1075 "sql.y"
        {
            List *list = create_list(NODE_ASSIGNMENT);
            append_list(list, (yyvsp[0].assignment_node));
            (yyval.list) = list;
        }
#line 3356 "y.tab.c"
    break;

  case 127: /* assignments: assignments ',' assignment  */
#line 1081 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].assignment_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 3365 "y.tab.c"
    break;

  case 128: /* assignment: column EQ value_item  */
#line 1088 "sql.y"
        {
            AssignmentNode *node = instance(AssignmentNode);
            node->column = (yyvsp[-2].column_node);
            node->value = (yyvsp[0].value_item_node);
            (yyval.assignment_node) = node;
        }
#line 3376 "y.tab.c"
    break;

  case 129: /* search_condition: boolean_term  */
#line 1097 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3386 "y.tab.c"
    break;

  case 130: /* search_condition: search_condition OR boolean_term  */
#line 1103 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->or_search_condition = (yyvsp[-2].search_condition_node);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3397 "y.tab.c"
    break;

  case 131: /* boolean_term: boolean_factor  */
#line 1112 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3407 "y.tab.c"
    break;

  case 132: /* boolean_term: boolean_term AND boolean_factor  */
#line 1118 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->and_boolean_term = (yyvsp[-2].boolean_term_node);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3418 "y.tab.c"
    break;

  case 133: /* boolean_factor: boolean_test  */
#line 1127 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = false;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3429 "y.tab.c"
    break;

  case 134: /* boolean_factor: NOT boolean_test  */
#line 1134 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = true;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3440 "y.tab.c"
    break;

  case 135: /* boolean_test: boolean_primary  */
#line 1143 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[0].boolean_primary_node);
            test_node->type = NONE_TRUE_VALUE;
            (yyval.boolean_test_node) = test_node;
        }
#line 3451 "y.tab.c"
    break;

  case 136: /* boolean_test: boolean_primary IS BOOLVALUE  */
#line 1150 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-2].boolean_primary_node);
            test_node->type = IS_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[0].boolVal);
            (yyval.boolean_test_node) = test_node;
        }
#line 3463 "y.tab.c"
    break;

  case 137: /* boolean_test: boolean_primary IS NOT BOOLVALUE  */
#line 1158 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-3].boolean_primary_node);
            test_node->type = IS_NOT_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[-1].keyword);
            (yyval.boolean_test_node) = test_node;
        }
#line 3475 "y.tab.c"
    break;

  case 138: /* boolean_primary: predicate  */
#line 1168 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = PREDICATE_BOOLEAN_PRIMAYR;
            primary_node->predicate = (yyvsp[0].predicate_node);
            primary_node->search_condition = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3487 "y.tab.c"
    break;

  case 139: /* boolean_primary: '(' search_condition ')'  */
#line 1176 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = SEARCH_CONDITION_BOOLEAN_PRIMAYR;
            primary_node->search_condition = (yyvsp[-1].search_condition_node);
            primary_node->predicate = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3499 "y.tab.c"
    break;

  case 140: /* predicate: comparison_predicate  */
#line 1185 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_COMPARISON;
            predicate->comparison = (yyvsp[0].comparison_node);
            (yyval.predicate_node) = predicate;
        }
#line 3510 "y.tab.c"
    break;

  case 141: /* predicate: like_predicate  */
#line 1192 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_LIKE;
            predicate->like = (yyvsp[0].like_node);
            (yyval.predicate_node) = predicate;
        }
#line 3521 "y.tab.c"
    break;

  case 142: /* predicate: in_predicate  */
#line 1199 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_IN;
            predicate->in = (yyvsp[0].in_node);
            (yyval.predicate_node) = predicate;
        }
#line 3532 "y.tab.c"
    break;

  case 143: /* comparison_predicate: scalar_exp compare scalar_exp  */
#line 1208 "sql.y"
        {
            ComparisonNode *comparison_node = instance(ComparisonNode);
            comparison_node->left = (yyvsp[-2].scalar_exp_node);
            comparison_node->type = (yyvsp[-1].compare_type);
            comparison_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.comparison_node) = comparison_node;
        }
#line 3544 "y.tab.c"
    break;

  case 144: /* like_predicate: column LIKE value_item  */
#line 1218 "sql.y"
        {
            LikeNode *like_node = instance(LikeNode);
            like_node->column = (yyvsp[-2].column_node);
            like_node->value = (yyvsp[0].value_item_node);
            (yyval.like_node) = like_node;
        }
#line 3555 "y.tab.c"
    break;

  case 145: /* in_predicate: column IN '(' value_items ')'  */
#line 1227 "sql.y"
        {
            InNode *in_node = instance(InNode);
            in_node->column = (yyvsp[-4].column_node);
            in_node->value_list = (yyvsp[-1].list);
            (yyval.in_node) = in_node;
        }
#line 3566 "y.tab.c"
    break;

  case 146: /* limit_clause: %empty  */
#line 1236 "sql.y"
        {
            (yyval.limit_clause_node) = NULL;
        }
#line 3574 "y.tab.c"
    break;

  case 147: /* limit_clause: LIMIT INTVALUE  */
#line 1240 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = 0;
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3585 "y.tab.c"
    break;

  case 148: /* limit_clause: LIMIT INTVALUE ',' INTVALUE  */
#line 1247 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = (yyvsp[-2].intVal);
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3596 "y.tab.c"
    break;

  case 149: /* limit_clause: LIMIT INTVALUE OFFSET INTVALUE  */
#line 1254 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->rows = (yyvsp[-2].intVal);
            node->offset = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3607 "y.tab.c"
    break;

  case 150: /* compare: EQ  */
#line 1262 "sql.y"
            { (yyval.compare_type) = O_EQ; }
#line 3613 "y.tab.c"
    break;

  case 151: /* compare: NE  */
#line 1263 "sql.y"
            { (yyval.compare_type) = O_NE; }
#line 3619 "y.tab.c"
    break;

  case 152: /* compare: GT  */
#line 1264 "sql.y"
            { (yyval.compare_type) = O_GT; }
#line 3625 "y.tab.c"
    break;

  case 153: /* compare: GE  */
#line 1265 "sql.y"
            { (yyval.compare_type) = O_GE; }
#line 3631 "y.tab.c"
    break;

  case 154: /* compare: LT  */
#line 1266 "sql.y"
            { (yyval.compare_type) = O_LT; }
#line 3637 "y.tab.c"
    break;

  case 155: /* compare: LE  */
#line 1267 "sql.y"
            { (yyval.compare_type) = O_LE; }
#line 3643 "y.tab.c"
    break;

  case 156: /* function: MAX '(' non_all_function_value ')'  */
#line 1271 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MAX;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3654 "y.tab.c"
    break;

  case 157: /* function: MIN '(' non_all_function_value ')'  */
#line 1278 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MIN;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3665 "y.tab.c"
    break;

  case 158: /* function: COUNT '(' function_value ')'  */
#line 1285 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_COUNT;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3676 "y.tab.c"
    break;

  case 159: /* function: SUM '(' function_value ')'  */
#line 1292 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_SUM;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3687 "y.tab.c"
    break;

  case 160: /* function: AVG '(' function_value ')'  */
#line 1299 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_AVG;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3698 "y.tab.c"
    break;

  case 161: /* function_value: INTVALUE  */
#line 1308 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3709 "y.tab.c"
    break;

  case 162: /* function_value: column  */
#line 1315 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3720 "y.tab.c"
    break;

  case 163: /* function_value: '*'  */
#line 1322 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->value_type = V_ALL;
            (yyval.function_value_node) = node;
        }
#line 3730 "y.tab.c"
    break;

  case 164: /* non_all_function_value: INTVALUE  */
#line 1330 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3741 "y.tab.c"
    break;

  case 165: /* non_all_function_value: column  */
#line 1337 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3752 "y.tab.c"
    break;


#line 3756 "y.tab.c"

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

#line 1348 "sql.y"


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
