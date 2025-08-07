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
int yylex();
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
#define YYLAST   380

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  90
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  68
/* YYNRULES -- Number of rules.  */
#define YYNRULES  167
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  319

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

#define YYPACT_NINF (-221)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-113)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     299,   -70,   -70,   -70,     8,    24,    85,    15,   -26,    39,
     -26,    69,    40,   268,  -221,  -221,  -221,  -221,  -221,  -221,
    -221,  -221,  -221,  -221,  -221,  -221,  -221,    92,  -221,  -221,
    -221,   -26,   -26,  -221,   100,    33,   129,   131,   161,   170,
     176,   178,  -221,  -221,  -221,    71,  -221,  -221,  -221,   111,
      77,    54,  -221,  -221,  -221,  -221,  -221,  -221,  -221,   -26,
    -221,   173,   -26,   -70,   -70,   -26,  -221,  -221,  -221,   210,
     -70,    -1,   288,    -2,   151,    33,     4,  -221,    43,    43,
      26,    26,    26,    94,    25,   180,   -26,   -70,   213,   180,
     180,   180,   180,   180,   159,    91,    25,   -12,  -221,  -221,
      -6,   152,  -221,   156,  -221,  -221,    33,  -221,   160,  -221,
    -221,   233,   234,  -221,  -221,  -221,   236,   237,   238,   127,
      41,   293,   193,   249,  -221,  -221,   184,  -221,  -221,  -221,
    -221,  -221,   -67,   172,  -221,     6,  -221,    94,   225,  -221,
      54,   144,   144,   208,   208,  -221,    25,    85,   251,   -70,
    -221,   203,   -15,  -221,    94,  -221,   194,   196,   -70,  -221,
    -221,   231,   261,   263,   239,  -221,     5,  -221,  -221,   292,
    -221,    25,  -221,   266,  -221,  -221,  -221,  -221,  -221,   199,
    -221,  -221,  -221,  -221,  -221,  -221,  -221,   264,    33,   180,
      94,  -221,    94,   126,  -221,   -26,   216,  -221,  -221,   296,
     219,  -221,    10,  -221,   111,    33,   217,  -221,    33,    25,
     -70,     2,   222,   223,  -221,   315,   223,    94,   316,   -70,
     152,  -221,  -221,  -221,   317,  -221,  -221,  -221,  -221,  -221,
    -221,  -221,   294,  -221,  -221,    33,  -221,    54,   249,  -221,
     158,  -221,  -221,  -221,    68,   135,    25,  -221,    12,   318,
    -221,  -221,  -221,  -221,  -221,   155,   223,    28,  -221,   207,
     223,  -221,  -221,   246,   320,   241,    29,  -221,   247,   248,
     -70,  -221,  -221,    33,   252,   253,  -221,    35,  -221,   223,
    -221,    36,   325,  -221,   324,   302,  -221,   167,   330,   -26,
     257,   305,   272,  -221,  -221,  -221,  -221,  -221,    38,  -221,
    -221,  -221,  -221,   303,  -221,  -221,  -221,  -221,  -221,    94,
    -221,  -221,  -221,  -221,  -221,   -26,   227,  -221,  -221
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
       0,     0,     0,   129,   131,   133,   135,   138,   140,   141,
     142,   111,     0,    41,    42,    44,    21,     0,   146,    50,
      58,    65,    66,    67,    68,    64,     0,     0,     0,     0,
      53,     0,    49,   126,     0,    25,     0,     0,     0,    30,
      31,     0,     0,     0,     0,    78,     0,    71,    73,     0,
      74,     0,   113,     0,   156,   157,   158,   159,   160,     0,
     134,   150,   151,   152,   153,   154,   155,     0,     0,     0,
       0,   123,     0,     0,   110,     0,     0,    48,    45,    51,
       0,    39,     0,    69,    40,     0,    52,    22,     0,     0,
       0,     0,     0,     0,    29,     0,     0,     0,     0,     0,
       0,    81,    79,    80,     0,    83,    84,    85,    86,    88,
      87,    89,    90,   109,   139,     0,   144,   143,   130,   132,
       0,   136,    43,    46,   147,     0,     0,    56,     0,     0,
     128,   127,    24,    26,    33,    34,     0,     0,    76,     0,
       0,    19,    72,     0,     0,    93,     0,   137,     0,     0,
       0,    70,    54,     0,     0,     0,    32,     0,   104,     0,
     107,     0,     0,    91,     0,     0,    97,     0,     0,     0,
       0,     0,    75,    94,   145,   149,   148,    23,     0,    35,
      36,   105,    77,     0,    82,    92,    98,   100,    99,     0,
     103,   101,    96,    95,    55,     0,     0,   106,   102
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -221,  -221,   350,  -221,  -221,  -221,  -221,  -221,  -221,  -221,
    -221,  -221,  -221,  -221,  -221,  -221,  -221,  -221,  -221,   218,
     162,  -221,  -221,   169,    -8,   171,   220,  -221,   123,  -221,
    -221,   284,   -17,  -221,  -221,  -221,   150,   163,  -220,   -61,
    -221,  -221,  -221,    82,  -221,   -53,   -34,   -28,  -221,  -221,
    -182,  -221,   166,  -105,   187,   186,   259,  -221,  -221,  -221,
    -221,  -221,  -221,  -221,  -221,   153,   301,     1
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,   158,   159,   160,   276,    49,
      87,    88,   133,   134,   135,   198,   138,   139,   149,   206,
     150,    50,    51,    52,   202,   166,   167,   168,   257,   169,
     232,   265,   292,   293,   170,    53,    73,    54,    55,    56,
      57,   152,   153,   122,   123,   124,   125,   126,   127,   128,
     129,   130,   201,   189,    58,   116,   111,    28
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      61,    76,    63,    29,    30,   190,    74,    77,   105,   103,
     137,   241,   156,   154,   179,   219,   107,    72,    89,    27,
     245,   194,   272,    69,    70,   110,   110,   115,   115,   115,
     121,   131,   199,   113,   108,   108,   277,    31,   278,   294,
     281,    59,    75,   151,    35,   301,   303,    77,   314,   211,
     119,    95,   108,    32,    97,    60,   196,   100,   267,    90,
      91,    92,    93,    62,    98,    99,   121,   121,   157,    65,
     209,   102,   140,   141,   142,   143,   144,    27,   172,    41,
      42,    43,    44,   106,   121,    84,    85,   197,   136,   106,
     220,    27,    33,   203,    34,   246,    35,   106,   155,   268,
     146,   121,    64,   119,    94,    68,    45,    45,   114,    34,
     147,    35,   259,   279,   106,    46,    47,    48,   233,   148,
     279,   279,    45,   106,    45,   109,    36,    37,    38,    39,
      40,    41,    42,    43,    44,    86,   119,   121,    78,   121,
      79,    36,    37,    38,    39,    40,    41,    42,    43,    44,
     207,    92,    93,   269,   147,   258,   151,    84,    85,   214,
     236,   231,    89,   148,   121,   120,    45,    46,    47,    48,
      80,   248,   237,    42,    43,    45,    75,    77,    35,    81,
     250,    71,    46,    47,    48,    82,   161,    83,   162,    34,
     163,    35,   164,   271,    94,   258,   190,   240,   120,   258,
      96,   266,   190,   191,   316,    42,    43,    77,    71,   234,
     190,   252,   253,    41,    42,    43,   307,   280,   302,   101,
     261,    36,    37,    38,    39,    40,    41,    42,    43,    44,
     190,   274,   275,   165,   117,   118,  -112,   318,   137,   298,
     145,   173,   171,   174,   175,    77,   176,   177,   178,    46,
      47,    48,   284,   192,   193,   200,   121,   195,    94,   308,
     205,    45,    46,    47,    48,   208,   215,   212,    66,   213,
     216,   297,   217,   235,   218,   285,   103,   286,   287,   288,
     289,   310,     1,     2,     3,     4,     5,     6,     7,     8,
       9,    10,   290,    90,    91,    92,    93,   197,   104,   190,
      11,   244,   249,   254,   165,   264,   285,   317,   286,   287,
     288,   289,   291,     1,     2,     3,     4,     5,     6,     7,
       8,     9,    10,   290,   256,   260,   263,   273,   282,   295,
     296,    11,   283,   299,   300,   304,   305,   306,    94,   309,
      12,   311,   315,   291,   221,   222,   223,   224,   225,   226,
     227,   228,   229,   230,   312,   181,   182,   183,   184,   185,
     186,   187,   188,    67,   242,   204,   247,   243,   270,   132,
     262,    12,   210,    60,   313,   251,   255,   238,   239,   180,
     112
};

static const yytype_int16 yycheck[] =
{
       8,    35,    10,     2,     3,     3,    34,    35,    10,    10,
      25,   193,    18,    25,   119,    10,    12,    34,    85,    89,
      10,    88,    10,    31,    32,    78,    79,    80,    81,    82,
      83,    84,   137,     7,     9,     9,   256,    29,    10,    10,
     260,    26,     9,    96,    11,    10,    10,    75,    10,   154,
       9,    59,     9,    29,    62,    81,    50,    65,   240,     5,
       6,     7,     8,    24,    63,    64,   119,   120,    74,    29,
      85,    70,    89,    90,    91,    92,    93,    89,   106,    46,
      47,    48,    49,    85,   137,    86,    87,    81,    87,    85,
      85,    89,     7,   146,     9,    85,    11,    85,    97,    31,
       9,   154,    33,     9,    50,    13,    81,    81,    82,     9,
      19,    11,   217,    85,    85,    82,    83,    84,   171,    28,
      85,    85,    81,    85,    81,    82,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    24,     9,   190,     9,   192,
       9,    41,    42,    43,    44,    45,    46,    47,    48,    49,
     149,     7,     8,    85,    19,   216,   209,    86,    87,   158,
     188,   169,    85,    28,   217,    71,    81,    82,    83,    84,
       9,   205,   189,    47,    48,    81,     9,   205,    11,     9,
     208,    81,    82,    83,    84,     9,    34,     9,    36,     9,
      38,    11,    40,   246,    50,   256,     3,    71,    71,   260,
      27,   235,     3,    10,   309,    47,    48,   235,    81,    10,
       3,   210,   211,    46,    47,    48,    49,    10,   279,     9,
     219,    41,    42,    43,    44,    45,    46,    47,    48,    49,
       3,    76,    77,    81,    81,    82,    85,    10,    25,   273,
      81,    81,    86,    10,    10,   273,    10,    10,    10,    82,
      83,    84,    11,     4,    70,    30,   309,    85,    50,   287,
       9,    81,    82,    83,    84,    62,    35,    73,     0,    73,
       9,   270,     9,     9,    35,    34,    10,    36,    37,    38,
      39,   289,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    51,     5,     6,     7,     8,    81,    10,     3,
      32,    82,    85,    81,    81,    11,    34,   315,    36,    37,
      38,    39,    71,    14,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    51,     9,     9,     9,     9,    82,    82,
      82,    32,    12,    81,    81,    10,    12,    35,    50,     9,
      72,    84,    39,    71,    52,    53,    54,    55,    56,    57,
      58,    59,    60,    61,    49,    62,    63,    64,    65,    66,
      67,    68,    69,    13,   195,   147,   204,   196,   245,    85,
     220,    72,   152,    81,   292,   209,   213,   190,   192,   120,
      79
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
      71,   135,   143,   144,   145,   146,   147,   148,   149,   150,
     151,   135,   121,   112,   113,   114,   157,    25,   116,   117,
     122,   122,   122,   122,   122,    81,     9,    19,    28,   118,
     120,   135,   141,   142,    25,   157,    18,    74,   105,   106,
     107,    34,    36,    38,    40,    81,   125,   126,   127,   129,
     134,    86,   137,    81,    10,    10,    10,    10,    10,   143,
     146,    62,    63,    64,    65,    66,    67,    68,    69,   153,
       3,    10,     4,    70,    88,    85,    50,    81,   115,   143,
      30,   152,   124,   135,   109,     9,   119,   157,    62,    85,
     116,   143,    73,    73,   157,    35,     9,     9,    35,    10,
      85,    52,    53,    54,    55,    56,    57,    58,    59,    60,
      61,   114,   130,   135,    10,     9,   137,   122,   144,   145,
      71,   140,   113,   115,    82,    10,    85,   110,   136,    85,
     137,   142,   157,   157,    81,   127,     9,   128,   129,   143,
       9,   157,   126,     9,    11,   131,   136,   140,    31,    85,
     118,   135,    10,     9,    76,    77,   108,   128,    10,    85,
      10,   128,    82,    12,    11,    34,    36,    37,    38,    39,
      51,    71,   132,   133,    10,    82,    82,   157,   136,    81,
      81,    10,   129,    10,    10,    12,    35,    49,   137,     9,
     114,    84,    49,   133,    10,    39,   143,   114,    10
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
#line 2094 "y.tab.c"
    break;

  case 3: /* statements: statements statement  */
#line 191 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].statement));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 2103 "y.tab.c"
    break;

  case 4: /* statement: begin_transaction_statement  */
#line 198 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = BEGIN_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2113 "y.tab.c"
    break;

  case 5: /* statement: commit_transaction_statement  */
#line 204 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = COMMIT_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2123 "y.tab.c"
    break;

  case 6: /* statement: rollback_transaction_statement  */
#line 210 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ROLLBACK_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2133 "y.tab.c"
    break;

  case 7: /* statement: create_table_statement  */
#line 216 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_TABLE_STMT;
            statement->create_table_node = (yyvsp[0].create_table_node);
            (yyval.statement) = statement;
        }
#line 2144 "y.tab.c"
    break;

  case 8: /* statement: drop_table_statement  */
#line 223 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_TABLE_STMT;
            statement->drop_table_node = (yyvsp[0].drop_table_node);
            (yyval.statement) = statement;
        }
#line 2155 "y.tab.c"
    break;

  case 9: /* statement: select_statement  */
#line 230 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SELECT_STMT;
            statement->select_node = (yyvsp[0].select_node);
            (yyval.statement) = statement;
        }
#line 2166 "y.tab.c"
    break;

  case 10: /* statement: insert_statement  */
#line 237 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = INSERT_STMT;
            statement->insert_node = (yyvsp[0].insert_node);
            (yyval.statement) = statement;
        }
#line 2177 "y.tab.c"
    break;

  case 11: /* statement: update_statement  */
#line 244 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = UPDATE_STMT;
            statement->update_node = (yyvsp[0].update_node);
            (yyval.statement) = statement;
        }
#line 2188 "y.tab.c"
    break;

  case 12: /* statement: delete_statement  */
#line 251 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DELETE_STMT;
            statement->delete_node = (yyvsp[0].delete_node);
            (yyval.statement) = statement;
        }
#line 2199 "y.tab.c"
    break;

  case 13: /* statement: describe_statement  */
#line 258 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DESCRIBE_STMT;
            statement->describe_node = (yyvsp[0].describe_node);
            (yyval.statement) = statement;
        }
#line 2210 "y.tab.c"
    break;

  case 14: /* statement: show_statement  */
#line 265 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SHOW_STMT;
            statement->show_node = (yyvsp[0].show_node);
            (yyval.statement) = statement;
        }
#line 2221 "y.tab.c"
    break;

  case 15: /* statement: alter_table_statement  */
#line 272 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ALTER_TABLE_STMT;
            statement->alter_table_node = (yyvsp[0].alter_table_node);
            (yyval.statement) = statement;
        }
#line 2232 "y.tab.c"
    break;

  case 19: /* create_table_statement: CREATE TABLE table '(' base_table_element_commalist ')' end  */
#line 291 "sql.y"
        {
            CreateTableNode *create_table_node = instance(CreateTableNode);
            create_table_node->table_name = (yyvsp[-4].strVal);
            create_table_node->base_table_element_commalist = (yyvsp[-2].list);
            (yyval.create_table_node) = create_table_node;
        }
#line 2243 "y.tab.c"
    break;

  case 20: /* drop_table_statement: DROP TABLE table end  */
#line 301 "sql.y"
        {
            DropTableNode *drop_table_node = instance(DropTableNode);
            drop_table_node->table_name = (yyvsp[-1].strVal);
            (yyval.drop_table_node) = drop_table_node;
        }
#line 2253 "y.tab.c"
    break;

  case 21: /* select_statement: SELECT selection table_exp end  */
#line 310 "sql.y"
        {
            SelectNode *select_node = instance(SelectNode);
            select_node->selection = (yyvsp[-2].selection_node);
            select_node->table_exp = (yyvsp[-1].table_exp_node);
            (yyval.select_node) = select_node;
        }
#line 2264 "y.tab.c"
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
#line 2276 "y.tab.c"
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
#line 2289 "y.tab.c"
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
#line 2301 "y.tab.c"
    break;

  case 25: /* delete_statement: DELETE FROM table end  */
#line 351 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.delete_node) = node;
        }
#line 2311 "y.tab.c"
    break;

  case 26: /* delete_statement: DELETE FROM table WHERE search_condition end  */
#line 357 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-3].strVal);
            node->condition_node = (yyvsp[-1].search_condition_node);
            (yyval.delete_node) = node;
        }
#line 2322 "y.tab.c"
    break;

  case 27: /* describe_statement: DESCRIBE table end  */
#line 367 "sql.y"
        {
            DescribeNode *node = instance(DescribeNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.describe_node) = node;
        }
#line 2332 "y.tab.c"
    break;

  case 28: /* show_statement: SHOW TABLES end  */
#line 376 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_TABLES;
            (yyval.show_node) = node;
        }
#line 2342 "y.tab.c"
    break;

  case 29: /* alter_table_statement: ALTER TABLE table alter_table_action end  */
#line 385 "sql.y"
        {
            (yyval.alter_table_node) = instance(AlterTableNode);
            (yyval.alter_table_node)->table_name = (yyvsp[-2].strVal);
            (yyval.alter_table_node)->action = (yyvsp[-1].alter_table_action);
        }
#line 2352 "y.tab.c"
    break;

  case 30: /* alter_table_action: add_column_def  */
#line 393 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_ADD_COLUMN;
            action->action.add_column = (yyvsp[0].add_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2363 "y.tab.c"
    break;

  case 31: /* alter_table_action: drop_column_def  */
#line 400 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_DROP_COLUMN;
            action->action.drop_column = (yyvsp[0].drop_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2374 "y.tab.c"
    break;

  case 32: /* add_column_def: ADD COLUMN column_def column_position_def  */
#line 409 "sql.y"
        {
            AddColumnDef *node = instance(AddColumnDef);
            node->column_def = (yyvsp[-1].column_def_node);
            node->position_def = (yyvsp[0].column_position_def);
            (yyval.add_column_def) = node;
        }
#line 2385 "y.tab.c"
    break;

  case 33: /* drop_column_def: DROP COLUMN IDENTIFIER  */
#line 418 "sql.y"
        {
            DropColumnDef *node = instance(DropColumnDef);
            node->column_name = (yyvsp[0].strVal);
            (yyval.drop_column_def) = node;
        }
#line 2395 "y.tab.c"
    break;

  case 34: /* column_position_def: %empty  */
#line 426 "sql.y"
    {
        (yyval.column_position_def) = NULL;
    }
#line 2403 "y.tab.c"
    break;

  case 35: /* column_position_def: BEFORE IDENTIFIER  */
#line 430 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_BEFORE;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2414 "y.tab.c"
    break;

  case 36: /* column_position_def: AFTER IDENTIFIER  */
#line 437 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_AFTER;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2425 "y.tab.c"
    break;

  case 37: /* selection: scalar_exp_commalist  */
#line 446 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = false;
            selection_node->scalar_exp_list = (yyvsp[0].list);
            (yyval.selection_node) = selection_node;
        }
#line 2436 "y.tab.c"
    break;

  case 38: /* selection: '*'  */
#line 453 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = true;
            (yyval.selection_node) = selection_node;
        }
#line 2446 "y.tab.c"
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
#line 2458 "y.tab.c"
    break;

  case 40: /* from_clause: %empty  */
#line 471 "sql.y"
        {
            (yyval.from_clause_node) = NULL;
        }
#line 2466 "y.tab.c"
    break;

  case 41: /* from_clause: FROM table_ref_commalist  */
#line 475 "sql.y"
        {
            FromClauseNode *from_clause = instance(FromClauseNode);
            from_clause->from = (yyvsp[0].list);
            (yyval.from_clause_node) = from_clause;
        }
#line 2476 "y.tab.c"
    break;

  case 42: /* table_ref_commalist: table_ref  */
#line 483 "sql.y"
        {
            List *list = create_list(NODE_TABLE_REFER);
            append_list(list, (yyvsp[0].table_ref_node));
            (yyval.list) = list;
        }
#line 2486 "y.tab.c"
    break;

  case 43: /* table_ref_commalist: table_ref_commalist ',' table_ref  */
#line 489 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].table_ref_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2495 "y.tab.c"
    break;

  case 44: /* table_ref: table  */
#line 496 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2505 "y.tab.c"
    break;

  case 45: /* table_ref: table range_variable  */
#line 502 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-1].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2516 "y.tab.c"
    break;

  case 46: /* table_ref: table AS range_variable  */
#line 509 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-2].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2527 "y.tab.c"
    break;

  case 47: /* table: IDENTIFIER  */
#line 518 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2535 "y.tab.c"
    break;

  case 48: /* range_variable: IDENTIFIER  */
#line 524 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2543 "y.tab.c"
    break;

  case 49: /* opt_where_clause: %empty  */
#line 530 "sql.y"
        {
            (yyval.where_clause_node) = NULL;
        }
#line 2551 "y.tab.c"
    break;

  case 50: /* opt_where_clause: where_clause  */
#line 534 "sql.y"
        {
            (yyval.where_clause_node) = (yyvsp[0].where_clause_node);
        }
#line 2559 "y.tab.c"
    break;

  case 51: /* where_clause: WHERE search_condition  */
#line 540 "sql.y"
        {
            WhereClauseNode *where_clause_node = instance(WhereClauseNode);
            where_clause_node->condition = (yyvsp[0].search_condition_node);
            (yyval.where_clause_node) = where_clause_node;
        }
#line 2569 "y.tab.c"
    break;

  case 52: /* values_or_query_spec: VALUES opt_values  */
#line 548 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_VALUES;
            values_or_query_spec->values = (yyvsp[0].list);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2580 "y.tab.c"
    break;

  case 53: /* values_or_query_spec: query_spec  */
#line 555 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_QUERY_SPEC;
            values_or_query_spec->query_spec = (yyvsp[0].query_spec_node);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2591 "y.tab.c"
    break;

  case 54: /* opt_values: '(' value_items ')'  */
#line 564 "sql.y"
        {
            (yyval.list) = create_list(NODE_LIST);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2600 "y.tab.c"
    break;

  case 55: /* opt_values: opt_values ',' '(' value_items ')'  */
#line 569 "sql.y"
        {
            (yyval.list) = (yyvsp[-4].list);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2609 "y.tab.c"
    break;

  case 56: /* query_spec: SELECT selection table_exp  */
#line 576 "sql.y"
        {
            QuerySpecNode *query_spec = instance(QuerySpecNode);
            query_spec->selection = (yyvsp[-1].selection_node);
            query_spec->table_exp = (yyvsp[0].table_exp_node);
            (yyval.query_spec_node) = query_spec;
        }
#line 2620 "y.tab.c"
    break;

  case 57: /* scalar_exp_commalist: scalar_exp  */
#line 585 "sql.y"
        {
            List *scalar_exp_list = create_list(NODE_SCALAR_EXP);
            append_list(scalar_exp_list, (yyvsp[0].scalar_exp_node));
            (yyval.list) = scalar_exp_list;
        }
#line 2630 "y.tab.c"
    break;

  case 58: /* scalar_exp_commalist: scalar_exp_commalist ',' scalar_exp  */
#line 591 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].scalar_exp_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2639 "y.tab.c"
    break;

  case 59: /* scalar_exp: calculate  */
#line 598 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_CALCULATE;
            scalar_exp_node->calculate = (yyvsp[0].calculate_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2650 "y.tab.c"
    break;

  case 60: /* scalar_exp: column  */
#line 605 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_COLUMN;
            scalar_exp_node->column = (yyvsp[0].column_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2661 "y.tab.c"
    break;

  case 61: /* scalar_exp: function  */
#line 612 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_FUNCTION;
            scalar_exp_node->function = (yyvsp[0].function_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2672 "y.tab.c"
    break;

  case 62: /* scalar_exp: value_item  */
#line 619 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_VALUE;
            scalar_exp_node->value = (yyvsp[0].value_item_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2683 "y.tab.c"
    break;

  case 63: /* scalar_exp: '(' scalar_exp ')'  */
#line 626 "sql.y"
        {
            (yyval.scalar_exp_node) = (yyvsp[-1].scalar_exp_node);
        }
#line 2691 "y.tab.c"
    break;

  case 64: /* scalar_exp: scalar_exp AS IDENTIFIER  */
#line 630 "sql.y"
        {
            (yyvsp[-2].scalar_exp_node)->alias = (yyvsp[0].strVal);
            (yyval.scalar_exp_node) = (yyvsp[-2].scalar_exp_node);
        }
#line 2700 "y.tab.c"
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
#line 2712 "y.tab.c"
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
#line 2724 "y.tab.c"
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
#line 2736 "y.tab.c"
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
#line 2748 "y.tab.c"
    break;

  case 69: /* columns: column  */
#line 671 "sql.y"
        {
            List *column_set_node = create_list(NODE_COLUMN);
            append_list(column_set_node, (yyvsp[0].column_node));
            (yyval.list) = column_set_node;
        }
#line 2758 "y.tab.c"
    break;

  case 70: /* columns: columns ',' column  */
#line 677 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].column_node));
        }
#line 2767 "y.tab.c"
    break;

  case 71: /* base_table_element_commalist: base_table_element  */
#line 684 "sql.y"
        {
            List *base_table_element_commalist = create_list(NODE_BASE_TABLE_ELEMENT);
            append_list(base_table_element_commalist, (yyvsp[0].base_table_element));
            (yyval.list) = base_table_element_commalist;
        }
#line 2777 "y.tab.c"
    break;

  case 72: /* base_table_element_commalist: base_table_element_commalist ',' base_table_element  */
#line 690 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].base_table_element));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2786 "y.tab.c"
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
#line 2798 "y.tab.c"
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
#line 2810 "y.tab.c"
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
#line 2823 "y.tab.c"
    break;

  case 76: /* column_def_name_commalist: column_def_name  */
#line 739 "sql.y"
        {
            List *list = create_list(NODE_COLUMN_DEF_NAME);
            append_list(list, (yyvsp[0].column_def_name));
            (yyval.list) = list;
        }
#line 2833 "y.tab.c"
    break;

  case 77: /* column_def_name_commalist: column_def_name_commalist ',' column_def_name  */
#line 745 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].column_def_name));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2842 "y.tab.c"
    break;

  case 78: /* column_def_name: IDENTIFIER  */
#line 752 "sql.y"
        {
            ColumnDefName *column_def_name = instance(ColumnDefName);
            column_def_name->column = (yyvsp[0].strVal);
            (yyval.column_def_name) = column_def_name;
        }
#line 2852 "y.tab.c"
    break;

  case 79: /* data_type: INT  */
#line 760 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_INT; 
            (yyval.data_type_node) = node;
        }
#line 2862 "y.tab.c"
    break;

  case 80: /* data_type: LONG  */
#line 766 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_LONG;  
            (yyval.data_type_node) = node;
        }
#line 2872 "y.tab.c"
    break;

  case 81: /* data_type: CHAR  */
#line 772 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_CHAR; 
            (yyval.data_type_node) = node;
        }
#line 2882 "y.tab.c"
    break;

  case 82: /* data_type: VARCHAR '(' INTVALUE ')'  */
#line 778 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_VARCHAR; 
            node->len = (yyvsp[-1].intVal);
            (yyval.data_type_node) = node;
        }
#line 2893 "y.tab.c"
    break;

  case 83: /* data_type: STRING  */
#line 785 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_STRING; 
            (yyval.data_type_node) = node;
        }
#line 2903 "y.tab.c"
    break;

  case 84: /* data_type: BOOL  */
#line 791 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_BOOL; 
            (yyval.data_type_node) = node;
        }
#line 2913 "y.tab.c"
    break;

  case 85: /* data_type: FLOAT  */
#line 797 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_FLOAT; 
            (yyval.data_type_node) = node;
        }
#line 2923 "y.tab.c"
    break;

  case 86: /* data_type: DOUBLE  */
#line 803 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DOUBLE; 
            (yyval.data_type_node) = node;
        }
#line 2933 "y.tab.c"
    break;

  case 87: /* data_type: TIMESTAMP  */
#line 809 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_TIMESTAMP; 
            (yyval.data_type_node) = node;
        }
#line 2943 "y.tab.c"
    break;

  case 88: /* data_type: DATE  */
#line 815 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DATE; 
            (yyval.data_type_node) = node;
        }
#line 2953 "y.tab.c"
    break;

  case 89: /* data_type: table  */
#line 821 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_REFERENCE;
            node->table_name = (yyvsp[0].strVal);
            (yyval.data_type_node) = node;
        }
#line 2964 "y.tab.c"
    break;

  case 90: /* array_dim_clause: %empty  */
#line 830 "sql.y"
        {
            (yyval.intVal) = 0;
        }
#line 2972 "y.tab.c"
    break;

  case 91: /* array_dim_clause: '[' ']'  */
#line 834 "sql.y"
        {
            (yyval.intVal) = 1;
        }
#line 2980 "y.tab.c"
    break;

  case 92: /* array_dim_clause: array_dim_clause '[' ']'  */
#line 838 "sql.y"
        {
            (yyval.intVal)++;
        }
#line 2988 "y.tab.c"
    break;

  case 93: /* column_def_opt_list: %empty  */
#line 844 "sql.y"
        {
            (yyval.list) = NULL;
        }
#line 2996 "y.tab.c"
    break;

  case 94: /* column_def_opt_list: column_def_opt  */
#line 848 "sql.y"
        {
            (yyval.list) = create_list(NODE_COLUMN_DEF_OPT);
            append_list((yyval.list), (yyvsp[0].column_def_opt));
        }
#line 3005 "y.tab.c"
    break;

  case 95: /* column_def_opt_list: column_def_opt_list column_def_opt  */
#line 853 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].column_def_opt));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 3014 "y.tab.c"
    break;

  case 96: /* column_def_opt: NOT NULLX  */
#line 860 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_NOT_NULL; 
            (yyval.column_def_opt) = node;
        }
#line 3024 "y.tab.c"
    break;

  case 97: /* column_def_opt: UNIQUE  */
#line 866 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_UNIQUE; 
            (yyval.column_def_opt) = node;
        }
#line 3034 "y.tab.c"
    break;

  case 98: /* column_def_opt: PRIMARY KEY  */
#line 872 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_PRIMARY_KEY; 
            (yyval.column_def_opt) = node;
        }
#line 3044 "y.tab.c"
    break;

  case 99: /* column_def_opt: DEFAULT value_item  */
#line 878 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_VALUE;
            node->value = (yyvsp[0].value_item_node);
            (yyval.column_def_opt) = node;
        }
#line 3055 "y.tab.c"
    break;

  case 100: /* column_def_opt: DEFAULT NULLX  */
#line 885 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_NULL;
            (yyval.column_def_opt) = node;
        }
#line 3065 "y.tab.c"
    break;

  case 101: /* column_def_opt: COMMENT STRINGVALUE  */
#line 891 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_COMMENT;
            node->comment = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3076 "y.tab.c"
    break;

  case 102: /* column_def_opt: CHECK '(' search_condition ')'  */
#line 898 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_CHECK_CONDITION;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.column_def_opt) = node;
        }
#line 3087 "y.tab.c"
    break;

  case 103: /* column_def_opt: REFERENCES table  */
#line 905 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_REFERENECS;
            node->refer_table = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3098 "y.tab.c"
    break;

  case 104: /* table_contraint_def: UNIQUE '(' column_def_name_commalist ')'  */
#line 914 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_UNIQUE;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3109 "y.tab.c"
    break;

  case 105: /* table_contraint_def: PRIMARY KEY '(' column_def_name_commalist ')'  */
#line 921 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_PRIMARY_KEY;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3120 "y.tab.c"
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
#line 3132 "y.tab.c"
    break;

  case 107: /* table_contraint_def: CHECK '(' search_condition ')'  */
#line 936 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_CHECK;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.table_contraint_def) = node;
        }
#line 3143 "y.tab.c"
    break;

  case 108: /* column: IDENTIFIER  */
#line 945 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[0].strVal);
            column_node->has_sub_column = false;
            (yyval.column_node) = column_node;
        }
#line 3154 "y.tab.c"
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
#line 3166 "y.tab.c"
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
#line 3178 "y.tab.c"
    break;

  case 111: /* column: IDENTIFIER '.' column  */
#line 968 "sql.y"
        {
            (yyval.column_node) = (yyvsp[0].column_node);
            (yyval.column_node)->range_variable = (yyvsp[-2].strVal);
        }
#line 3187 "y.tab.c"
    break;

  case 112: /* value_items: value_item  */
#line 975 "sql.y"
        {
            List *value_list = create_list(NODE_VALUE_ITEM);
            append_list(value_list, (yyvsp[0].value_item_node));
            (yyval.list) = value_list;
        }
#line 3197 "y.tab.c"
    break;

  case 113: /* value_items: value_items ',' value_item  */
#line 981 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].value_item_node));
        }
#line 3206 "y.tab.c"
    break;

  case 114: /* value_item: atom  */
#line 988 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ATOM;
            node->value.atom = (yyvsp[0].atom_node);
            (yyval.value_item_node) = node;
        }
#line 3217 "y.tab.c"
    break;

  case 115: /* value_item: NULLX  */
#line 995 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_NULL;
            (yyval.value_item_node) = node;
        }
#line 3227 "y.tab.c"
    break;

  case 116: /* value_item: '[' value_items ']'  */
#line 1001 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ARRAY;
            node->value.value_list = (yyvsp[-1].list);
            (yyval.value_item_node) = node;
        }
#line 3238 "y.tab.c"
    break;

  case 117: /* atom: INTVALUE  */
#line 1010 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.intval = (yyvsp[0].intVal);
            node->type = A_INT;
            (yyval.atom_node) = node;
        }
#line 3249 "y.tab.c"
    break;

  case 118: /* atom: BOOLVALUE  */
#line 1017 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.boolval = (yyvsp[0].boolVal);
            node->type = A_BOOL;
            (yyval.atom_node) = node;
        }
#line 3260 "y.tab.c"
    break;

  case 119: /* atom: STRINGVALUE  */
#line 1024 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.strval = (yyvsp[0].strVal);
            node->type = A_STRING;
            (yyval.atom_node) = node;
        }
#line 3271 "y.tab.c"
    break;

  case 120: /* atom: FLOATVALUE  */
#line 1031 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.floatval = (yyvsp[0].floatVal);
            node->type = A_FLOAT;
            (yyval.atom_node) = node;
        }
#line 3282 "y.tab.c"
    break;

  case 121: /* atom: REFERVALUE  */
#line 1038 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.referval = (yyvsp[0].referVal);
            node->type = A_REFERENCE;
            (yyval.atom_node) = node;
        }
#line 3293 "y.tab.c"
    break;

  case 122: /* REFERVALUE: '(' value_items ')'  */
#line 1048 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = DIRECTLY;
            refer->nest_value_list = (yyvsp[-1].list);
            (yyval.referVal) = refer;
        }
#line 3304 "y.tab.c"
    break;

  case 123: /* REFERVALUE: REF '(' search_condition ')'  */
#line 1056 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = INDIRECTLY;
            refer->condition = (yyvsp[-1].search_condition_node);
            (yyval.referVal) = refer;
        }
#line 3315 "y.tab.c"
    break;

  case 124: /* BOOLVALUE: TRUE  */
#line 1065 "sql.y"
        {
            (yyval.boolVal) = true;
        }
#line 3323 "y.tab.c"
    break;

  case 125: /* BOOLVALUE: FALSE  */
#line 1069 "sql.y"
        {
            (yyval.boolVal) = false;
        }
#line 3331 "y.tab.c"
    break;

  case 126: /* assignments: assignment  */
#line 1075 "sql.y"
        {
            List *list = create_list(NODE_ASSIGNMENT);
            append_list(list, (yyvsp[0].assignment_node));
            (yyval.list) = list;
        }
#line 3341 "y.tab.c"
    break;

  case 127: /* assignments: assignments ',' assignment  */
#line 1081 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].assignment_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 3350 "y.tab.c"
    break;

  case 128: /* assignment: column EQ value_item  */
#line 1088 "sql.y"
        {
            AssignmentNode *node = instance(AssignmentNode);
            node->column = (yyvsp[-2].column_node);
            node->value = (yyvsp[0].value_item_node);
            (yyval.assignment_node) = node;
        }
#line 3361 "y.tab.c"
    break;

  case 129: /* search_condition: boolean_term  */
#line 1097 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3371 "y.tab.c"
    break;

  case 130: /* search_condition: search_condition OR boolean_term  */
#line 1103 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->or_search_condition = (yyvsp[-2].search_condition_node);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3382 "y.tab.c"
    break;

  case 131: /* boolean_term: boolean_factor  */
#line 1112 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3392 "y.tab.c"
    break;

  case 132: /* boolean_term: boolean_term AND boolean_factor  */
#line 1118 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->and_boolean_term = (yyvsp[-2].boolean_term_node);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3403 "y.tab.c"
    break;

  case 133: /* boolean_factor: boolean_test  */
#line 1127 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = false;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3414 "y.tab.c"
    break;

  case 134: /* boolean_factor: NOT boolean_test  */
#line 1134 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = true;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3425 "y.tab.c"
    break;

  case 135: /* boolean_test: boolean_primary  */
#line 1143 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[0].boolean_primary_node);
            test_node->type = NONE_TRUE_VALUE;
            (yyval.boolean_test_node) = test_node;
        }
#line 3436 "y.tab.c"
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
#line 3448 "y.tab.c"
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
#line 3460 "y.tab.c"
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
#line 3472 "y.tab.c"
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
#line 3484 "y.tab.c"
    break;

  case 140: /* predicate: comparison_predicate  */
#line 1185 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_COMPARISON;
            predicate->comparison = (yyvsp[0].comparison_node);
            (yyval.predicate_node) = predicate;
        }
#line 3495 "y.tab.c"
    break;

  case 141: /* predicate: like_predicate  */
#line 1192 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_LIKE;
            predicate->like = (yyvsp[0].like_node);
            (yyval.predicate_node) = predicate;
        }
#line 3506 "y.tab.c"
    break;

  case 142: /* predicate: in_predicate  */
#line 1199 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_IN;
            predicate->in = (yyvsp[0].in_node);
            (yyval.predicate_node) = predicate;
        }
#line 3517 "y.tab.c"
    break;

  case 143: /* comparison_predicate: column compare scalar_exp  */
#line 1208 "sql.y"
        {
            ComparisonNode *comparison_node = instance(ComparisonNode);
            comparison_node->column = (yyvsp[-2].column_node);
            comparison_node->type = (yyvsp[-1].compare_type);
            comparison_node->value = (yyvsp[0].scalar_exp_node);
            (yyval.comparison_node) = comparison_node;
        }
#line 3529 "y.tab.c"
    break;

  case 144: /* like_predicate: column LIKE value_item  */
#line 1218 "sql.y"
        {
            LikeNode *like_node = instance(LikeNode);
            like_node->column = (yyvsp[-2].column_node);
            like_node->value = (yyvsp[0].value_item_node);
            (yyval.like_node) = like_node;
        }
#line 3540 "y.tab.c"
    break;

  case 145: /* in_predicate: column IN '(' value_items ')'  */
#line 1227 "sql.y"
        {
            InNode *in_node = instance(InNode);
            in_node->column = (yyvsp[-4].column_node);
            in_node->value_list = (yyvsp[-1].list);
            (yyval.in_node) = in_node;
        }
#line 3551 "y.tab.c"
    break;

  case 146: /* limit_clause: %empty  */
#line 1236 "sql.y"
        {
            (yyval.limit_clause_node) = NULL;
        }
#line 3559 "y.tab.c"
    break;

  case 147: /* limit_clause: LIMIT INTVALUE  */
#line 1240 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = 0;
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3570 "y.tab.c"
    break;

  case 148: /* limit_clause: LIMIT INTVALUE ',' INTVALUE  */
#line 1247 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = (yyvsp[-2].intVal);
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3581 "y.tab.c"
    break;

  case 149: /* limit_clause: LIMIT INTVALUE OFFSET INTVALUE  */
#line 1254 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->rows = (yyvsp[-2].intVal);
            node->offset = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3592 "y.tab.c"
    break;

  case 150: /* compare: EQ  */
#line 1262 "sql.y"
            { (yyval.compare_type) = O_EQ; }
#line 3598 "y.tab.c"
    break;

  case 151: /* compare: NE  */
#line 1263 "sql.y"
            { (yyval.compare_type) = O_NE; }
#line 3604 "y.tab.c"
    break;

  case 152: /* compare: GT  */
#line 1264 "sql.y"
            { (yyval.compare_type) = O_GT; }
#line 3610 "y.tab.c"
    break;

  case 153: /* compare: GE  */
#line 1265 "sql.y"
            { (yyval.compare_type) = O_GE; }
#line 3616 "y.tab.c"
    break;

  case 154: /* compare: LT  */
#line 1266 "sql.y"
            { (yyval.compare_type) = O_LT; }
#line 3622 "y.tab.c"
    break;

  case 155: /* compare: LE  */
#line 1267 "sql.y"
            { (yyval.compare_type) = O_LE; }
#line 3628 "y.tab.c"
    break;

  case 156: /* function: MAX '(' non_all_function_value ')'  */
#line 1271 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MAX;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3639 "y.tab.c"
    break;

  case 157: /* function: MIN '(' non_all_function_value ')'  */
#line 1278 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MIN;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3650 "y.tab.c"
    break;

  case 158: /* function: COUNT '(' function_value ')'  */
#line 1285 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_COUNT;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3661 "y.tab.c"
    break;

  case 159: /* function: SUM '(' function_value ')'  */
#line 1292 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_SUM;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3672 "y.tab.c"
    break;

  case 160: /* function: AVG '(' function_value ')'  */
#line 1299 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_AVG;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3683 "y.tab.c"
    break;

  case 161: /* function_value: INTVALUE  */
#line 1308 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3694 "y.tab.c"
    break;

  case 162: /* function_value: column  */
#line 1315 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3705 "y.tab.c"
    break;

  case 163: /* function_value: '*'  */
#line 1322 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->value_type = V_ALL;
            (yyval.function_value_node) = node;
        }
#line 3715 "y.tab.c"
    break;

  case 164: /* non_all_function_value: INTVALUE  */
#line 1330 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3726 "y.tab.c"
    break;

  case 165: /* non_all_function_value: column  */
#line 1337 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3737 "y.tab.c"
    break;


#line 3741 "y.tab.c"

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
