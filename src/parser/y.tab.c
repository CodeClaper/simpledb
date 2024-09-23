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
#include "mmu.h"
#include "log.h"
#include "utils.h"
#include "y.tab.h"

int yywrap() {
    return 1;
}
int yylex();

#line 89 "y.tab.c"

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
    SHOW = 278,                    /* SHOW  */
    TABLES = 279,                  /* TABLES  */
    PRIMARY = 280,                 /* PRIMARY  */
    KEY = 281,                     /* KEY  */
    UNIQUE = 282,                  /* UNIQUE  */
    DEFAULT = 283,                 /* DEFAULT  */
    CHECK = 284,                   /* CHECK  */
    REFERENCES = 285,              /* REFERENCES  */
    FOREIGN = 286,                 /* FOREIGN  */
    MAX = 287,                     /* MAX  */
    MIN = 288,                     /* MIN  */
    COUNT = 289,                   /* COUNT  */
    SUM = 290,                     /* SUM  */
    AVG = 291,                     /* AVG  */
    REF = 292,                     /* REF  */
    TRUE = 293,                    /* TRUE  */
    FALSE = 294,                   /* FALSE  */
    NULLX = 295,                   /* NULLX  */
    AS = 296,                      /* AS  */
    COMMENT = 297,                 /* COMMENT  */
    CHAR = 298,                    /* CHAR  */
    INT = 299,                     /* INT  */
    LONG = 300,                    /* LONG  */
    VARCHAR = 301,                 /* VARCHAR  */
    STRING = 302,                  /* STRING  */
    BOOL = 303,                    /* BOOL  */
    FLOAT = 304,                   /* FLOAT  */
    DOUBLE = 305,                  /* DOUBLE  */
    DATE = 306,                    /* DATE  */
    TIMESTAMP = 307,               /* TIMESTAMP  */
    EQ = 308,                      /* EQ  */
    NE = 309,                      /* NE  */
    GT = 310,                      /* GT  */
    GE = 311,                      /* GE  */
    LT = 312,                      /* LT  */
    LE = 313,                      /* LE  */
    IN = 314,                      /* IN  */
    LIKE = 315,                    /* LIKE  */
    NOT = 316,                     /* NOT  */
    ALTER = 317,                   /* ALTER  */
    COLUMN = 318,                  /* COLUMN  */
    ADD = 319,                     /* ADD  */
    RENAME = 320,                  /* RENAME  */
    BEFORE = 321,                  /* BEFORE  */
    AFTER = 322,                   /* AFTER  */
    SYSTEM = 323,                  /* SYSTEM  */
    CONFIG = 324,                  /* CONFIG  */
    MEMORY = 325,                  /* MEMORY  */
    IDENTIFIER = 326,              /* IDENTIFIER  */
    INTVALUE = 327,                /* INTVALUE  */
    FLOATVALUE = 328,              /* FLOATVALUE  */
    STRINGVALUE = 329              /* STRINGVALUE  */
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
#define SHOW 278
#define TABLES 279
#define PRIMARY 280
#define KEY 281
#define UNIQUE 282
#define DEFAULT 283
#define CHECK 284
#define REFERENCES 285
#define FOREIGN 286
#define MAX 287
#define MIN 288
#define COUNT 289
#define SUM 290
#define AVG 291
#define REF 292
#define TRUE 293
#define FALSE 294
#define NULLX 295
#define AS 296
#define COMMENT 297
#define CHAR 298
#define INT 299
#define LONG 300
#define VARCHAR 301
#define STRING 302
#define BOOL 303
#define FLOAT 304
#define DOUBLE 305
#define DATE 306
#define TIMESTAMP 307
#define EQ 308
#define NE 309
#define GT 310
#define GE 311
#define LT 312
#define LE 313
#define IN 314
#define LIKE 315
#define NOT 316
#define ALTER 317
#define COLUMN 318
#define ADD 319
#define RENAME 320
#define BEFORE 321
#define AFTER 322
#define SYSTEM 323
#define CONFIG 324
#define MEMORY 325
#define IDENTIFIER 326
#define INTVALUE 327
#define FLOATVALUE 328
#define STRINGVALUE 329

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 19 "sql.y"

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
   LimitNode                    *limit_node;
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

#line 342 "y.tab.c"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;


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
  YYSYMBOL_SHOW = 31,                      /* SHOW  */
  YYSYMBOL_TABLES = 32,                    /* TABLES  */
  YYSYMBOL_PRIMARY = 33,                   /* PRIMARY  */
  YYSYMBOL_KEY = 34,                       /* KEY  */
  YYSYMBOL_UNIQUE = 35,                    /* UNIQUE  */
  YYSYMBOL_DEFAULT = 36,                   /* DEFAULT  */
  YYSYMBOL_CHECK = 37,                     /* CHECK  */
  YYSYMBOL_REFERENCES = 38,                /* REFERENCES  */
  YYSYMBOL_FOREIGN = 39,                   /* FOREIGN  */
  YYSYMBOL_MAX = 40,                       /* MAX  */
  YYSYMBOL_MIN = 41,                       /* MIN  */
  YYSYMBOL_COUNT = 42,                     /* COUNT  */
  YYSYMBOL_SUM = 43,                       /* SUM  */
  YYSYMBOL_AVG = 44,                       /* AVG  */
  YYSYMBOL_REF = 45,                       /* REF  */
  YYSYMBOL_TRUE = 46,                      /* TRUE  */
  YYSYMBOL_FALSE = 47,                     /* FALSE  */
  YYSYMBOL_NULLX = 48,                     /* NULLX  */
  YYSYMBOL_AS = 49,                        /* AS  */
  YYSYMBOL_COMMENT = 50,                   /* COMMENT  */
  YYSYMBOL_CHAR = 51,                      /* CHAR  */
  YYSYMBOL_INT = 52,                       /* INT  */
  YYSYMBOL_LONG = 53,                      /* LONG  */
  YYSYMBOL_VARCHAR = 54,                   /* VARCHAR  */
  YYSYMBOL_STRING = 55,                    /* STRING  */
  YYSYMBOL_BOOL = 56,                      /* BOOL  */
  YYSYMBOL_FLOAT = 57,                     /* FLOAT  */
  YYSYMBOL_DOUBLE = 58,                    /* DOUBLE  */
  YYSYMBOL_DATE = 59,                      /* DATE  */
  YYSYMBOL_TIMESTAMP = 60,                 /* TIMESTAMP  */
  YYSYMBOL_EQ = 61,                        /* EQ  */
  YYSYMBOL_NE = 62,                        /* NE  */
  YYSYMBOL_GT = 63,                        /* GT  */
  YYSYMBOL_GE = 64,                        /* GE  */
  YYSYMBOL_LT = 65,                        /* LT  */
  YYSYMBOL_LE = 66,                        /* LE  */
  YYSYMBOL_IN = 67,                        /* IN  */
  YYSYMBOL_LIKE = 68,                      /* LIKE  */
  YYSYMBOL_NOT = 69,                       /* NOT  */
  YYSYMBOL_ALTER = 70,                     /* ALTER  */
  YYSYMBOL_COLUMN = 71,                    /* COLUMN  */
  YYSYMBOL_ADD = 72,                       /* ADD  */
  YYSYMBOL_RENAME = 73,                    /* RENAME  */
  YYSYMBOL_BEFORE = 74,                    /* BEFORE  */
  YYSYMBOL_AFTER = 75,                     /* AFTER  */
  YYSYMBOL_SYSTEM = 76,                    /* SYSTEM  */
  YYSYMBOL_CONFIG = 77,                    /* CONFIG  */
  YYSYMBOL_MEMORY = 78,                    /* MEMORY  */
  YYSYMBOL_IDENTIFIER = 79,                /* IDENTIFIER  */
  YYSYMBOL_INTVALUE = 80,                  /* INTVALUE  */
  YYSYMBOL_FLOATVALUE = 81,                /* FLOATVALUE  */
  YYSYMBOL_STRINGVALUE = 82,               /* STRINGVALUE  */
  YYSYMBOL_83_ = 83,                       /* ','  */
  YYSYMBOL_84_ = 84,                       /* '.'  */
  YYSYMBOL_85_ = 85,                       /* '{'  */
  YYSYMBOL_86_ = 86,                       /* '}'  */
  YYSYMBOL_87_ = 87,                       /* ';'  */
  YYSYMBOL_YYACCEPT = 88,                  /* $accept  */
  YYSYMBOL_statements = 89,                /* statements  */
  YYSYMBOL_statement = 90,                 /* statement  */
  YYSYMBOL_begin_transaction_statement = 91, /* begin_transaction_statement  */
  YYSYMBOL_commit_transaction_statement = 92, /* commit_transaction_statement  */
  YYSYMBOL_rollback_transaction_statement = 93, /* rollback_transaction_statement  */
  YYSYMBOL_create_table_statement = 94,    /* create_table_statement  */
  YYSYMBOL_drop_table_statement = 95,      /* drop_table_statement  */
  YYSYMBOL_select_statement = 96,          /* select_statement  */
  YYSYMBOL_insert_statement = 97,          /* insert_statement  */
  YYSYMBOL_update_statement = 98,          /* update_statement  */
  YYSYMBOL_delete_statement = 99,          /* delete_statement  */
  YYSYMBOL_describe_statement = 100,       /* describe_statement  */
  YYSYMBOL_show_statement = 101,           /* show_statement  */
  YYSYMBOL_alter_table_statement = 102,    /* alter_table_statement  */
  YYSYMBOL_alter_table_action = 103,       /* alter_table_action  */
  YYSYMBOL_add_column_def = 104,           /* add_column_def  */
  YYSYMBOL_drop_column_def = 105,          /* drop_column_def  */
  YYSYMBOL_column_position_def = 106,      /* column_position_def  */
  YYSYMBOL_selection = 107,                /* selection  */
  YYSYMBOL_table_exp = 108,                /* table_exp  */
  YYSYMBOL_from_clause = 109,              /* from_clause  */
  YYSYMBOL_table_ref_commalist = 110,      /* table_ref_commalist  */
  YYSYMBOL_table_ref = 111,                /* table_ref  */
  YYSYMBOL_table = 112,                    /* table  */
  YYSYMBOL_range_variable = 113,           /* range_variable  */
  YYSYMBOL_opt_where_clause = 114,         /* opt_where_clause  */
  YYSYMBOL_where_clause = 115,             /* where_clause  */
  YYSYMBOL_values_or_query_spec = 116,     /* values_or_query_spec  */
  YYSYMBOL_opt_values = 117,               /* opt_values  */
  YYSYMBOL_query_spec = 118,               /* query_spec  */
  YYSYMBOL_scalar_exp_commalist = 119,     /* scalar_exp_commalist  */
  YYSYMBOL_scalar_exp = 120,               /* scalar_exp  */
  YYSYMBOL_calculate = 121,                /* calculate  */
  YYSYMBOL_columns = 122,                  /* columns  */
  YYSYMBOL_base_table_element_commalist = 123, /* base_table_element_commalist  */
  YYSYMBOL_base_table_element = 124,       /* base_table_element  */
  YYSYMBOL_column_def = 125,               /* column_def  */
  YYSYMBOL_column_def_name_commalist = 126, /* column_def_name_commalist  */
  YYSYMBOL_column_def_name = 127,          /* column_def_name  */
  YYSYMBOL_data_type = 128,                /* data_type  */
  YYSYMBOL_array_dim_clause = 129,         /* array_dim_clause  */
  YYSYMBOL_column_def_opt_list = 130,      /* column_def_opt_list  */
  YYSYMBOL_column_def_opt = 131,           /* column_def_opt  */
  YYSYMBOL_table_contraint_def = 132,      /* table_contraint_def  */
  YYSYMBOL_column = 133,                   /* column  */
  YYSYMBOL_value_items = 134,              /* value_items  */
  YYSYMBOL_value_item = 135,               /* value_item  */
  YYSYMBOL_atom = 136,                     /* atom  */
  YYSYMBOL_REFERVALUE = 137,               /* REFERVALUE  */
  YYSYMBOL_BOOLVALUE = 138,                /* BOOLVALUE  */
  YYSYMBOL_assignments = 139,              /* assignments  */
  YYSYMBOL_assignment = 140,               /* assignment  */
  YYSYMBOL_condition = 141,                /* condition  */
  YYSYMBOL_predicate = 142,                /* predicate  */
  YYSYMBOL_comparison_predicate = 143,     /* comparison_predicate  */
  YYSYMBOL_like_predicate = 144,           /* like_predicate  */
  YYSYMBOL_in_predicate = 145,             /* in_predicate  */
  YYSYMBOL_compare = 146,                  /* compare  */
  YYSYMBOL_function = 147,                 /* function  */
  YYSYMBOL_function_value = 148,           /* function_value  */
  YYSYMBOL_non_all_function_value = 149,   /* non_all_function_value  */
  YYSYMBOL_end = 150                       /* end  */
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

#if !defined yyoverflow

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
#endif /* !defined yyoverflow */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

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
#define YYFINAL  67
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   379

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  88
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  63
/* YYNRULES -- Number of rules.  */
#define YYNRULES  157
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  304

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   329


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
       9,    10,     7,     5,    83,     6,    84,     8,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    87,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    11,     2,    12,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    85,     2,    86,     2,     2,     2,     2,
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
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   172,   172,   177,   184,   190,   196,   202,   209,   216,
     223,   230,   237,   244,   251,   258,   267,   270,   273,   277,
     287,   296,   306,   314,   326,   337,   343,   353,   362,   368,
     377,   385,   392,   401,   410,   419,   422,   429,   438,   445,
     453,   463,   466,   474,   480,   487,   493,   500,   509,   515,
     522,   525,   531,   539,   546,   555,   560,   567,   576,   582,
     589,   596,   603,   610,   617,   621,   628,   636,   644,   652,
     662,   668,   675,   681,   688,   696,   719,   730,   736,   743,
     751,   757,   763,   769,   776,   782,   788,   794,   800,   806,
     812,   822,   825,   829,   836,   839,   844,   851,   857,   863,
     869,   876,   882,   889,   896,   905,   912,   919,   927,   936,
     943,   951,   959,   966,   972,   979,   986,   992,  1001,  1008,
    1015,  1022,  1029,  1039,  1047,  1056,  1060,  1066,  1072,  1079,
    1088,  1096,  1104,  1108,  1117,  1124,  1131,  1140,  1150,  1159,
    1188,  1189,  1190,  1191,  1192,  1193,  1196,  1203,  1210,  1217,
    1224,  1233,  1240,  1247,  1255,  1262,  1271,  1272
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "OR", "AND", "'+'",
  "'-'", "'*'", "'/'", "'('", "')'", "'['", "']'", "NL", "BEGINN",
  "COMMIT", "ROLLBACK", "CREATE", "DROP", "SELECT", "INSERT", "UPDATE",
  "DELETE", "DESCRIBE", "FROM", "WHERE", "INTO", "SET", "VALUES", "TABLE",
  "LIMIT", "SHOW", "TABLES", "PRIMARY", "KEY", "UNIQUE", "DEFAULT",
  "CHECK", "REFERENCES", "FOREIGN", "MAX", "MIN", "COUNT", "SUM", "AVG",
  "REF", "TRUE", "FALSE", "NULLX", "AS", "COMMENT", "CHAR", "INT", "LONG",
  "VARCHAR", "STRING", "BOOL", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP",
  "EQ", "NE", "GT", "GE", "LT", "LE", "IN", "LIKE", "NOT", "ALTER",
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
  "REFERVALUE", "BOOLVALUE", "assignments", "assignment", "condition",
  "predicate", "comparison_predicate", "like_predicate", "in_predicate",
  "compare", "function", "function_value", "non_all_function_value", "end", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-207)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-114)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     280,   -77,   -77,   -77,    -9,    -7,    69,     7,   -52,    60,
     -52,     5,    41,   262,  -207,  -207,  -207,  -207,  -207,  -207,
    -207,  -207,  -207,  -207,  -207,  -207,  -207,    75,  -207,  -207,
    -207,   -52,   -52,  -207,   175,    58,   126,   144,   150,   164,
     166,   181,  -207,  -207,  -207,   -25,  -207,  -207,  -207,   174,
     105,   160,  -207,  -207,  -207,  -207,  -207,  -207,  -207,   -52,
    -207,   173,   -52,   -77,   -77,   -77,   -52,  -207,  -207,  -207,
     193,   -77,    -3,   281,    15,   123,    58,    24,  -207,    52,
      52,    21,    21,    21,    17,    54,   192,   -52,   -77,   183,
     192,   192,   192,   192,   192,   135,    34,    54,    50,  -207,
    -207,  -207,    14,   226,  -207,   140,  -207,  -207,    58,  -207,
     146,  -207,  -207,   217,   219,  -207,  -207,  -207,   220,   231,
     236,    62,   251,    38,  -207,  -207,  -207,  -207,  -207,    71,
     167,  -207,   -40,  -207,    17,  -207,  -207,   160,   138,   138,
     198,   198,  -207,    54,    69,   242,   -77,  -207,   191,     4,
    -207,    17,  -207,   182,   189,   -77,  -207,  -207,   232,   258,
     260,   241,  -207,    25,  -207,  -207,   300,  -207,    54,  -207,
     310,  -207,  -207,  -207,  -207,  -207,   124,  -207,  -207,  -207,
    -207,  -207,  -207,   261,    58,   192,    17,    17,  -207,  -207,
     -52,   211,  -207,  -207,   139,    35,  -207,   174,    58,   227,
    -207,    58,    54,   -77,     8,   243,   244,  -207,   315,   244,
      17,   316,   -77,   226,  -207,  -207,  -207,   317,  -207,  -207,
    -207,  -207,  -207,  -207,  -207,   318,  -207,  -207,    58,  -207,
     160,   323,  -207,  -207,  -207,   152,    54,  -207,    36,   319,
    -207,  -207,  -207,  -207,  -207,   118,   244,    39,  -207,   179,
     244,  -207,  -207,   253,   322,   141,    40,   -77,  -207,  -207,
      58,   252,   256,  -207,    42,  -207,   244,  -207,    46,   326,
    -207,   325,   304,  -207,   115,   330,   -52,   259,   294,   271,
    -207,  -207,  -207,    47,  -207,  -207,  -207,  -207,   305,  -207,
    -207,  -207,  -207,  -207,    17,  -207,  -207,  -207,  -207,  -207,
     -52,   239,  -207,  -207
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     2,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,   156,    16,    17,
      18,     0,     0,    39,     0,     0,     0,     0,     0,     0,
       0,     0,   125,   126,   116,   109,   118,   121,   120,    41,
      38,    58,    60,    61,    63,   115,   122,   119,    62,     0,
      48,     0,     0,     0,     0,     0,     0,     1,     3,   157,
       0,     0,   109,     0,     0,    63,     0,     0,   113,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    50,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    27,
      28,    29,     0,     0,    20,     0,    64,   123,     0,   117,
       0,   154,   155,     0,     0,   153,   151,   152,     0,     0,
       0,     0,     0,     0,   133,   134,   135,   136,   112,     0,
      42,    43,    45,    21,     0,    40,    51,    59,    66,    67,
      68,    69,    65,     0,     0,     0,     0,    54,     0,    50,
     127,     0,    25,     0,     0,     0,    31,    32,     0,     0,
       0,     0,    79,     0,    72,    74,     0,    75,     0,   114,
       0,   146,   147,   148,   149,   150,     0,   140,   141,   142,
     143,   144,   145,     0,     0,     0,     0,     0,   124,   111,
       0,     0,    49,    46,    52,     0,    70,    41,     0,    53,
      22,     0,     0,     0,     0,     0,     0,    30,     0,     0,
       0,     0,     0,     0,    82,    80,    81,     0,    84,    85,
      86,    87,    89,    88,    90,    91,   110,   132,     0,   138,
     137,   130,   131,    44,    47,     0,     0,    57,     0,     0,
     129,   128,    24,    26,    34,    35,     0,     0,    77,     0,
       0,    19,    73,     0,     0,    94,     0,     0,    71,    55,
       0,     0,     0,    33,     0,   105,     0,   108,     0,     0,
      92,     0,     0,    98,     0,     0,     0,     0,     0,    76,
      95,   139,    23,     0,    36,    37,   106,    78,     0,    83,
      93,    99,   101,   100,     0,   104,   102,    97,    96,    56,
       0,     0,   107,   103
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -207,  -207,   331,  -207,  -207,  -207,  -207,  -207,  -207,  -207,
    -207,  -207,  -207,  -207,  -207,  -207,  -207,  -207,  -207,   201,
     149,  -207,  -207,   157,    -8,   158,   199,  -207,   127,  -207,
    -207,   275,     0,  -207,  -207,  -207,   151,   159,  -206,    -2,
    -207,  -207,  -207,    84,  -207,   -66,   -34,   -29,  -207,  -207,
    -207,  -207,   165,  -113,  -207,  -207,  -207,  -207,  -207,  -207,
     129,   286,     1
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,   155,   156,   157,   263,    49,
      88,    89,   130,   131,   132,   193,   135,   136,   146,   199,
     147,    50,    51,    52,   195,   163,   164,   165,   247,   166,
     225,   255,   279,   280,   167,    53,    74,    54,    55,    56,
      57,   149,   150,   123,   124,   125,   126,   127,   185,    58,
     118,   113,    28
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      61,    77,    63,    29,    30,    75,    78,   105,   176,   191,
      27,   186,   187,   112,   112,   117,   117,   117,   122,   128,
      31,   194,    32,    70,    71,   107,   121,    60,   115,   134,
     110,   148,   153,    59,    73,   212,   109,    64,   204,   192,
     264,   186,   187,   143,   268,   235,   259,    78,   188,   265,
     281,    96,   286,   144,    98,   122,   288,   299,   102,    85,
      86,   110,   145,   110,    99,   100,   101,    76,   122,    35,
      66,   121,   104,   231,   232,   151,    33,   196,    34,   169,
      35,    85,    86,    65,    62,   122,   154,   202,    69,   133,
     137,   138,   139,   140,   141,    27,    45,   249,   108,   152,
      45,   116,   226,    41,    42,    43,    44,   108,   213,    36,
      37,    38,    39,    40,    41,    42,    43,    44,   236,   108,
     122,   122,   266,   108,    76,   266,    35,   186,   187,   266,
     108,    45,   111,    45,   227,    79,   148,    27,    46,    47,
      48,    72,   186,   187,   122,    93,    94,   200,    45,    46,
      47,    48,   271,    80,    90,   229,   207,   189,   224,    81,
      41,    42,    43,   292,   238,    91,    92,    93,    94,    78,
     258,   144,   240,    82,   272,    83,   273,   274,   275,   276,
     145,   301,   186,   187,    34,   230,    35,    95,    90,   267,
      84,   277,   261,   262,   256,    46,    47,    48,    87,    78,
      97,    34,   103,    35,   242,   243,  -113,   248,   134,    95,
     278,   119,   120,   251,   142,    36,    37,    38,    39,    40,
      41,    42,    43,    44,   168,   170,   283,   171,   122,   172,
     173,    78,    36,    37,    38,    39,    40,    41,    42,    43,
      44,   174,   186,   187,   248,   293,   175,    95,   248,   303,
     190,   198,   201,   205,    72,    46,    47,    48,   282,   158,
     206,   159,    67,   160,   287,   161,   208,   209,   295,   210,
     228,    45,    46,    47,    48,   211,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    91,    92,    93,    94,
     192,   106,   302,    11,     1,     2,     3,     4,     5,     6,
       7,     8,     9,    10,   272,   162,   273,   274,   275,   276,
     239,    11,   177,   178,   179,   180,   181,   182,   183,   184,
     105,   277,   244,   162,   246,   250,   253,   187,   260,   254,
      95,   284,    12,   269,   270,   285,   289,   290,   291,   294,
     278,   296,   297,   300,    68,   197,   237,   233,   203,   234,
      12,   214,   215,   216,   217,   218,   219,   220,   221,   222,
     223,   129,   257,   298,   252,   245,   114,   241,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    60
};

static const yytype_int16 yycheck[] =
{
       8,    35,    10,     2,     3,    34,    35,    10,   121,    49,
      87,     3,     4,    79,    80,    81,    82,    83,    84,    85,
      29,   134,    29,    31,    32,    10,     9,    79,     7,    25,
       9,    97,    18,    26,    34,    10,    12,    32,   151,    79,
     246,     3,     4,     9,   250,    10,    10,    76,    10,    10,
      10,    59,    10,    19,    62,   121,    10,    10,    66,    84,
      85,     9,    28,     9,    63,    64,    65,     9,   134,    11,
      29,     9,    71,   186,   187,    25,     7,   143,     9,   108,
      11,    84,    85,    78,    24,   151,    72,    83,    13,    88,
      90,    91,    92,    93,    94,    87,    79,   210,    83,    98,
      79,    80,   168,    45,    46,    47,    48,    83,    83,    40,
      41,    42,    43,    44,    45,    46,    47,    48,    83,    83,
     186,   187,    83,    83,     9,    83,    11,     3,     4,    83,
      83,    79,    80,    79,    10,     9,   202,    87,    80,    81,
      82,    79,     3,     4,   210,     7,     8,   146,    79,    80,
      81,    82,    11,     9,    83,   184,   155,    86,   166,     9,
      45,    46,    47,    48,   198,     5,     6,     7,     8,   198,
     236,    19,   201,     9,    33,     9,    35,    36,    37,    38,
      28,   294,     3,     4,     9,   185,    11,    49,    83,    10,
       9,    50,    74,    75,   228,    80,    81,    82,    24,   228,
      27,     9,     9,    11,   203,   204,    83,   209,    25,    49,
      69,    82,    83,   212,    79,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    84,    79,   260,    10,   294,    10,
      10,   260,    40,    41,    42,    43,    44,    45,    46,    47,
      48,    10,     3,     4,   246,   274,    10,    49,   250,    10,
      83,     9,    61,    71,    79,    80,    81,    82,   257,    33,
      71,    35,     0,    37,   266,    39,    34,     9,   276,     9,
       9,    79,    80,    81,    82,    34,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,     5,     6,     7,     8,
      79,    10,   300,    31,    14,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    33,    79,    35,    36,    37,    38,
      83,    31,    61,    62,    63,    64,    65,    66,    67,    68,
      10,    50,    79,    79,     9,     9,     9,     4,     9,    11,
      49,    79,    70,    80,    12,    79,    10,    12,    34,     9,
      69,    82,    48,    38,    13,   144,   197,   190,   149,   191,
      70,    51,    52,    53,    54,    55,    56,    57,    58,    59,
      60,    86,   235,   279,   213,   206,    80,   202,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    79
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    31,    70,    89,    90,    91,    92,    93,    94,    95,
      96,    97,    98,    99,   100,   101,   102,    87,   150,   150,
     150,    29,    29,     7,     9,    11,    40,    41,    42,    43,
      44,    45,    46,    47,    48,    79,    80,    81,    82,   107,
     119,   120,   121,   133,   135,   136,   137,   138,   147,    26,
      79,   112,    24,   112,    32,    78,    29,     0,    90,    13,
     112,   112,    79,   120,   134,   135,     9,   134,   135,     9,
       9,     9,     9,     9,     9,    84,    85,    24,   108,   109,
      83,     5,     6,     7,     8,    49,   112,    27,   112,   150,
     150,   150,   112,     9,   150,    10,    10,    10,    83,    12,
       9,    80,   133,   149,   149,     7,    80,   133,   148,   148,
     148,     9,   133,   141,   142,   143,   144,   145,   133,   119,
     110,   111,   112,   150,    25,   114,   115,   120,   120,   120,
     120,   120,    79,     9,    19,    28,   116,   118,   133,   139,
     140,    25,   150,    18,    72,   103,   104,   105,    33,    35,
      37,    39,    79,   123,   124,   125,   127,   132,    84,   135,
      79,    10,    10,    10,    10,    10,   141,    61,    62,    63,
      64,    65,    66,    67,    68,   146,     3,     4,    10,    86,
      83,    49,    79,   113,   141,   122,   133,   107,     9,   117,
     150,    61,    83,   114,   141,    71,    71,   150,    34,     9,
       9,    34,    10,    83,    51,    52,    53,    54,    55,    56,
      57,    58,    59,    60,   112,   128,   133,    10,     9,   135,
     120,   141,   141,   111,   113,    10,    83,   108,   134,    83,
     135,   140,   150,   150,    79,   125,     9,   126,   127,   141,
       9,   150,   124,     9,    11,   129,   134,   116,   133,    10,
       9,    74,    75,   106,   126,    10,    83,    10,   126,    80,
      12,    11,    33,    35,    36,    37,    38,    50,    69,   130,
     131,    10,   150,   134,    79,    79,    10,   127,    10,    10,
      12,    34,    48,   135,     9,   112,    82,    48,   131,    10,
      38,   141,   112,    10
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,    88,    89,    89,    90,    90,    90,    90,    90,    90,
      90,    90,    90,    90,    90,    90,    91,    92,    93,    94,
      95,    96,    97,    97,    98,    99,    99,   100,   101,   101,
     102,   103,   103,   104,   105,   106,   106,   106,   107,   107,
     108,   109,   109,   110,   110,   111,   111,   111,   112,   113,
     114,   114,   115,   116,   116,   117,   117,   118,   119,   119,
     120,   120,   120,   120,   120,   120,   121,   121,   121,   121,
     122,   122,   123,   123,   124,   124,   125,   126,   126,   127,
     128,   128,   128,   128,   128,   128,   128,   128,   128,   128,
     128,   129,   129,   129,   130,   130,   130,   131,   131,   131,
     131,   131,   131,   131,   131,   132,   132,   132,   132,   133,
     133,   133,   133,   134,   134,   135,   135,   135,   136,   136,
     136,   136,   136,   137,   137,   138,   138,   139,   139,   140,
     141,   141,   141,   141,   142,   142,   142,   143,   144,   145,
     146,   146,   146,   146,   146,   146,   147,   147,   147,   147,
     147,   148,   148,   148,   149,   149,   150,   150
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     2,     2,     2,     7,
       4,     4,     5,     8,     6,     4,     6,     3,     3,     3,
       5,     1,     1,     4,     3,     0,     2,     2,     1,     1,
       2,     0,     2,     1,     3,     1,     2,     3,     1,     1,
       0,     1,     2,     2,     1,     3,     5,     3,     1,     3,
       1,     1,     1,     1,     3,     3,     3,     3,     3,     3,
       1,     3,     1,     3,     1,     1,     4,     1,     3,     1,
       1,     1,     1,     4,     1,     1,     1,     1,     1,     1,
       1,     0,     2,     3,     0,     1,     2,     2,     1,     2,
       2,     2,     2,     4,     2,     4,     5,     7,     4,     1,
       5,     4,     3,     1,     3,     1,     1,     3,     1,     1,
       1,     1,     1,     3,     4,     1,     1,     1,     3,     3,
       3,     3,     3,     1,     1,     1,     1,     3,     3,     5,
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




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, states); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, List *states)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
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
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, List *states)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep, states);
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
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
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
                       &yyvsp[(yyi + 1) - (yynrhs)], states);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, states); \
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






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, List *states)
{
  YY_USE (yyvaluep);
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

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

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

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
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
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

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
      yychar = yylex ();
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


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* statements: statement  */
#line 173 "sql.y"
        {
            append_list(states, (yyvsp[0].statement));
            (yyval.list) = states;
        }
#line 1717 "y.tab.c"
    break;

  case 3: /* statements: statements statement  */
#line 178 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].statement));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 1726 "y.tab.c"
    break;

  case 4: /* statement: begin_transaction_statement  */
#line 185 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = BEGIN_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 1736 "y.tab.c"
    break;

  case 5: /* statement: commit_transaction_statement  */
#line 191 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = COMMIT_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 1746 "y.tab.c"
    break;

  case 6: /* statement: rollback_transaction_statement  */
#line 197 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ROLLBACK_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 1756 "y.tab.c"
    break;

  case 7: /* statement: create_table_statement  */
#line 203 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_TABLE_STMT;
            statement->create_table_node = (yyvsp[0].create_table_node);
            (yyval.statement) = statement;
        }
#line 1767 "y.tab.c"
    break;

  case 8: /* statement: drop_table_statement  */
#line 210 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_TABLE_STMT;
            statement->drop_table_node = (yyvsp[0].drop_table_node);
            (yyval.statement) = statement;
        }
#line 1778 "y.tab.c"
    break;

  case 9: /* statement: select_statement  */
#line 217 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SELECT_STMT;
            statement->select_node = (yyvsp[0].select_node);
            (yyval.statement) = statement;
        }
#line 1789 "y.tab.c"
    break;

  case 10: /* statement: insert_statement  */
#line 224 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = INSERT_STMT;
            statement->insert_node = (yyvsp[0].insert_node);
            (yyval.statement) = statement;
        }
#line 1800 "y.tab.c"
    break;

  case 11: /* statement: update_statement  */
#line 231 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = UPDATE_STMT;
            statement->update_node = (yyvsp[0].update_node);
            (yyval.statement) = statement;
        }
#line 1811 "y.tab.c"
    break;

  case 12: /* statement: delete_statement  */
#line 238 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DELETE_STMT;
            statement->delete_node = (yyvsp[0].delete_node);
            (yyval.statement) = statement;
        }
#line 1822 "y.tab.c"
    break;

  case 13: /* statement: describe_statement  */
#line 245 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DESCRIBE_STMT;
            statement->describe_node = (yyvsp[0].describe_node);
            (yyval.statement) = statement;
        }
#line 1833 "y.tab.c"
    break;

  case 14: /* statement: show_statement  */
#line 252 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SHOW_STMT;
            statement->show_node = (yyvsp[0].show_node);
            (yyval.statement) = statement;
        }
#line 1844 "y.tab.c"
    break;

  case 15: /* statement: alter_table_statement  */
#line 259 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ALTER_TABLE_STMT;
            statement->alter_table_node = (yyvsp[0].alter_table_node);
            (yyval.statement) = statement;
        }
#line 1855 "y.tab.c"
    break;

  case 19: /* create_table_statement: CREATE TABLE table '(' base_table_element_commalist ')' end  */
#line 278 "sql.y"
        {
            CreateTableNode *create_table_node = instance(CreateTableNode);
            create_table_node->table_name = (yyvsp[-4].strVal);
            create_table_node->base_table_element_commalist = (yyvsp[-2].list);
            (yyval.create_table_node) = create_table_node;
        }
#line 1866 "y.tab.c"
    break;

  case 20: /* drop_table_statement: DROP TABLE table end  */
#line 288 "sql.y"
        {
            DropTableNode *drop_table_node = instance(DropTableNode);
            drop_table_node->table_name = (yyvsp[-1].strVal);
            (yyval.drop_table_node) = drop_table_node;
        }
#line 1876 "y.tab.c"
    break;

  case 21: /* select_statement: SELECT selection table_exp end  */
#line 297 "sql.y"
        {
            SelectNode *select_node = instance(SelectNode);
            select_node->selection = (yyvsp[-2].selection_node);
            select_node->table_exp = (yyvsp[-1].table_exp_node);
            (yyval.select_node) = select_node;
        }
#line 1887 "y.tab.c"
    break;

  case 22: /* insert_statement: INSERT INTO table values_or_query_spec end  */
#line 307 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = true;
            node->table_name = (yyvsp[-2].strVal);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 1899 "y.tab.c"
    break;

  case 23: /* insert_statement: INSERT INTO table '(' columns ')' values_or_query_spec end  */
#line 315 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = false;
            node->table_name = (yyvsp[-5].strVal);
            node->column_list = (yyvsp[-3].list);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 1912 "y.tab.c"
    break;

  case 24: /* update_statement: UPDATE table SET assignments opt_where_clause end  */
#line 327 "sql.y"
        {
            UpdateNode *node = instance(UpdateNode);
            node->table_name = (yyvsp[-4].strVal);
            node->assignment_list = (yyvsp[-2].list);
            node->where_clause = (yyvsp[-1].where_clause_node);
            (yyval.update_node) = node;
        }
#line 1924 "y.tab.c"
    break;

  case 25: /* delete_statement: DELETE FROM table end  */
#line 338 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.delete_node) = node;
        }
#line 1934 "y.tab.c"
    break;

  case 26: /* delete_statement: DELETE FROM table WHERE condition end  */
#line 344 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-3].strVal);
            node->condition_node = (yyvsp[-1].condition_node);
            (yyval.delete_node) = node;
        }
#line 1945 "y.tab.c"
    break;

  case 27: /* describe_statement: DESCRIBE table end  */
#line 354 "sql.y"
        {
            DescribeNode *node = instance(DescribeNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.describe_node) = node;
        }
#line 1955 "y.tab.c"
    break;

  case 28: /* show_statement: SHOW TABLES end  */
#line 363 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_TABLES;
            (yyval.show_node) = node;
        }
#line 1965 "y.tab.c"
    break;

  case 29: /* show_statement: SHOW MEMORY end  */
#line 369 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_MEMORY;
            (yyval.show_node) = node;
        }
#line 1975 "y.tab.c"
    break;

  case 30: /* alter_table_statement: ALTER TABLE table alter_table_action end  */
#line 378 "sql.y"
        {
            (yyval.alter_table_node) = instance(AlterTableNode);
            (yyval.alter_table_node)->table_name = (yyvsp[-2].strVal);
            (yyval.alter_table_node)->action = (yyvsp[-1].alter_table_action);
        }
#line 1985 "y.tab.c"
    break;

  case 31: /* alter_table_action: add_column_def  */
#line 386 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_ADD_COLUMN;
            action->action.add_column = (yyvsp[0].add_column_def);
            (yyval.alter_table_action) = action;
        }
#line 1996 "y.tab.c"
    break;

  case 32: /* alter_table_action: drop_column_def  */
#line 393 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_DROP_COLUMN;
            action->action.drop_column = (yyvsp[0].drop_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2007 "y.tab.c"
    break;

  case 33: /* add_column_def: ADD COLUMN column_def column_position_def  */
#line 402 "sql.y"
        {
            AddColumnDef *node = instance(AddColumnDef);
            node->column_def = (yyvsp[-1].column_def_node);
            node->position_def = (yyvsp[0].column_position_def);
            (yyval.add_column_def) = node;
        }
#line 2018 "y.tab.c"
    break;

  case 34: /* drop_column_def: DROP COLUMN IDENTIFIER  */
#line 411 "sql.y"
        {
            DropColumnDef *node = instance(DropColumnDef);
            node->column_name = (yyvsp[0].strVal);
            (yyval.drop_column_def) = node;
        }
#line 2028 "y.tab.c"
    break;

  case 35: /* column_position_def: %empty  */
#line 419 "sql.y"
    {
        (yyval.column_position_def) = NULL;
    }
#line 2036 "y.tab.c"
    break;

  case 36: /* column_position_def: BEFORE IDENTIFIER  */
#line 423 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_BEFORE;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2047 "y.tab.c"
    break;

  case 37: /* column_position_def: AFTER IDENTIFIER  */
#line 430 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_AFTER;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2058 "y.tab.c"
    break;

  case 38: /* selection: scalar_exp_commalist  */
#line 439 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = false;
            selection_node->scalar_exp_list = (yyvsp[0].list);
            (yyval.selection_node) = selection_node;
        }
#line 2069 "y.tab.c"
    break;

  case 39: /* selection: '*'  */
#line 446 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = true;
            (yyval.selection_node) = selection_node;
        }
#line 2079 "y.tab.c"
    break;

  case 40: /* table_exp: from_clause opt_where_clause  */
#line 454 "sql.y"
        {
            TableExpNode *table_exp = instance(TableExpNode);
            table_exp->from_clause = (yyvsp[-1].from_clause_node);
            table_exp->where_clause = (yyvsp[0].where_clause_node);
            (yyval.table_exp_node) = table_exp;
        }
#line 2090 "y.tab.c"
    break;

  case 41: /* from_clause: %empty  */
#line 463 "sql.y"
        {
            (yyval.from_clause_node) = NULL;
        }
#line 2098 "y.tab.c"
    break;

  case 42: /* from_clause: FROM table_ref_commalist  */
#line 467 "sql.y"
        {
            FromClauseNode *from_clause = instance(FromClauseNode);
            from_clause->from = (yyvsp[0].list);
            (yyval.from_clause_node) = from_clause;
        }
#line 2108 "y.tab.c"
    break;

  case 43: /* table_ref_commalist: table_ref  */
#line 475 "sql.y"
        {
            List *list = create_list(NODE_TABLE_REFER);
            append_list(list, (yyvsp[0].table_ref_node));
            (yyval.list) = list;
        }
#line 2118 "y.tab.c"
    break;

  case 44: /* table_ref_commalist: table_ref_commalist ',' table_ref  */
#line 481 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].table_ref_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2127 "y.tab.c"
    break;

  case 45: /* table_ref: table  */
#line 488 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2137 "y.tab.c"
    break;

  case 46: /* table_ref: table range_variable  */
#line 494 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-1].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2148 "y.tab.c"
    break;

  case 47: /* table_ref: table AS range_variable  */
#line 501 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-2].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2159 "y.tab.c"
    break;

  case 48: /* table: IDENTIFIER  */
#line 510 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2167 "y.tab.c"
    break;

  case 49: /* range_variable: IDENTIFIER  */
#line 516 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2175 "y.tab.c"
    break;

  case 50: /* opt_where_clause: %empty  */
#line 522 "sql.y"
        {
            (yyval.where_clause_node) = NULL;
        }
#line 2183 "y.tab.c"
    break;

  case 51: /* opt_where_clause: where_clause  */
#line 526 "sql.y"
        {
            (yyval.where_clause_node) = (yyvsp[0].where_clause_node);
        }
#line 2191 "y.tab.c"
    break;

  case 52: /* where_clause: WHERE condition  */
#line 532 "sql.y"
        {
            WhereClauseNode *where_clause_node = instance(WhereClauseNode);
            where_clause_node->condition = (yyvsp[0].condition_node);
            (yyval.where_clause_node) = where_clause_node;
        }
#line 2201 "y.tab.c"
    break;

  case 53: /* values_or_query_spec: VALUES opt_values  */
#line 540 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_VALUES;
            values_or_query_spec->values = (yyvsp[0].list);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2212 "y.tab.c"
    break;

  case 54: /* values_or_query_spec: query_spec  */
#line 547 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_QUERY_SPEC;
            values_or_query_spec->query_spec = (yyvsp[0].query_spec_node);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2223 "y.tab.c"
    break;

  case 55: /* opt_values: '(' value_items ')'  */
#line 556 "sql.y"
        {
            (yyval.list) = create_list(NODE_LIST);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2232 "y.tab.c"
    break;

  case 56: /* opt_values: opt_values ',' '(' value_items ')'  */
#line 561 "sql.y"
        {
            (yyval.list) = (yyvsp[-4].list);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2241 "y.tab.c"
    break;

  case 57: /* query_spec: SELECT selection table_exp  */
#line 568 "sql.y"
        {
            QuerySpecNode *query_spec = instance(QuerySpecNode);
            query_spec->selection = (yyvsp[-1].selection_node);
            query_spec->table_exp = (yyvsp[0].table_exp_node);
            (yyval.query_spec_node) = query_spec;
        }
#line 2252 "y.tab.c"
    break;

  case 58: /* scalar_exp_commalist: scalar_exp  */
#line 577 "sql.y"
        {
            List *scalar_exp_list = create_list(NODE_SCALAR_EXP);
            append_list(scalar_exp_list, (yyvsp[0].scalar_exp_node));
            (yyval.list) = scalar_exp_list;
        }
#line 2262 "y.tab.c"
    break;

  case 59: /* scalar_exp_commalist: scalar_exp_commalist ',' scalar_exp  */
#line 583 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].scalar_exp_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2271 "y.tab.c"
    break;

  case 60: /* scalar_exp: calculate  */
#line 590 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_CALCULATE;
            scalar_exp_node->calculate = (yyvsp[0].calculate_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2282 "y.tab.c"
    break;

  case 61: /* scalar_exp: column  */
#line 597 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_COLUMN;
            scalar_exp_node->column = (yyvsp[0].column_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2293 "y.tab.c"
    break;

  case 62: /* scalar_exp: function  */
#line 604 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_FUNCTION;
            scalar_exp_node->function = (yyvsp[0].function_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2304 "y.tab.c"
    break;

  case 63: /* scalar_exp: value_item  */
#line 611 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_VALUE;
            scalar_exp_node->value = (yyvsp[0].value_item_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2315 "y.tab.c"
    break;

  case 64: /* scalar_exp: '(' scalar_exp ')'  */
#line 618 "sql.y"
        {
            (yyval.scalar_exp_node) = (yyvsp[-1].scalar_exp_node);
        }
#line 2323 "y.tab.c"
    break;

  case 65: /* scalar_exp: scalar_exp AS IDENTIFIER  */
#line 622 "sql.y"
        {
            (yyvsp[-2].scalar_exp_node)->alias = (yyvsp[0].strVal);
            (yyval.scalar_exp_node) = (yyvsp[-2].scalar_exp_node);
        }
#line 2332 "y.tab.c"
    break;

  case 66: /* calculate: scalar_exp '+' scalar_exp  */
#line 629 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_ADD;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2344 "y.tab.c"
    break;

  case 67: /* calculate: scalar_exp '-' scalar_exp  */
#line 637 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_SUB;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2356 "y.tab.c"
    break;

  case 68: /* calculate: scalar_exp '*' scalar_exp  */
#line 645 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_MUL;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2368 "y.tab.c"
    break;

  case 69: /* calculate: scalar_exp '/' scalar_exp  */
#line 653 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_DIV;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2380 "y.tab.c"
    break;

  case 70: /* columns: column  */
#line 663 "sql.y"
        {
            List *column_set_node = create_list(NODE_COLUMN);
            append_list(column_set_node, (yyvsp[0].column_node));
            (yyval.list) = column_set_node;
        }
#line 2390 "y.tab.c"
    break;

  case 71: /* columns: columns ',' column  */
#line 669 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].column_node));
        }
#line 2399 "y.tab.c"
    break;

  case 72: /* base_table_element_commalist: base_table_element  */
#line 676 "sql.y"
        {
            List *base_table_element_commalist = create_list(NODE_BASE_TABLE_ELEMENT);
            append_list(base_table_element_commalist, (yyvsp[0].base_table_element));
            (yyval.list) = base_table_element_commalist;
        }
#line 2409 "y.tab.c"
    break;

  case 73: /* base_table_element_commalist: base_table_element_commalist ',' base_table_element  */
#line 682 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].base_table_element));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2418 "y.tab.c"
    break;

  case 74: /* base_table_element: column_def  */
#line 689 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = (yyvsp[0].column_def_node);
            node->table_contraint_def = NULL;
            node->type = TELE_COLUMN_DEF;
            (yyval.base_table_element) = node;
        }
#line 2430 "y.tab.c"
    break;

  case 75: /* base_table_element: table_contraint_def  */
#line 697 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = NULL;
            node->table_contraint_def = (yyvsp[0].table_contraint_def);
            node->type = TELE_TABLE_CONTRAINT_DEF;
            (yyval.base_table_element) = node;
        }
#line 2442 "y.tab.c"
    break;

  case 76: /* column_def: column_def_name data_type array_dim_clause column_def_opt_list  */
#line 720 "sql.y"
        {
            ColumnDefNode *column_def = instance(ColumnDefNode);
            column_def->column = (yyvsp[-3].column_def_name);
            column_def->data_type = (yyvsp[-2].data_type_node);
            column_def->array_dim = (yyvsp[-1].intVal);
            column_def->column_def_opt_list = (yyvsp[0].list);
            (yyval.column_def_node) = column_def;
        }
#line 2455 "y.tab.c"
    break;

  case 77: /* column_def_name_commalist: column_def_name  */
#line 731 "sql.y"
        {
            List *list = create_list(NODE_COLUMN_DEF_NAME);
            append_list(list, (yyvsp[0].column_def_name));
            (yyval.list) = list;
        }
#line 2465 "y.tab.c"
    break;

  case 78: /* column_def_name_commalist: column_def_name_commalist ',' column_def_name  */
#line 737 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].column_def_name));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2474 "y.tab.c"
    break;

  case 79: /* column_def_name: IDENTIFIER  */
#line 744 "sql.y"
        {
            ColumnDefName *column_def_name = instance(ColumnDefName);
            column_def_name->column = (yyvsp[0].strVal);
            (yyval.column_def_name) = column_def_name;
        }
#line 2484 "y.tab.c"
    break;

  case 80: /* data_type: INT  */
#line 752 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_INT; 
            (yyval.data_type_node) = node;
        }
#line 2494 "y.tab.c"
    break;

  case 81: /* data_type: LONG  */
#line 758 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_LONG;  
            (yyval.data_type_node) = node;
        }
#line 2504 "y.tab.c"
    break;

  case 82: /* data_type: CHAR  */
#line 764 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_CHAR; 
            (yyval.data_type_node) = node;
        }
#line 2514 "y.tab.c"
    break;

  case 83: /* data_type: VARCHAR '(' INTVALUE ')'  */
#line 770 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_VARCHAR; 
            node->len = (yyvsp[-1].intVal);
            (yyval.data_type_node) = node;
        }
#line 2525 "y.tab.c"
    break;

  case 84: /* data_type: STRING  */
#line 777 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_STRING; 
            (yyval.data_type_node) = node;
        }
#line 2535 "y.tab.c"
    break;

  case 85: /* data_type: BOOL  */
#line 783 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_BOOL; 
            (yyval.data_type_node) = node;
        }
#line 2545 "y.tab.c"
    break;

  case 86: /* data_type: FLOAT  */
#line 789 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_FLOAT; 
            (yyval.data_type_node) = node;
        }
#line 2555 "y.tab.c"
    break;

  case 87: /* data_type: DOUBLE  */
#line 795 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DOUBLE; 
            (yyval.data_type_node) = node;
        }
#line 2565 "y.tab.c"
    break;

  case 88: /* data_type: TIMESTAMP  */
#line 801 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_TIMESTAMP; 
            (yyval.data_type_node) = node;
        }
#line 2575 "y.tab.c"
    break;

  case 89: /* data_type: DATE  */
#line 807 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DATE; 
            (yyval.data_type_node) = node;
        }
#line 2585 "y.tab.c"
    break;

  case 90: /* data_type: table  */
#line 813 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_REFERENCE;
            node->table_name = (yyvsp[0].strVal);
            (yyval.data_type_node) = node;
        }
#line 2596 "y.tab.c"
    break;

  case 91: /* array_dim_clause: %empty  */
#line 822 "sql.y"
        {
            (yyval.intVal) = 0;
        }
#line 2604 "y.tab.c"
    break;

  case 92: /* array_dim_clause: '[' ']'  */
#line 826 "sql.y"
        {
            (yyval.intVal) = 1;
        }
#line 2612 "y.tab.c"
    break;

  case 93: /* array_dim_clause: array_dim_clause '[' ']'  */
#line 830 "sql.y"
        {
            (yyval.intVal)++;
        }
#line 2620 "y.tab.c"
    break;

  case 94: /* column_def_opt_list: %empty  */
#line 836 "sql.y"
        {
            (yyval.list) = NULL;
        }
#line 2628 "y.tab.c"
    break;

  case 95: /* column_def_opt_list: column_def_opt  */
#line 840 "sql.y"
        {
            (yyval.list) = create_list(NODE_COLUMN_DEF_OPT);
            append_list((yyval.list), (yyvsp[0].column_def_opt));
        }
#line 2637 "y.tab.c"
    break;

  case 96: /* column_def_opt_list: column_def_opt_list column_def_opt  */
#line 845 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].column_def_opt));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 2646 "y.tab.c"
    break;

  case 97: /* column_def_opt: NOT NULLX  */
#line 852 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_NOT_NULL; 
            (yyval.column_def_opt) = node;
        }
#line 2656 "y.tab.c"
    break;

  case 98: /* column_def_opt: UNIQUE  */
#line 858 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_UNIQUE; 
            (yyval.column_def_opt) = node;
        }
#line 2666 "y.tab.c"
    break;

  case 99: /* column_def_opt: PRIMARY KEY  */
#line 864 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_PRIMARY_KEY; 
            (yyval.column_def_opt) = node;
        }
#line 2676 "y.tab.c"
    break;

  case 100: /* column_def_opt: DEFAULT value_item  */
#line 870 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_VALUE;
            node->value = (yyvsp[0].value_item_node);
            (yyval.column_def_opt) = node;
        }
#line 2687 "y.tab.c"
    break;

  case 101: /* column_def_opt: DEFAULT NULLX  */
#line 877 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_NULL;
            (yyval.column_def_opt) = node;
        }
#line 2697 "y.tab.c"
    break;

  case 102: /* column_def_opt: COMMENT STRINGVALUE  */
#line 883 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_COMMENT;
            node->comment = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 2708 "y.tab.c"
    break;

  case 103: /* column_def_opt: CHECK '(' condition ')'  */
#line 890 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_CHECK_CONDITION;
            node->condition = (yyvsp[-1].condition_node);
            (yyval.column_def_opt) = node;
        }
#line 2719 "y.tab.c"
    break;

  case 104: /* column_def_opt: REFERENCES table  */
#line 897 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_REFERENECS;
            node->refer_table = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 2730 "y.tab.c"
    break;

  case 105: /* table_contraint_def: UNIQUE '(' column_def_name_commalist ')'  */
#line 906 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_UNIQUE;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 2741 "y.tab.c"
    break;

  case 106: /* table_contraint_def: PRIMARY KEY '(' column_def_name_commalist ')'  */
#line 913 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_PRIMARY_KEY;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 2752 "y.tab.c"
    break;

  case 107: /* table_contraint_def: FOREIGN KEY '(' column_def_name_commalist ')' REFERENCES table  */
#line 920 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_FOREIGN_KEY;
            node->column_commalist = (yyvsp[-3].list);
            node->table = (yyvsp[0].strVal);
            (yyval.table_contraint_def) = node;
        }
#line 2764 "y.tab.c"
    break;

  case 108: /* table_contraint_def: CHECK '(' condition ')'  */
#line 928 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_CHECK;
            node->condition = (yyvsp[-1].condition_node);
            (yyval.table_contraint_def) = node;
        }
#line 2775 "y.tab.c"
    break;

  case 109: /* column: IDENTIFIER  */
#line 937 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[0].strVal);
            column_node->has_sub_column = false;
            (yyval.column_node) = column_node;
        }
#line 2786 "y.tab.c"
    break;

  case 110: /* column: '(' IDENTIFIER ')' '.' column  */
#line 944 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->sub_column = (yyvsp[0].column_node);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 2798 "y.tab.c"
    break;

  case 111: /* column: IDENTIFIER '{' scalar_exp_commalist '}'  */
#line 952 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->scalar_exp_list = (yyvsp[-1].list);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 2810 "y.tab.c"
    break;

  case 112: /* column: IDENTIFIER '.' column  */
#line 960 "sql.y"
        {
            (yyval.column_node) = (yyvsp[0].column_node);
            (yyval.column_node)->range_variable = (yyvsp[-2].strVal);
        }
#line 2819 "y.tab.c"
    break;

  case 113: /* value_items: value_item  */
#line 967 "sql.y"
        {
            List *value_list = create_list(NODE_VALUE_ITEM);
            append_list(value_list, (yyvsp[0].value_item_node));
            (yyval.list) = value_list;
        }
#line 2829 "y.tab.c"
    break;

  case 114: /* value_items: value_items ',' value_item  */
#line 973 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].value_item_node));
        }
#line 2838 "y.tab.c"
    break;

  case 115: /* value_item: atom  */
#line 980 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ATOM;
            node->value.atom = (yyvsp[0].atom_node);
            (yyval.value_item_node) = node;
        }
#line 2849 "y.tab.c"
    break;

  case 116: /* value_item: NULLX  */
#line 987 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_NULL;
            (yyval.value_item_node) = node;
        }
#line 2859 "y.tab.c"
    break;

  case 117: /* value_item: '[' value_items ']'  */
#line 993 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ARRAY;
            node->value.value_list = (yyvsp[-1].list);
            (yyval.value_item_node) = node;
        }
#line 2870 "y.tab.c"
    break;

  case 118: /* atom: INTVALUE  */
#line 1002 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.intval = (yyvsp[0].intVal);
            node->type = A_INT;
            (yyval.atom_node) = node;
        }
#line 2881 "y.tab.c"
    break;

  case 119: /* atom: BOOLVALUE  */
#line 1009 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.boolval = (yyvsp[0].boolVal);
            node->type = A_BOOL;
            (yyval.atom_node) = node;
        }
#line 2892 "y.tab.c"
    break;

  case 120: /* atom: STRINGVALUE  */
#line 1016 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.strval = (yyvsp[0].strVal);
            node->type = A_STRING;
            (yyval.atom_node) = node;
        }
#line 2903 "y.tab.c"
    break;

  case 121: /* atom: FLOATVALUE  */
#line 1023 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.floatval = (yyvsp[0].floatVal);
            node->type = A_FLOAT;
            (yyval.atom_node) = node;
        }
#line 2914 "y.tab.c"
    break;

  case 122: /* atom: REFERVALUE  */
#line 1030 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.referval = (yyvsp[0].referVal);
            node->type = A_REFERENCE;
            (yyval.atom_node) = node;
        }
#line 2925 "y.tab.c"
    break;

  case 123: /* REFERVALUE: '(' value_items ')'  */
#line 1040 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = DIRECTLY;
            refer->nest_value_list = (yyvsp[-1].list);
            (yyval.referVal) = refer;
        }
#line 2936 "y.tab.c"
    break;

  case 124: /* REFERVALUE: REF '(' condition ')'  */
#line 1048 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = INDIRECTLY;
            refer->condition = (yyvsp[-1].condition_node);
            (yyval.referVal) = refer;
        }
#line 2947 "y.tab.c"
    break;

  case 125: /* BOOLVALUE: TRUE  */
#line 1057 "sql.y"
        {
            (yyval.boolVal) = true;
        }
#line 2955 "y.tab.c"
    break;

  case 126: /* BOOLVALUE: FALSE  */
#line 1061 "sql.y"
        {
            (yyval.boolVal) = false;
        }
#line 2963 "y.tab.c"
    break;

  case 127: /* assignments: assignment  */
#line 1067 "sql.y"
        {
            List *list = create_list(NODE_ASSIGNMENT);
            append_list(list, (yyvsp[0].assignment_node));
            (yyval.list) = list;
        }
#line 2973 "y.tab.c"
    break;

  case 128: /* assignments: assignments ',' assignment  */
#line 1073 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].assignment_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2982 "y.tab.c"
    break;

  case 129: /* assignment: column EQ value_item  */
#line 1080 "sql.y"
        {
            AssignmentNode *node = instance(AssignmentNode);
            node->column = (yyvsp[-2].column_node);
            node->value = (yyvsp[0].value_item_node);
            (yyval.assignment_node) = node;
        }
#line 2993 "y.tab.c"
    break;

  case 130: /* condition: condition OR condition  */
#line 1089 "sql.y"
        {
            ConditionNode *condition = instance(ConditionNode);
            condition->conn_type = C_OR;
            condition->left = (yyvsp[-2].condition_node);
            condition->right = (yyvsp[0].condition_node);
            (yyval.condition_node) = condition;
        }
#line 3005 "y.tab.c"
    break;

  case 131: /* condition: condition AND condition  */
#line 1097 "sql.y"
        {
            ConditionNode *condition = instance(ConditionNode);
            condition->conn_type = C_AND;
            condition->left = (yyvsp[-2].condition_node);
            condition->right = (yyvsp[0].condition_node);
            (yyval.condition_node) = condition;
        }
#line 3017 "y.tab.c"
    break;

  case 132: /* condition: '(' condition ')'  */
#line 1105 "sql.y"
        {
            (yyval.condition_node) = (yyvsp[-1].condition_node);
        }
#line 3025 "y.tab.c"
    break;

  case 133: /* condition: predicate  */
#line 1109 "sql.y"
        {
            ConditionNode *condition = instance(ConditionNode);
            condition->conn_type = C_NONE;
            condition->predicate = (yyvsp[0].predicate_node);
            (yyval.condition_node) = condition;
        }
#line 3036 "y.tab.c"
    break;

  case 134: /* predicate: comparison_predicate  */
#line 1118 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_COMPARISON;
            predicate->comparison = (yyvsp[0].comparison_node);
            (yyval.predicate_node) = predicate;
        }
#line 3047 "y.tab.c"
    break;

  case 135: /* predicate: like_predicate  */
#line 1125 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_LIKE;
            predicate->like = (yyvsp[0].like_node);
            (yyval.predicate_node) = predicate;
        }
#line 3058 "y.tab.c"
    break;

  case 136: /* predicate: in_predicate  */
#line 1132 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_IN;
            predicate->in = (yyvsp[0].in_node);
            (yyval.predicate_node) = predicate;
        }
#line 3069 "y.tab.c"
    break;

  case 137: /* comparison_predicate: column compare scalar_exp  */
#line 1141 "sql.y"
        {
            ComparisonNode *comparison_node = instance(ComparisonNode);
            comparison_node->column = (yyvsp[-2].column_node);
            comparison_node->type = (yyvsp[-1].compare_type);
            comparison_node->value = (yyvsp[0].scalar_exp_node);
            (yyval.comparison_node) = comparison_node;
        }
#line 3081 "y.tab.c"
    break;

  case 138: /* like_predicate: column LIKE value_item  */
#line 1151 "sql.y"
        {
            LikeNode *like_node = instance(LikeNode);
            like_node->column = (yyvsp[-2].column_node);
            like_node->value = (yyvsp[0].value_item_node);
            (yyval.like_node) = like_node;
        }
#line 3092 "y.tab.c"
    break;

  case 139: /* in_predicate: column IN '(' value_items ')'  */
#line 1160 "sql.y"
        {
            InNode *in_node = instance(InNode);
            in_node->column = (yyvsp[-4].column_node);
            in_node->value_list = (yyvsp[-1].list);
            (yyval.in_node) = in_node;
        }
#line 3103 "y.tab.c"
    break;

  case 140: /* compare: EQ  */
#line 1188 "sql.y"
            { (yyval.compare_type) = O_EQ; }
#line 3109 "y.tab.c"
    break;

  case 141: /* compare: NE  */
#line 1189 "sql.y"
            { (yyval.compare_type) = O_NE; }
#line 3115 "y.tab.c"
    break;

  case 142: /* compare: GT  */
#line 1190 "sql.y"
            { (yyval.compare_type) = O_GT; }
#line 3121 "y.tab.c"
    break;

  case 143: /* compare: GE  */
#line 1191 "sql.y"
            { (yyval.compare_type) = O_GE; }
#line 3127 "y.tab.c"
    break;

  case 144: /* compare: LT  */
#line 1192 "sql.y"
            { (yyval.compare_type) = O_LT; }
#line 3133 "y.tab.c"
    break;

  case 145: /* compare: LE  */
#line 1193 "sql.y"
            { (yyval.compare_type) = O_LE; }
#line 3139 "y.tab.c"
    break;

  case 146: /* function: MAX '(' non_all_function_value ')'  */
#line 1197 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MAX;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3150 "y.tab.c"
    break;

  case 147: /* function: MIN '(' non_all_function_value ')'  */
#line 1204 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MIN;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3161 "y.tab.c"
    break;

  case 148: /* function: COUNT '(' function_value ')'  */
#line 1211 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_COUNT;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3172 "y.tab.c"
    break;

  case 149: /* function: SUM '(' function_value ')'  */
#line 1218 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_SUM;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3183 "y.tab.c"
    break;

  case 150: /* function: AVG '(' function_value ')'  */
#line 1225 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_AVG;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3194 "y.tab.c"
    break;

  case 151: /* function_value: INTVALUE  */
#line 1234 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3205 "y.tab.c"
    break;

  case 152: /* function_value: column  */
#line 1241 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3216 "y.tab.c"
    break;

  case 153: /* function_value: '*'  */
#line 1248 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->value_type = V_ALL;
            (yyval.function_value_node) = node;
        }
#line 3226 "y.tab.c"
    break;

  case 154: /* non_all_function_value: INTVALUE  */
#line 1256 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3237 "y.tab.c"
    break;

  case 155: /* non_all_function_value: column  */
#line 1263 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3248 "y.tab.c"
    break;


#line 3252 "y.tab.c"

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
      yyerror (states, YY_("syntax error"));
    }

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
                      yytoken, &yylval, states);
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


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, states);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


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
                  yytoken, &yylval, states);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, states);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 1274 "sql.y"

