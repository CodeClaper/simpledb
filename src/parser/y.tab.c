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
   DropIndexNode                *drop_index_node;
   SelectNode                   *select_node;
   InsertNode                   *insert_node;
   UpdateNode                   *update_node;
   DeleteNode                   *delete_node;
   DescribeNode                 *describe_node;
   ShowNode                     *show_node;
   AlterTableNode               *alter_table_node;
   Statement                    *statement;
   List                         *list;

#line 358 "y.tab.c"

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
  YYSYMBOL_INDEX = 30,                     /* INDEX  */
  YYSYMBOL_LIMIT = 31,                     /* LIMIT  */
  YYSYMBOL_OFFSET = 32,                    /* OFFSET  */
  YYSYMBOL_SHOW = 33,                      /* SHOW  */
  YYSYMBOL_TABLES = 34,                    /* TABLES  */
  YYSYMBOL_PRIMARY = 35,                   /* PRIMARY  */
  YYSYMBOL_KEY = 36,                       /* KEY  */
  YYSYMBOL_UNIQUE = 37,                    /* UNIQUE  */
  YYSYMBOL_DEFAULT = 38,                   /* DEFAULT  */
  YYSYMBOL_CHECK = 39,                     /* CHECK  */
  YYSYMBOL_REFERENCES = 40,                /* REFERENCES  */
  YYSYMBOL_FOREIGN = 41,                   /* FOREIGN  */
  YYSYMBOL_MAX = 42,                       /* MAX  */
  YYSYMBOL_MIN = 43,                       /* MIN  */
  YYSYMBOL_COUNT = 44,                     /* COUNT  */
  YYSYMBOL_SUM = 45,                       /* SUM  */
  YYSYMBOL_AVG = 46,                       /* AVG  */
  YYSYMBOL_REF = 47,                       /* REF  */
  YYSYMBOL_TRUE = 48,                      /* TRUE  */
  YYSYMBOL_FALSE = 49,                     /* FALSE  */
  YYSYMBOL_NULLX = 50,                     /* NULLX  */
  YYSYMBOL_AS = 51,                        /* AS  */
  YYSYMBOL_COMMENT = 52,                   /* COMMENT  */
  YYSYMBOL_CHAR = 53,                      /* CHAR  */
  YYSYMBOL_INT = 54,                       /* INT  */
  YYSYMBOL_LONG = 55,                      /* LONG  */
  YYSYMBOL_VARCHAR = 56,                   /* VARCHAR  */
  YYSYMBOL_STRING = 57,                    /* STRING  */
  YYSYMBOL_BOOL = 58,                      /* BOOL  */
  YYSYMBOL_FLOAT = 59,                     /* FLOAT  */
  YYSYMBOL_DOUBLE = 60,                    /* DOUBLE  */
  YYSYMBOL_DATE = 61,                      /* DATE  */
  YYSYMBOL_TIMESTAMP = 62,                 /* TIMESTAMP  */
  YYSYMBOL_EQ = 63,                        /* EQ  */
  YYSYMBOL_NE = 64,                        /* NE  */
  YYSYMBOL_GT = 65,                        /* GT  */
  YYSYMBOL_GE = 66,                        /* GE  */
  YYSYMBOL_LT = 67,                        /* LT  */
  YYSYMBOL_LE = 68,                        /* LE  */
  YYSYMBOL_IN = 69,                        /* IN  */
  YYSYMBOL_LIKE = 70,                      /* LIKE  */
  YYSYMBOL_IS = 71,                        /* IS  */
  YYSYMBOL_NOT = 72,                       /* NOT  */
  YYSYMBOL_ALTER = 73,                     /* ALTER  */
  YYSYMBOL_COLUMN = 74,                    /* COLUMN  */
  YYSYMBOL_ADD = 75,                       /* ADD  */
  YYSYMBOL_RENAME = 76,                    /* RENAME  */
  YYSYMBOL_ON = 77,                        /* ON  */
  YYSYMBOL_BEFORE = 78,                    /* BEFORE  */
  YYSYMBOL_AFTER = 79,                     /* AFTER  */
  YYSYMBOL_SYSTEM = 80,                    /* SYSTEM  */
  YYSYMBOL_CONFIG = 81,                    /* CONFIG  */
  YYSYMBOL_MEMORY = 82,                    /* MEMORY  */
  YYSYMBOL_IDENTIFIER = 83,                /* IDENTIFIER  */
  YYSYMBOL_INTVALUE = 84,                  /* INTVALUE  */
  YYSYMBOL_FLOATVALUE = 85,                /* FLOATVALUE  */
  YYSYMBOL_STRINGVALUE = 86,               /* STRINGVALUE  */
  YYSYMBOL_87_ = 87,                       /* ','  */
  YYSYMBOL_88_ = 88,                       /* '.'  */
  YYSYMBOL_89_ = 89,                       /* '{'  */
  YYSYMBOL_90_ = 90,                       /* '}'  */
  YYSYMBOL_91_ = 91,                       /* ';'  */
  YYSYMBOL_YYACCEPT = 92,                  /* $accept  */
  YYSYMBOL_statements = 93,                /* statements  */
  YYSYMBOL_statement = 94,                 /* statement  */
  YYSYMBOL_begin_transaction_statement = 95, /* begin_transaction_statement  */
  YYSYMBOL_commit_transaction_statement = 96, /* commit_transaction_statement  */
  YYSYMBOL_rollback_transaction_statement = 97, /* rollback_transaction_statement  */
  YYSYMBOL_create_table_statement = 98,    /* create_table_statement  */
  YYSYMBOL_create_index_statement = 99,    /* create_index_statement  */
  YYSYMBOL_drop_table_statement = 100,     /* drop_table_statement  */
  YYSYMBOL_drop_index_statement = 101,     /* drop_index_statement  */
  YYSYMBOL_select_statement = 102,         /* select_statement  */
  YYSYMBOL_insert_statement = 103,         /* insert_statement  */
  YYSYMBOL_update_statement = 104,         /* update_statement  */
  YYSYMBOL_delete_statement = 105,         /* delete_statement  */
  YYSYMBOL_describe_statement = 106,       /* describe_statement  */
  YYSYMBOL_show_statement = 107,           /* show_statement  */
  YYSYMBOL_alter_table_statement = 108,    /* alter_table_statement  */
  YYSYMBOL_alter_table_action = 109,       /* alter_table_action  */
  YYSYMBOL_add_column_def = 110,           /* add_column_def  */
  YYSYMBOL_drop_column_def = 111,          /* drop_column_def  */
  YYSYMBOL_column_position_def = 112,      /* column_position_def  */
  YYSYMBOL_selection = 113,                /* selection  */
  YYSYMBOL_table_exp = 114,                /* table_exp  */
  YYSYMBOL_from_clause = 115,              /* from_clause  */
  YYSYMBOL_table_ref_commalist = 116,      /* table_ref_commalist  */
  YYSYMBOL_table_ref = 117,                /* table_ref  */
  YYSYMBOL_table = 118,                    /* table  */
  YYSYMBOL_index_name = 119,               /* index_name  */
  YYSYMBOL_range_variable = 120,           /* range_variable  */
  YYSYMBOL_opt_where_clause = 121,         /* opt_where_clause  */
  YYSYMBOL_where_clause = 122,             /* where_clause  */
  YYSYMBOL_values_or_query_spec = 123,     /* values_or_query_spec  */
  YYSYMBOL_opt_values = 124,               /* opt_values  */
  YYSYMBOL_query_spec = 125,               /* query_spec  */
  YYSYMBOL_scalar_exp_commalist = 126,     /* scalar_exp_commalist  */
  YYSYMBOL_scalar_exp = 127,               /* scalar_exp  */
  YYSYMBOL_calculate = 128,                /* calculate  */
  YYSYMBOL_columns = 129,                  /* columns  */
  YYSYMBOL_base_table_element_commalist = 130, /* base_table_element_commalist  */
  YYSYMBOL_base_table_element = 131,       /* base_table_element  */
  YYSYMBOL_column_def = 132,               /* column_def  */
  YYSYMBOL_column_def_name_commalist = 133, /* column_def_name_commalist  */
  YYSYMBOL_column_def_name = 134,          /* column_def_name  */
  YYSYMBOL_data_type = 135,                /* data_type  */
  YYSYMBOL_array_dim_clause = 136,         /* array_dim_clause  */
  YYSYMBOL_column_def_opt_list = 137,      /* column_def_opt_list  */
  YYSYMBOL_column_def_opt = 138,           /* column_def_opt  */
  YYSYMBOL_table_contraint_def = 139,      /* table_contraint_def  */
  YYSYMBOL_column = 140,                   /* column  */
  YYSYMBOL_value_items = 141,              /* value_items  */
  YYSYMBOL_value_item = 142,               /* value_item  */
  YYSYMBOL_atom = 143,                     /* atom  */
  YYSYMBOL_REFERVALUE = 144,               /* REFERVALUE  */
  YYSYMBOL_BOOLVALUE = 145,                /* BOOLVALUE  */
  YYSYMBOL_assignments = 146,              /* assignments  */
  YYSYMBOL_assignment = 147,               /* assignment  */
  YYSYMBOL_search_condition = 148,         /* search_condition  */
  YYSYMBOL_boolean_term = 149,             /* boolean_term  */
  YYSYMBOL_boolean_factor = 150,           /* boolean_factor  */
  YYSYMBOL_boolean_test = 151,             /* boolean_test  */
  YYSYMBOL_boolean_primary = 152,          /* boolean_primary  */
  YYSYMBOL_predicate = 153,                /* predicate  */
  YYSYMBOL_comparison_predicate = 154,     /* comparison_predicate  */
  YYSYMBOL_like_predicate = 155,           /* like_predicate  */
  YYSYMBOL_in_predicate = 156,             /* in_predicate  */
  YYSYMBOL_limit_clause = 157,             /* limit_clause  */
  YYSYMBOL_compare = 158,                  /* compare  */
  YYSYMBOL_function = 159,                 /* function  */
  YYSYMBOL_function_value = 160,           /* function_value  */
  YYSYMBOL_non_all_function_value = 161,   /* non_all_function_value  */
  YYSYMBOL_end = 162                       /* end  */
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
#define YYFINAL  72
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   498

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  92
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  71
/* YYNRULES -- Number of rules.  */
#define YYNRULES  174
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  348

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   333


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
       9,    10,     7,     5,    87,     6,    88,     8,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    91,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    11,     2,    12,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    89,     2,    90,     2,     2,     2,     2,
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
      83,    84,    85,    86
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   190,   190,   195,   202,   208,   214,   220,   227,   234,
     241,   248,   255,   262,   269,   276,   283,   290,   299,   302,
     305,   309,   319,   329,   342,   351,   360,   370,   378,   390,
     401,   407,   417,   426,   432,   442,   450,   457,   466,   475,
     484,   487,   494,   503,   510,   518,   529,   532,   540,   546,
     553,   559,   566,   575,   581,   587,   594,   597,   603,   611,
     618,   627,   632,   639,   648,   654,   661,   668,   675,   682,
     689,   693,   700,   708,   716,   724,   734,   740,   747,   753,
     760,   768,   791,   802,   808,   815,   823,   829,   835,   841,
     848,   854,   860,   866,   872,   878,   884,   894,   897,   901,
     908,   911,   916,   923,   929,   935,   941,   948,   954,   961,
     968,   977,   984,   991,   999,  1008,  1015,  1023,  1031,  1038,
    1044,  1051,  1058,  1064,  1073,  1080,  1087,  1094,  1101,  1111,
    1119,  1128,  1132,  1138,  1144,  1151,  1160,  1166,  1175,  1181,
    1190,  1197,  1206,  1213,  1221,  1231,  1239,  1248,  1255,  1262,
    1271,  1281,  1290,  1300,  1303,  1310,  1317,  1326,  1327,  1328,
    1329,  1330,  1331,  1334,  1341,  1348,  1355,  1362,  1371,  1378,
    1385,  1393,  1400,  1409,  1410
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
  "DESCRIBE", "FROM", "WHERE", "INTO", "SET", "VALUES", "TABLE", "INDEX",
  "LIMIT", "OFFSET", "SHOW", "TABLES", "PRIMARY", "KEY", "UNIQUE",
  "DEFAULT", "CHECK", "REFERENCES", "FOREIGN", "MAX", "MIN", "COUNT",
  "SUM", "AVG", "REF", "TRUE", "FALSE", "NULLX", "AS", "COMMENT", "CHAR",
  "INT", "LONG", "VARCHAR", "STRING", "BOOL", "FLOAT", "DOUBLE", "DATE",
  "TIMESTAMP", "EQ", "NE", "GT", "GE", "LT", "LE", "IN", "LIKE", "IS",
  "NOT", "ALTER", "COLUMN", "ADD", "RENAME", "ON", "BEFORE", "AFTER",
  "SYSTEM", "CONFIG", "MEMORY", "IDENTIFIER", "INTVALUE", "FLOATVALUE",
  "STRINGVALUE", "','", "'.'", "'{'", "'}'", "';'", "$accept",
  "statements", "statement", "begin_transaction_statement",
  "commit_transaction_statement", "rollback_transaction_statement",
  "create_table_statement", "create_index_statement",
  "drop_table_statement", "drop_index_statement", "select_statement",
  "insert_statement", "update_statement", "delete_statement",
  "describe_statement", "show_statement", "alter_table_statement",
  "alter_table_action", "add_column_def", "drop_column_def",
  "column_position_def", "selection", "table_exp", "from_clause",
  "table_ref_commalist", "table_ref", "table", "index_name",
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

#define YYPACT_NINF (-220)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-120)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     425,   -51,   -51,   -51,   124,   126,   156,   121,   -21,   128,
     -21,   134,    35,   281,  -220,  -220,  -220,  -220,  -220,  -220,
    -220,  -220,  -220,  -220,  -220,  -220,  -220,  -220,  -220,   137,
    -220,  -220,  -220,   -21,    98,   153,   -21,    98,  -220,   299,
      74,   177,   188,   202,   205,   207,   211,  -220,  -220,  -220,
     107,  -220,  -220,  -220,   199,   148,   138,  -220,  -220,  -220,
    -220,  -220,  -220,  -220,   -21,  -220,   209,   -21,   -51,   214,
     -51,   -21,  -220,  -220,  -220,   220,  -220,   175,    98,   -51,
     -51,     5,    37,    -3,   166,    74,    17,  -220,     8,     8,
      14,    14,    14,   201,    29,   324,   -21,   -51,   237,   324,
     324,   324,   324,   324,   183,    51,    29,    -1,  -220,   -21,
    -220,    53,   178,   -21,   187,  -220,  -220,   179,  -220,  -220,
      74,  -220,   185,  -220,  -220,   255,   259,  -220,  -220,  -220,
     260,   262,   264,   279,   344,   370,   162,    36,   271,  -220,
    -220,   208,  -220,  -220,  -220,  -220,  -220,    45,   189,  -220,
     -10,  -220,   201,   247,  -220,   138,   129,   129,   229,   229,
    -220,    29,   156,   273,   -51,  -220,   226,    -5,  -220,   201,
    -220,   -51,   231,   232,   -51,  -220,  -220,   257,   274,   285,
     275,  -220,     9,  -220,  -220,   406,  -220,   300,   -21,    29,
    -220,   302,  -220,  -220,  -220,  -220,  -220,   351,    64,  -220,
    -220,  -220,  -220,  -220,  -220,  -220,   324,   304,    74,   201,
    -220,   201,   122,  -220,   -21,   248,  -220,  -220,   327,   250,
    -220,    21,  -220,   199,    74,   249,  -220,    74,    29,   -51,
      15,  -220,   267,   277,  -220,   329,   277,   201,   331,   -51,
     178,  -220,  -220,  -220,   371,  -220,  -220,  -220,  -220,  -220,
    -220,  -220,   368,    29,   372,  -220,  -220,   138,    74,  -220,
     271,  -220,   159,  -220,  -220,  -220,     0,   123,    29,  -220,
      23,   386,  -220,  -220,  -220,  -220,  -220,   147,   277,    26,
    -220,   181,   277,  -220,  -220,   312,   385,   219,    27,    29,
      38,  -220,   314,   315,   -51,  -220,  -220,    74,   317,   318,
    -220,    39,  -220,   277,  -220,    40,   393,  -220,   392,   369,
    -220,    91,   397,   -21,   325,   362,   280,  -220,   -51,    46,
    -220,  -220,  -220,  -220,    47,  -220,  -220,  -220,  -220,   373,
    -220,  -220,  -220,  -220,  -220,   201,  -220,  -220,  -220,  -220,
    -220,   -51,  -220,   -21,   182,  -220,  -220,  -220
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     2,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,   173,
      18,    19,    20,     0,     0,     0,     0,     0,    44,     0,
       0,     0,     0,     0,     0,     0,     0,   131,   132,   122,
     115,   124,   127,   126,    46,    43,    64,    66,    67,    69,
     121,   128,   125,    68,     0,    53,     0,     0,     0,     0,
       0,     0,     1,     3,   174,     0,    54,     0,     0,     0,
       0,   115,     0,     0,    69,     0,     0,   119,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    56,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    32,     0,
      33,     0,     0,     0,     0,    24,    25,     0,    70,   129,
       0,   123,     0,   171,   172,     0,     0,   170,   168,   169,
       0,     0,     0,     0,     0,     0,    67,     0,   136,   138,
     140,   142,   145,   147,   148,   149,   118,     0,    47,    48,
      50,    26,     0,   153,    57,    65,    72,    73,    74,    75,
      71,     0,     0,     0,     0,    60,     0,    56,   133,     0,
      30,     0,     0,     0,     0,    36,    37,     0,     0,     0,
       0,    85,     0,    78,    80,     0,    81,     0,     0,     0,
     120,     0,   163,   164,   165,   166,   167,     0,     0,   141,
     157,   158,   159,   160,   161,   162,     0,     0,     0,     0,
     130,     0,     0,   117,     0,     0,    55,    51,    58,     0,
      45,     0,    76,    46,     0,    59,    27,     0,     0,     0,
       0,    34,     0,     0,    35,     0,     0,     0,     0,     0,
       0,    88,    86,    87,     0,    90,    91,    92,    93,    95,
      94,    96,    97,     0,     0,   116,   146,   150,     0,   151,
     137,   139,     0,   143,    49,    52,   154,     0,     0,    63,
       0,     0,   135,   134,    29,    31,    39,    40,     0,     0,
      83,     0,     0,    21,    79,     0,     0,   100,     0,     0,
       0,   144,     0,     0,     0,    77,    61,     0,     0,     0,
      38,     0,   111,     0,   114,     0,     0,    98,     0,     0,
     104,     0,     0,     0,     0,     0,    82,   101,     0,     0,
     152,   156,   155,    28,     0,    41,    42,   112,    84,     0,
      89,    99,   105,   107,   106,     0,   110,   108,   103,   102,
      22,     0,    62,     0,     0,    23,   113,   109
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -220,  -220,   407,  -220,  -220,  -220,  -220,  -220,  -220,  -220,
    -220,  -220,  -220,  -220,  -220,  -220,  -220,  -220,  -220,  -220,
    -220,   261,   203,  -220,  -220,   210,    -6,    -9,   216,   258,
    -220,   155,  -220,  -220,   337,    16,  -220,  -123,  -220,   212,
     217,  -219,  -202,  -220,  -220,  -220,   133,  -220,   -80,   -37,
     -34,  -220,  -220,  -187,  -220,   223,  -117,   244,   243,   321,
    -220,  -220,  -220,  -220,  -220,  -220,  -220,  -220,   142,   367,
      -2
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,   174,   175,   176,
     300,    54,    97,    98,   148,   149,   150,    77,   217,   153,
     154,   164,   225,   165,    55,   135,    57,   221,   182,   183,
     184,   279,   185,   252,   287,   316,   317,   186,    58,    83,
      59,    60,    61,    62,   167,   168,   137,   138,   139,   140,
     141,   142,   143,   144,   145,   220,   206,    63,   130,   125,
      30
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      31,    32,    66,    86,    68,    84,    87,   119,   124,   124,
     129,   129,   129,   136,   146,   117,   198,   122,   209,   239,
     152,   127,    56,   122,   169,   263,   166,    75,    80,   121,
      79,   267,   292,   296,   280,   218,   302,   318,   122,   209,
      29,   215,   100,   101,   102,   103,   210,   118,   320,   327,
     329,    87,   230,   136,   136,    82,   341,   342,   105,   301,
     161,   107,    65,   305,    71,   111,   108,   209,   110,   114,
     162,   172,   136,   216,   256,   291,   280,   115,   116,   163,
     280,   222,   228,    85,   120,    40,   190,   293,   104,   136,
      29,    50,   123,    94,    95,   151,   240,    50,   128,    84,
      85,   328,    40,   171,   120,   170,    29,   187,   268,   255,
     120,    56,    50,   303,   268,   155,   156,   157,   158,   159,
     281,    46,    47,    48,    49,   120,   303,   303,   173,   136,
     288,   136,    99,   268,   120,   213,   102,   103,    46,    47,
      48,   333,   162,   100,   101,   102,   103,    64,   166,   197,
      74,   163,    67,    33,    34,    36,    37,   136,    51,    52,
      53,    35,   226,    38,    69,    39,   319,    40,    70,   231,
      47,    48,   234,   222,   259,    51,    52,    53,    56,   251,
     104,    76,   254,    78,   209,   209,    88,   270,   295,   104,
      87,   304,   347,   272,   262,    94,    95,    89,    41,    42,
      43,    44,    45,    46,    47,    48,    49,    47,    48,   222,
     133,    90,    40,   177,    91,   178,    92,   179,   344,   180,
      93,   290,   257,    96,    87,   298,   299,   274,   275,   112,
     308,   207,   208,   131,   132,    99,   106,   283,   109,    50,
      51,    52,    53,    41,    42,    43,    44,    45,    46,    47,
      48,    49,   113,  -119,   309,   136,   310,   311,   312,   313,
     324,   181,   152,    87,   188,   192,   160,   189,   191,   193,
     194,   314,   195,   134,   196,   211,   214,   334,   219,   212,
     104,    72,   224,   236,    50,    51,    52,    53,   133,   227,
      40,   315,   323,   235,   237,     1,     2,     3,     4,     5,
       6,     7,     8,     9,    10,   232,   233,   336,    39,   253,
      40,   238,   117,   258,    11,   309,   340,   310,   311,   312,
     313,    41,    42,    43,    44,    45,    46,    47,    48,    49,
     209,   216,   314,    39,   266,    40,   271,   346,   278,   345,
     282,    41,    42,    43,    44,    45,    46,    47,    48,    49,
     276,   134,   315,   133,    12,    40,   100,   101,   102,   103,
     181,   118,    81,    51,    52,    53,    41,    42,    43,    44,
      45,    46,    47,    48,    49,   100,   101,   102,   103,   286,
     285,   289,    81,    51,    52,    53,    41,    42,    43,    44,
      45,    46,    47,    48,    49,   297,   306,   307,   321,   322,
     325,   326,   104,   330,   331,   332,   335,    50,    51,    52,
      53,   337,   338,   343,   200,   201,   202,   203,   204,   205,
      73,   104,   294,   223,   264,   229,   269,    50,    51,    52,
      53,   265,   147,   200,   201,   202,   203,   204,   205,     1,
       2,     3,     4,     5,     6,     7,     8,     9,    10,   339,
     277,   273,   284,   260,   261,   199,   126,     0,    11,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    65,
       0,     0,     0,     0,     0,     0,     0,     0,    12
};

static const yytype_int16 yycheck[] =
{
       2,     3,     8,    40,    10,    39,    40,    10,    88,    89,
      90,    91,    92,    93,    94,    10,   133,     9,     3,    10,
      25,     7,     6,     9,    25,   212,   106,    33,    37,    12,
      36,    10,    32,    10,   236,   152,    10,    10,     9,     3,
      91,    51,     5,     6,     7,     8,    10,    10,    10,    10,
      10,    85,   169,   133,   134,    39,    10,    10,    64,   278,
       9,    67,    83,   282,    29,    71,    68,     3,    70,    78,
      19,    18,   152,    83,    10,   262,   278,    79,    80,    28,
     282,   161,    87,     9,    87,    11,   120,    87,    51,   169,
      91,    83,    84,    88,    89,    97,    87,    83,    84,   133,
       9,   303,    11,   109,    87,   107,    91,   113,    87,   189,
      87,    95,    83,    87,    87,    99,   100,   101,   102,   103,
     237,    47,    48,    49,    50,    87,    87,    87,    75,   209,
     253,   211,    87,    87,    87,    90,     7,     8,    47,    48,
      49,    50,    19,     5,     6,     7,     8,    26,   228,   133,
      13,    28,    24,    29,    30,    29,    30,   237,    84,    85,
      86,    37,   164,     7,    30,     9,   289,    11,    34,   171,
      48,    49,   174,   253,   208,    84,    85,    86,   162,   185,
      51,    83,   188,    30,     3,     3,     9,   224,   268,    51,
     224,    10,    10,   227,    72,    88,    89,     9,    42,    43,
      44,    45,    46,    47,    48,    49,    50,    48,    49,   289,
       9,     9,    11,    35,     9,    37,     9,    39,   335,    41,
       9,   258,   206,    24,   258,    78,    79,   229,   230,     9,
      11,    69,    70,    91,    92,    87,    27,   239,    24,    83,
      84,    85,    86,    42,    43,    44,    45,    46,    47,    48,
      49,    50,    77,    87,    35,   335,    37,    38,    39,    40,
     297,    83,    25,   297,    77,    10,    83,    88,    83,    10,
      10,    52,    10,    72,    10,     4,    87,   311,    31,    71,
      51,     0,     9,     9,    83,    84,    85,    86,     9,    63,
      11,    72,   294,    36,     9,    14,    15,    16,    17,    18,
      19,    20,    21,    22,    23,    74,    74,   313,     9,     9,
      11,    36,    10,     9,    33,    35,   318,    37,    38,    39,
      40,    42,    43,    44,    45,    46,    47,    48,    49,    50,
       3,    83,    52,     9,    84,    11,    87,   343,     9,   341,
       9,    42,    43,    44,    45,    46,    47,    48,    49,    50,
      83,    72,    72,     9,    73,    11,     5,     6,     7,     8,
      83,    10,    83,    84,    85,    86,    42,    43,    44,    45,
      46,    47,    48,    49,    50,     5,     6,     7,     8,    11,
       9,     9,    83,    84,    85,    86,    42,    43,    44,    45,
      46,    47,    48,    49,    50,     9,    84,    12,    84,    84,
      83,    83,    51,    10,    12,    36,     9,    83,    84,    85,
      86,    86,    50,    40,    63,    64,    65,    66,    67,    68,
      13,    51,   267,   162,   214,   167,   223,    83,    84,    85,
      86,   215,    95,    63,    64,    65,    66,    67,    68,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,   316,
     233,   228,   240,   209,   211,   134,    89,    -1,    33,    53,
      54,    55,    56,    57,    58,    59,    60,    61,    62,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    83,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    73
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    33,    73,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,    91,
     162,   162,   162,    29,    30,    37,    29,    30,     7,     9,
      11,    42,    43,    44,    45,    46,    47,    48,    49,    50,
      83,    84,    85,    86,   113,   126,   127,   128,   140,   142,
     143,   144,   145,   159,    26,    83,   118,    24,   118,    30,
      34,    29,     0,    94,    13,   118,    83,   119,    30,   118,
     119,    83,   127,   141,   142,     9,   141,   142,     9,     9,
       9,     9,     9,     9,    88,    89,    24,   114,   115,    87,
       5,     6,     7,     8,    51,   118,    27,   118,   162,    24,
     162,   118,     9,    77,   119,   162,   162,    10,    10,    10,
      87,    12,     9,    84,   140,   161,   161,     7,    84,   140,
     160,   160,   160,     9,    72,   127,   140,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   140,   126,   116,   117,
     118,   162,    25,   121,   122,   127,   127,   127,   127,   127,
      83,     9,    19,    28,   123,   125,   140,   146,   147,    25,
     162,   118,    18,    75,   109,   110,   111,    35,    37,    39,
      41,    83,   130,   131,   132,   134,   139,   118,    77,    88,
     142,    83,    10,    10,    10,    10,    10,   127,   148,   151,
      63,    64,    65,    66,    67,    68,   158,    69,    70,     3,
      10,     4,    71,    90,    87,    51,    83,   120,   148,    31,
     157,   129,   140,   113,     9,   124,   162,    63,    87,   121,
     148,   162,    74,    74,   162,    36,     9,     9,    36,    10,
      87,    53,    54,    55,    56,    57,    58,    59,    60,    61,
      62,   118,   135,     9,   118,   140,    10,   127,     9,   142,
     149,   150,    72,   145,   117,   120,    84,    10,    87,   114,
     141,    87,   142,   147,   162,   162,    83,   132,     9,   133,
     134,   148,     9,   162,   131,     9,    11,   136,   129,     9,
     141,   145,    32,    87,   123,   140,    10,     9,    78,    79,
     112,   133,    10,    87,    10,   133,    84,    12,    11,    35,
      37,    38,    39,    40,    52,    72,   137,   138,    10,   129,
      10,    84,    84,   162,   141,    83,    83,    10,   134,    10,
      10,    12,    36,    50,   142,     9,   118,    86,    50,   138,
     162,    10,    10,    40,   148,   162,   118,    10
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,    92,    93,    93,    94,    94,    94,    94,    94,    94,
      94,    94,    94,    94,    94,    94,    94,    94,    95,    96,
      97,    98,    99,    99,   100,   101,   102,   103,   103,   104,
     105,   105,   106,   107,   107,   108,   109,   109,   110,   111,
     112,   112,   112,   113,   113,   114,   115,   115,   116,   116,
     117,   117,   117,   118,   119,   120,   121,   121,   122,   123,
     123,   124,   124,   125,   126,   126,   127,   127,   127,   127,
     127,   127,   128,   128,   128,   128,   129,   129,   130,   130,
     131,   131,   132,   133,   133,   134,   135,   135,   135,   135,
     135,   135,   135,   135,   135,   135,   135,   136,   136,   136,
     137,   137,   137,   138,   138,   138,   138,   138,   138,   138,
     138,   139,   139,   139,   139,   140,   140,   140,   140,   141,
     141,   142,   142,   142,   143,   143,   143,   143,   143,   144,
     144,   145,   145,   146,   146,   147,   148,   148,   149,   149,
     150,   150,   151,   151,   151,   152,   152,   153,   153,   153,
     154,   155,   156,   157,   157,   157,   157,   158,   158,   158,
     158,   158,   158,   159,   159,   159,   159,   159,   160,   160,
     160,   161,   161,   162,   162
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     2,     2,
       2,     7,     9,    10,     4,     4,     4,     5,     8,     6,
       4,     6,     3,     3,     5,     5,     1,     1,     4,     3,
       0,     2,     2,     1,     1,     3,     0,     2,     1,     3,
       1,     2,     3,     1,     1,     1,     0,     1,     2,     2,
       1,     3,     5,     3,     1,     3,     1,     1,     1,     1,
       3,     3,     3,     3,     3,     3,     1,     3,     1,     3,
       1,     1,     4,     1,     3,     1,     1,     1,     1,     4,
       1,     1,     1,     1,     1,     1,     1,     0,     2,     3,
       0,     1,     2,     2,     1,     2,     2,     2,     2,     4,
       2,     4,     5,     7,     4,     1,     5,     4,     3,     1,
       3,     1,     1,     3,     1,     1,     1,     1,     1,     3,
       4,     1,     1,     1,     3,     3,     1,     3,     1,     3,
       1,     2,     1,     3,     4,     1,     3,     1,     1,     1,
       3,     3,     5,     0,     2,     4,     4,     1,     1,     1,
       1,     1,     1,     4,     4,     4,     4,     4,     1,     1,
       1,     1,     1,     1,     2
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
#line 191 "sql.y"
        {
            append_list(states, (yyvsp[0].statement));
            (yyval.list) = states;
        }
#line 2143 "y.tab.c"
    break;

  case 3: /* statements: statements statement  */
#line 196 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].statement));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 2152 "y.tab.c"
    break;

  case 4: /* statement: begin_transaction_statement  */
#line 203 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = BEGIN_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2162 "y.tab.c"
    break;

  case 5: /* statement: commit_transaction_statement  */
#line 209 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = COMMIT_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2172 "y.tab.c"
    break;

  case 6: /* statement: rollback_transaction_statement  */
#line 215 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ROLLBACK_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2182 "y.tab.c"
    break;

  case 7: /* statement: create_table_statement  */
#line 221 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_TABLE_STMT;
            statement->create_table_node = (yyvsp[0].create_table_node);
            (yyval.statement) = statement;
        }
#line 2193 "y.tab.c"
    break;

  case 8: /* statement: create_index_statement  */
#line 228 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_INDEX_STMT;
            statement->create_index_node = (yyvsp[0].create_index_node);
            (yyval.statement) = statement;
        }
#line 2204 "y.tab.c"
    break;

  case 9: /* statement: drop_table_statement  */
#line 235 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_TABLE_STMT;
            statement->drop_table_node = (yyvsp[0].drop_table_node);
            (yyval.statement) = statement;
        }
#line 2215 "y.tab.c"
    break;

  case 10: /* statement: drop_index_statement  */
#line 242 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_INDEX_STMT;
            statement->drop_index_node = (yyvsp[0].drop_index_node);
            (yyval.statement) = statement;
        }
#line 2226 "y.tab.c"
    break;

  case 11: /* statement: select_statement  */
#line 249 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SELECT_STMT;
            statement->select_node = (yyvsp[0].select_node);
            (yyval.statement) = statement;
        }
#line 2237 "y.tab.c"
    break;

  case 12: /* statement: insert_statement  */
#line 256 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = INSERT_STMT;
            statement->insert_node = (yyvsp[0].insert_node);
            (yyval.statement) = statement;
        }
#line 2248 "y.tab.c"
    break;

  case 13: /* statement: update_statement  */
#line 263 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = UPDATE_STMT;
            statement->update_node = (yyvsp[0].update_node);
            (yyval.statement) = statement;
        }
#line 2259 "y.tab.c"
    break;

  case 14: /* statement: delete_statement  */
#line 270 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DELETE_STMT;
            statement->delete_node = (yyvsp[0].delete_node);
            (yyval.statement) = statement;
        }
#line 2270 "y.tab.c"
    break;

  case 15: /* statement: describe_statement  */
#line 277 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DESCRIBE_STMT;
            statement->describe_node = (yyvsp[0].describe_node);
            (yyval.statement) = statement;
        }
#line 2281 "y.tab.c"
    break;

  case 16: /* statement: show_statement  */
#line 284 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SHOW_STMT;
            statement->show_node = (yyvsp[0].show_node);
            (yyval.statement) = statement;
        }
#line 2292 "y.tab.c"
    break;

  case 17: /* statement: alter_table_statement  */
#line 291 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ALTER_TABLE_STMT;
            statement->alter_table_node = (yyvsp[0].alter_table_node);
            (yyval.statement) = statement;
        }
#line 2303 "y.tab.c"
    break;

  case 21: /* create_table_statement: CREATE TABLE table '(' base_table_element_commalist ')' end  */
#line 310 "sql.y"
        {
            CreateTableNode *create_table_node = instance(CreateTableNode);
            create_table_node->table_name = (yyvsp[-4].strVal);
            create_table_node->base_table_element_commalist = (yyvsp[-2].list);
            (yyval.create_table_node) = create_table_node;
        }
#line 2314 "y.tab.c"
    break;

  case 22: /* create_index_statement: CREATE INDEX index_name ON table '(' columns ')' end  */
#line 320 "sql.y"
        {
            CreateIndexNode *create_index_node = instance(CreateIndexNode);
            create_index_node->index_name = (yyvsp[-6].strVal);
            create_index_node->table_name = (yyvsp[-4].strVal);
            create_index_node->is_unique = false;
            create_index_node->type = BTREE_INDEX;
            create_index_node->columns = (yyvsp[-2].list);
            (yyval.create_index_node) = create_index_node;
        }
#line 2328 "y.tab.c"
    break;

  case 23: /* create_index_statement: CREATE UNIQUE INDEX index_name ON table '(' columns ')' end  */
#line 330 "sql.y"
        {
            CreateIndexNode *create_index_node = instance(CreateIndexNode);
            create_index_node->index_name = (yyvsp[-6].strVal);
            create_index_node->table_name = (yyvsp[-4].strVal);
            create_index_node->is_unique = true;
            create_index_node->type = BTREE_INDEX;
            create_index_node->columns = (yyvsp[-2].list);
            (yyval.create_index_node) = create_index_node;
        }
#line 2342 "y.tab.c"
    break;

  case 24: /* drop_table_statement: DROP TABLE table end  */
#line 343 "sql.y"
        {
            DropTableNode *drop_table_node = instance(DropTableNode);
            drop_table_node->table_name = (yyvsp[-1].strVal);
            (yyval.drop_table_node) = drop_table_node;
        }
#line 2352 "y.tab.c"
    break;

  case 25: /* drop_index_statement: DROP INDEX index_name end  */
#line 352 "sql.y"
        {
            DropIndexNode *drop_index_node = instance(DropIndexNode);
            drop_index_node->index_name = (yyvsp[-1].strVal);
            (yyval.drop_index_node) = drop_index_node;
        }
#line 2362 "y.tab.c"
    break;

  case 26: /* select_statement: SELECT selection table_exp end  */
#line 361 "sql.y"
        {
            SelectNode *select_node = instance(SelectNode);
            select_node->selection = (yyvsp[-2].selection_node);
            select_node->table_exp = (yyvsp[-1].table_exp_node);
            (yyval.select_node) = select_node;
        }
#line 2373 "y.tab.c"
    break;

  case 27: /* insert_statement: INSERT INTO table values_or_query_spec end  */
#line 371 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = true;
            node->table_name = (yyvsp[-2].strVal);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2385 "y.tab.c"
    break;

  case 28: /* insert_statement: INSERT INTO table '(' columns ')' values_or_query_spec end  */
#line 379 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = false;
            node->table_name = (yyvsp[-5].strVal);
            node->column_list = (yyvsp[-3].list);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2398 "y.tab.c"
    break;

  case 29: /* update_statement: UPDATE table SET assignments opt_where_clause end  */
#line 391 "sql.y"
        {
            UpdateNode *node = instance(UpdateNode);
            node->table_name = (yyvsp[-4].strVal);
            node->assignment_list = (yyvsp[-2].list);
            node->where_clause = (yyvsp[-1].where_clause_node);
            (yyval.update_node) = node;
        }
#line 2410 "y.tab.c"
    break;

  case 30: /* delete_statement: DELETE FROM table end  */
#line 402 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.delete_node) = node;
        }
#line 2420 "y.tab.c"
    break;

  case 31: /* delete_statement: DELETE FROM table WHERE search_condition end  */
#line 408 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-3].strVal);
            node->condition_node = (yyvsp[-1].search_condition_node);
            (yyval.delete_node) = node;
        }
#line 2431 "y.tab.c"
    break;

  case 32: /* describe_statement: DESCRIBE table end  */
#line 418 "sql.y"
        {
            DescribeNode *node = instance(DescribeNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.describe_node) = node;
        }
#line 2441 "y.tab.c"
    break;

  case 33: /* show_statement: SHOW TABLES end  */
#line 427 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_TABLES;
            (yyval.show_node) = node;
        }
#line 2451 "y.tab.c"
    break;

  case 34: /* show_statement: SHOW INDEX FROM table end  */
#line 433 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_IDNEXS;
            node->table_name = (yyvsp[-1].strVal);
            (yyval.show_node) = node;
        }
#line 2462 "y.tab.c"
    break;

  case 35: /* alter_table_statement: ALTER TABLE table alter_table_action end  */
#line 443 "sql.y"
        {
            (yyval.alter_table_node) = instance(AlterTableNode);
            (yyval.alter_table_node)->table_name = (yyvsp[-2].strVal);
            (yyval.alter_table_node)->action = (yyvsp[-1].alter_table_action);
        }
#line 2472 "y.tab.c"
    break;

  case 36: /* alter_table_action: add_column_def  */
#line 451 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_ADD_COLUMN;
            action->action.add_column = (yyvsp[0].add_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2483 "y.tab.c"
    break;

  case 37: /* alter_table_action: drop_column_def  */
#line 458 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_DROP_COLUMN;
            action->action.drop_column = (yyvsp[0].drop_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2494 "y.tab.c"
    break;

  case 38: /* add_column_def: ADD COLUMN column_def column_position_def  */
#line 467 "sql.y"
        {
            AddColumnDef *node = instance(AddColumnDef);
            node->column_def = (yyvsp[-1].column_def_node);
            node->position_def = (yyvsp[0].column_position_def);
            (yyval.add_column_def) = node;
        }
#line 2505 "y.tab.c"
    break;

  case 39: /* drop_column_def: DROP COLUMN IDENTIFIER  */
#line 476 "sql.y"
        {
            DropColumnDef *node = instance(DropColumnDef);
            node->column_name = (yyvsp[0].strVal);
            (yyval.drop_column_def) = node;
        }
#line 2515 "y.tab.c"
    break;

  case 40: /* column_position_def: %empty  */
#line 484 "sql.y"
    {
        (yyval.column_position_def) = NULL;
    }
#line 2523 "y.tab.c"
    break;

  case 41: /* column_position_def: BEFORE IDENTIFIER  */
#line 488 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_BEFORE;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2534 "y.tab.c"
    break;

  case 42: /* column_position_def: AFTER IDENTIFIER  */
#line 495 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_AFTER;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2545 "y.tab.c"
    break;

  case 43: /* selection: scalar_exp_commalist  */
#line 504 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = false;
            selection_node->scalar_exp_list = (yyvsp[0].list);
            (yyval.selection_node) = selection_node;
        }
#line 2556 "y.tab.c"
    break;

  case 44: /* selection: '*'  */
#line 511 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = true;
            (yyval.selection_node) = selection_node;
        }
#line 2566 "y.tab.c"
    break;

  case 45: /* table_exp: from_clause opt_where_clause limit_clause  */
#line 519 "sql.y"
        {
            TableExpNode *table_exp = instance(TableExpNode);
            table_exp->from_clause = (yyvsp[-2].from_clause_node);
            table_exp->where_clause = (yyvsp[-1].where_clause_node);
            table_exp->limit_clause = (yyvsp[0].limit_clause_node);
            (yyval.table_exp_node) = table_exp;
        }
#line 2578 "y.tab.c"
    break;

  case 46: /* from_clause: %empty  */
#line 529 "sql.y"
        {
            (yyval.from_clause_node) = NULL;
        }
#line 2586 "y.tab.c"
    break;

  case 47: /* from_clause: FROM table_ref_commalist  */
#line 533 "sql.y"
        {
            FromClauseNode *from_clause = instance(FromClauseNode);
            from_clause->from = (yyvsp[0].list);
            (yyval.from_clause_node) = from_clause;
        }
#line 2596 "y.tab.c"
    break;

  case 48: /* table_ref_commalist: table_ref  */
#line 541 "sql.y"
        {
            List *list = create_list(NODE_TABLE_REFER);
            append_list(list, (yyvsp[0].table_ref_node));
            (yyval.list) = list;
        }
#line 2606 "y.tab.c"
    break;

  case 49: /* table_ref_commalist: table_ref_commalist ',' table_ref  */
#line 547 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].table_ref_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2615 "y.tab.c"
    break;

  case 50: /* table_ref: table  */
#line 554 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2625 "y.tab.c"
    break;

  case 51: /* table_ref: table range_variable  */
#line 560 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-1].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2636 "y.tab.c"
    break;

  case 52: /* table_ref: table AS range_variable  */
#line 567 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-2].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2647 "y.tab.c"
    break;

  case 53: /* table: IDENTIFIER  */
#line 576 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2655 "y.tab.c"
    break;

  case 54: /* index_name: IDENTIFIER  */
#line 582 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2663 "y.tab.c"
    break;

  case 55: /* range_variable: IDENTIFIER  */
#line 588 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2671 "y.tab.c"
    break;

  case 56: /* opt_where_clause: %empty  */
#line 594 "sql.y"
        {
            (yyval.where_clause_node) = NULL;
        }
#line 2679 "y.tab.c"
    break;

  case 57: /* opt_where_clause: where_clause  */
#line 598 "sql.y"
        {
            (yyval.where_clause_node) = (yyvsp[0].where_clause_node);
        }
#line 2687 "y.tab.c"
    break;

  case 58: /* where_clause: WHERE search_condition  */
#line 604 "sql.y"
        {
            WhereClauseNode *where_clause_node = instance(WhereClauseNode);
            where_clause_node->condition = (yyvsp[0].search_condition_node);
            (yyval.where_clause_node) = where_clause_node;
        }
#line 2697 "y.tab.c"
    break;

  case 59: /* values_or_query_spec: VALUES opt_values  */
#line 612 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_VALUES;
            values_or_query_spec->values = (yyvsp[0].list);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2708 "y.tab.c"
    break;

  case 60: /* values_or_query_spec: query_spec  */
#line 619 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_QUERY_SPEC;
            values_or_query_spec->query_spec = (yyvsp[0].query_spec_node);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2719 "y.tab.c"
    break;

  case 61: /* opt_values: '(' value_items ')'  */
#line 628 "sql.y"
        {
            (yyval.list) = create_list(NODE_LIST);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2728 "y.tab.c"
    break;

  case 62: /* opt_values: opt_values ',' '(' value_items ')'  */
#line 633 "sql.y"
        {
            (yyval.list) = (yyvsp[-4].list);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2737 "y.tab.c"
    break;

  case 63: /* query_spec: SELECT selection table_exp  */
#line 640 "sql.y"
        {
            QuerySpecNode *query_spec = instance(QuerySpecNode);
            query_spec->selection = (yyvsp[-1].selection_node);
            query_spec->table_exp = (yyvsp[0].table_exp_node);
            (yyval.query_spec_node) = query_spec;
        }
#line 2748 "y.tab.c"
    break;

  case 64: /* scalar_exp_commalist: scalar_exp  */
#line 649 "sql.y"
        {
            List *scalar_exp_list = create_list(NODE_SCALAR_EXP);
            append_list(scalar_exp_list, (yyvsp[0].scalar_exp_node));
            (yyval.list) = scalar_exp_list;
        }
#line 2758 "y.tab.c"
    break;

  case 65: /* scalar_exp_commalist: scalar_exp_commalist ',' scalar_exp  */
#line 655 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].scalar_exp_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2767 "y.tab.c"
    break;

  case 66: /* scalar_exp: calculate  */
#line 662 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_CALCULATE;
            scalar_exp_node->calculate = (yyvsp[0].calculate_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2778 "y.tab.c"
    break;

  case 67: /* scalar_exp: column  */
#line 669 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_COLUMN;
            scalar_exp_node->column = (yyvsp[0].column_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2789 "y.tab.c"
    break;

  case 68: /* scalar_exp: function  */
#line 676 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_FUNCTION;
            scalar_exp_node->function = (yyvsp[0].function_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2800 "y.tab.c"
    break;

  case 69: /* scalar_exp: value_item  */
#line 683 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_VALUE;
            scalar_exp_node->value = (yyvsp[0].value_item_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2811 "y.tab.c"
    break;

  case 70: /* scalar_exp: '(' scalar_exp ')'  */
#line 690 "sql.y"
        {
            (yyval.scalar_exp_node) = (yyvsp[-1].scalar_exp_node);
        }
#line 2819 "y.tab.c"
    break;

  case 71: /* scalar_exp: scalar_exp AS IDENTIFIER  */
#line 694 "sql.y"
        {
            (yyvsp[-2].scalar_exp_node)->alias = (yyvsp[0].strVal);
            (yyval.scalar_exp_node) = (yyvsp[-2].scalar_exp_node);
        }
#line 2828 "y.tab.c"
    break;

  case 72: /* calculate: scalar_exp '+' scalar_exp  */
#line 701 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_ADD;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2840 "y.tab.c"
    break;

  case 73: /* calculate: scalar_exp '-' scalar_exp  */
#line 709 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_SUB;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2852 "y.tab.c"
    break;

  case 74: /* calculate: scalar_exp '*' scalar_exp  */
#line 717 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_MUL;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2864 "y.tab.c"
    break;

  case 75: /* calculate: scalar_exp '/' scalar_exp  */
#line 725 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_DIV;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2876 "y.tab.c"
    break;

  case 76: /* columns: column  */
#line 735 "sql.y"
        {
            List *column_set_node = create_list(NODE_COLUMN);
            append_list(column_set_node, (yyvsp[0].column_node));
            (yyval.list) = column_set_node;
        }
#line 2886 "y.tab.c"
    break;

  case 77: /* columns: columns ',' column  */
#line 741 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].column_node));
        }
#line 2895 "y.tab.c"
    break;

  case 78: /* base_table_element_commalist: base_table_element  */
#line 748 "sql.y"
        {
            List *base_table_element_commalist = create_list(NODE_BASE_TABLE_ELEMENT);
            append_list(base_table_element_commalist, (yyvsp[0].base_table_element));
            (yyval.list) = base_table_element_commalist;
        }
#line 2905 "y.tab.c"
    break;

  case 79: /* base_table_element_commalist: base_table_element_commalist ',' base_table_element  */
#line 754 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].base_table_element));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2914 "y.tab.c"
    break;

  case 80: /* base_table_element: column_def  */
#line 761 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = (yyvsp[0].column_def_node);
            node->table_contraint_def = NULL;
            node->type = TELE_COLUMN_DEF;
            (yyval.base_table_element) = node;
        }
#line 2926 "y.tab.c"
    break;

  case 81: /* base_table_element: table_contraint_def  */
#line 769 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = NULL;
            node->table_contraint_def = (yyvsp[0].table_contraint_def);
            node->type = TELE_TABLE_CONTRAINT_DEF;
            (yyval.base_table_element) = node;
        }
#line 2938 "y.tab.c"
    break;

  case 82: /* column_def: column_def_name data_type array_dim_clause column_def_opt_list  */
#line 792 "sql.y"
        {
            ColumnDefNode *column_def = instance(ColumnDefNode);
            column_def->column = (yyvsp[-3].column_def_name);
            column_def->data_type = (yyvsp[-2].data_type_node);
            column_def->array_dim = (yyvsp[-1].intVal);
            column_def->column_def_opt_list = (yyvsp[0].list);
            (yyval.column_def_node) = column_def;
        }
#line 2951 "y.tab.c"
    break;

  case 83: /* column_def_name_commalist: column_def_name  */
#line 803 "sql.y"
        {
            List *list = create_list(NODE_COLUMN_DEF_NAME);
            append_list(list, (yyvsp[0].column_def_name));
            (yyval.list) = list;
        }
#line 2961 "y.tab.c"
    break;

  case 84: /* column_def_name_commalist: column_def_name_commalist ',' column_def_name  */
#line 809 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].column_def_name));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2970 "y.tab.c"
    break;

  case 85: /* column_def_name: IDENTIFIER  */
#line 816 "sql.y"
        {
            ColumnDefName *column_def_name = instance(ColumnDefName);
            column_def_name->column = (yyvsp[0].strVal);
            (yyval.column_def_name) = column_def_name;
        }
#line 2980 "y.tab.c"
    break;

  case 86: /* data_type: INT  */
#line 824 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_INT; 
            (yyval.data_type_node) = node;
        }
#line 2990 "y.tab.c"
    break;

  case 87: /* data_type: LONG  */
#line 830 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_LONG;  
            (yyval.data_type_node) = node;
        }
#line 3000 "y.tab.c"
    break;

  case 88: /* data_type: CHAR  */
#line 836 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_CHAR; 
            (yyval.data_type_node) = node;
        }
#line 3010 "y.tab.c"
    break;

  case 89: /* data_type: VARCHAR '(' INTVALUE ')'  */
#line 842 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_VARCHAR; 
            node->len = (yyvsp[-1].intVal);
            (yyval.data_type_node) = node;
        }
#line 3021 "y.tab.c"
    break;

  case 90: /* data_type: STRING  */
#line 849 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_STRING; 
            (yyval.data_type_node) = node;
        }
#line 3031 "y.tab.c"
    break;

  case 91: /* data_type: BOOL  */
#line 855 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_BOOL; 
            (yyval.data_type_node) = node;
        }
#line 3041 "y.tab.c"
    break;

  case 92: /* data_type: FLOAT  */
#line 861 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_FLOAT; 
            (yyval.data_type_node) = node;
        }
#line 3051 "y.tab.c"
    break;

  case 93: /* data_type: DOUBLE  */
#line 867 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DOUBLE; 
            (yyval.data_type_node) = node;
        }
#line 3061 "y.tab.c"
    break;

  case 94: /* data_type: TIMESTAMP  */
#line 873 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_TIMESTAMP; 
            (yyval.data_type_node) = node;
        }
#line 3071 "y.tab.c"
    break;

  case 95: /* data_type: DATE  */
#line 879 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DATE; 
            (yyval.data_type_node) = node;
        }
#line 3081 "y.tab.c"
    break;

  case 96: /* data_type: table  */
#line 885 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_RID;
            node->table_name = (yyvsp[0].strVal);
            (yyval.data_type_node) = node;
        }
#line 3092 "y.tab.c"
    break;

  case 97: /* array_dim_clause: %empty  */
#line 894 "sql.y"
        {
            (yyval.intVal) = 0;
        }
#line 3100 "y.tab.c"
    break;

  case 98: /* array_dim_clause: '[' ']'  */
#line 898 "sql.y"
        {
            (yyval.intVal) = 1;
        }
#line 3108 "y.tab.c"
    break;

  case 99: /* array_dim_clause: array_dim_clause '[' ']'  */
#line 902 "sql.y"
        {
            (yyval.intVal)++;
        }
#line 3116 "y.tab.c"
    break;

  case 100: /* column_def_opt_list: %empty  */
#line 908 "sql.y"
        {
            (yyval.list) = NULL;
        }
#line 3124 "y.tab.c"
    break;

  case 101: /* column_def_opt_list: column_def_opt  */
#line 912 "sql.y"
        {
            (yyval.list) = create_list(NODE_COLUMN_DEF_OPT);
            append_list((yyval.list), (yyvsp[0].column_def_opt));
        }
#line 3133 "y.tab.c"
    break;

  case 102: /* column_def_opt_list: column_def_opt_list column_def_opt  */
#line 917 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].column_def_opt));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 3142 "y.tab.c"
    break;

  case 103: /* column_def_opt: NOT NULLX  */
#line 924 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_NOT_NULL; 
            (yyval.column_def_opt) = node;
        }
#line 3152 "y.tab.c"
    break;

  case 104: /* column_def_opt: UNIQUE  */
#line 930 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_UNIQUE; 
            (yyval.column_def_opt) = node;
        }
#line 3162 "y.tab.c"
    break;

  case 105: /* column_def_opt: PRIMARY KEY  */
#line 936 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_PRIMARY_KEY; 
            (yyval.column_def_opt) = node;
        }
#line 3172 "y.tab.c"
    break;

  case 106: /* column_def_opt: DEFAULT value_item  */
#line 942 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_VALUE;
            node->value = (yyvsp[0].value_item_node);
            (yyval.column_def_opt) = node;
        }
#line 3183 "y.tab.c"
    break;

  case 107: /* column_def_opt: DEFAULT NULLX  */
#line 949 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_NULL;
            (yyval.column_def_opt) = node;
        }
#line 3193 "y.tab.c"
    break;

  case 108: /* column_def_opt: COMMENT STRINGVALUE  */
#line 955 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_COMMENT;
            node->comment = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3204 "y.tab.c"
    break;

  case 109: /* column_def_opt: CHECK '(' search_condition ')'  */
#line 962 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_CHECK_CONDITION;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.column_def_opt) = node;
        }
#line 3215 "y.tab.c"
    break;

  case 110: /* column_def_opt: REFERENCES table  */
#line 969 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_REFERENECS;
            node->refer_table = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3226 "y.tab.c"
    break;

  case 111: /* table_contraint_def: UNIQUE '(' column_def_name_commalist ')'  */
#line 978 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_UNIQUE;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3237 "y.tab.c"
    break;

  case 112: /* table_contraint_def: PRIMARY KEY '(' column_def_name_commalist ')'  */
#line 985 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_PRIMARY_KEY;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3248 "y.tab.c"
    break;

  case 113: /* table_contraint_def: FOREIGN KEY '(' column_def_name_commalist ')' REFERENCES table  */
#line 992 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_FOREIGN_KEY;
            node->column_commalist = (yyvsp[-3].list);
            node->table = (yyvsp[0].strVal);
            (yyval.table_contraint_def) = node;
        }
#line 3260 "y.tab.c"
    break;

  case 114: /* table_contraint_def: CHECK '(' search_condition ')'  */
#line 1000 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_CHECK;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.table_contraint_def) = node;
        }
#line 3271 "y.tab.c"
    break;

  case 115: /* column: IDENTIFIER  */
#line 1009 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[0].strVal);
            column_node->has_sub_column = false;
            (yyval.column_node) = column_node;
        }
#line 3282 "y.tab.c"
    break;

  case 116: /* column: '(' IDENTIFIER ')' '.' column  */
#line 1016 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->sub_column = (yyvsp[0].column_node);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3294 "y.tab.c"
    break;

  case 117: /* column: IDENTIFIER '{' scalar_exp_commalist '}'  */
#line 1024 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->scalar_exp_list = (yyvsp[-1].list);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3306 "y.tab.c"
    break;

  case 118: /* column: IDENTIFIER '.' column  */
#line 1032 "sql.y"
        {
            (yyval.column_node) = (yyvsp[0].column_node);
            (yyval.column_node)->range_variable = (yyvsp[-2].strVal);
        }
#line 3315 "y.tab.c"
    break;

  case 119: /* value_items: value_item  */
#line 1039 "sql.y"
        {
            List *value_list = create_list(NODE_VALUE_ITEM);
            append_list(value_list, (yyvsp[0].value_item_node));
            (yyval.list) = value_list;
        }
#line 3325 "y.tab.c"
    break;

  case 120: /* value_items: value_items ',' value_item  */
#line 1045 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].value_item_node));
        }
#line 3334 "y.tab.c"
    break;

  case 121: /* value_item: atom  */
#line 1052 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ATOM;
            node->value.atom = (yyvsp[0].atom_node);
            (yyval.value_item_node) = node;
        }
#line 3345 "y.tab.c"
    break;

  case 122: /* value_item: NULLX  */
#line 1059 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_NULL;
            (yyval.value_item_node) = node;
        }
#line 3355 "y.tab.c"
    break;

  case 123: /* value_item: '[' value_items ']'  */
#line 1065 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ARRAY;
            node->value.value_list = (yyvsp[-1].list);
            (yyval.value_item_node) = node;
        }
#line 3366 "y.tab.c"
    break;

  case 124: /* atom: INTVALUE  */
#line 1074 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.intval = (yyvsp[0].intVal);
            node->type = A_INT;
            (yyval.atom_node) = node;
        }
#line 3377 "y.tab.c"
    break;

  case 125: /* atom: BOOLVALUE  */
#line 1081 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.boolval = (yyvsp[0].boolVal);
            node->type = A_BOOL;
            (yyval.atom_node) = node;
        }
#line 3388 "y.tab.c"
    break;

  case 126: /* atom: STRINGVALUE  */
#line 1088 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.strval = (yyvsp[0].strVal);
            node->type = A_STRING;
            (yyval.atom_node) = node;
        }
#line 3399 "y.tab.c"
    break;

  case 127: /* atom: FLOATVALUE  */
#line 1095 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.floatval = (yyvsp[0].floatVal);
            node->type = A_FLOAT;
            (yyval.atom_node) = node;
        }
#line 3410 "y.tab.c"
    break;

  case 128: /* atom: REFERVALUE  */
#line 1102 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.referval = (yyvsp[0].referVal);
            node->type = A_REFERENCE;
            (yyval.atom_node) = node;
        }
#line 3421 "y.tab.c"
    break;

  case 129: /* REFERVALUE: '(' value_items ')'  */
#line 1112 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = DIRECTLY;
            refer->nest_value_list = (yyvsp[-1].list);
            (yyval.referVal) = refer;
        }
#line 3432 "y.tab.c"
    break;

  case 130: /* REFERVALUE: REF '(' search_condition ')'  */
#line 1120 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = INDIRECTLY;
            refer->condition = (yyvsp[-1].search_condition_node);
            (yyval.referVal) = refer;
        }
#line 3443 "y.tab.c"
    break;

  case 131: /* BOOLVALUE: TRUE  */
#line 1129 "sql.y"
        {
            (yyval.boolVal) = true;
        }
#line 3451 "y.tab.c"
    break;

  case 132: /* BOOLVALUE: FALSE  */
#line 1133 "sql.y"
        {
            (yyval.boolVal) = false;
        }
#line 3459 "y.tab.c"
    break;

  case 133: /* assignments: assignment  */
#line 1139 "sql.y"
        {
            List *list = create_list(NODE_ASSIGNMENT);
            append_list(list, (yyvsp[0].assignment_node));
            (yyval.list) = list;
        }
#line 3469 "y.tab.c"
    break;

  case 134: /* assignments: assignments ',' assignment  */
#line 1145 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].assignment_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 3478 "y.tab.c"
    break;

  case 135: /* assignment: column EQ value_item  */
#line 1152 "sql.y"
        {
            AssignmentNode *node = instance(AssignmentNode);
            node->column = (yyvsp[-2].column_node);
            node->value = (yyvsp[0].value_item_node);
            (yyval.assignment_node) = node;
        }
#line 3489 "y.tab.c"
    break;

  case 136: /* search_condition: boolean_term  */
#line 1161 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3499 "y.tab.c"
    break;

  case 137: /* search_condition: search_condition OR boolean_term  */
#line 1167 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->or_search_condition = (yyvsp[-2].search_condition_node);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3510 "y.tab.c"
    break;

  case 138: /* boolean_term: boolean_factor  */
#line 1176 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3520 "y.tab.c"
    break;

  case 139: /* boolean_term: boolean_term AND boolean_factor  */
#line 1182 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->and_boolean_term = (yyvsp[-2].boolean_term_node);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3531 "y.tab.c"
    break;

  case 140: /* boolean_factor: boolean_test  */
#line 1191 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = false;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3542 "y.tab.c"
    break;

  case 141: /* boolean_factor: NOT boolean_test  */
#line 1198 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = true;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3553 "y.tab.c"
    break;

  case 142: /* boolean_test: boolean_primary  */
#line 1207 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[0].boolean_primary_node);
            test_node->type = NONE_TRUE_VALUE;
            (yyval.boolean_test_node) = test_node;
        }
#line 3564 "y.tab.c"
    break;

  case 143: /* boolean_test: boolean_primary IS BOOLVALUE  */
#line 1214 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-2].boolean_primary_node);
            test_node->type = IS_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[0].boolVal);
            (yyval.boolean_test_node) = test_node;
        }
#line 3576 "y.tab.c"
    break;

  case 144: /* boolean_test: boolean_primary IS NOT BOOLVALUE  */
#line 1222 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-3].boolean_primary_node);
            test_node->type = IS_NOT_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[-1].keyword);
            (yyval.boolean_test_node) = test_node;
        }
#line 3588 "y.tab.c"
    break;

  case 145: /* boolean_primary: predicate  */
#line 1232 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = PREDICATE_BOOLEAN_PRIMAYR;
            primary_node->predicate = (yyvsp[0].predicate_node);
            primary_node->search_condition = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3600 "y.tab.c"
    break;

  case 146: /* boolean_primary: '(' search_condition ')'  */
#line 1240 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = SEARCH_CONDITION_BOOLEAN_PRIMAYR;
            primary_node->search_condition = (yyvsp[-1].search_condition_node);
            primary_node->predicate = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3612 "y.tab.c"
    break;

  case 147: /* predicate: comparison_predicate  */
#line 1249 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_COMPARISON;
            predicate->comparison = (yyvsp[0].comparison_node);
            (yyval.predicate_node) = predicate;
        }
#line 3623 "y.tab.c"
    break;

  case 148: /* predicate: like_predicate  */
#line 1256 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_LIKE;
            predicate->like = (yyvsp[0].like_node);
            (yyval.predicate_node) = predicate;
        }
#line 3634 "y.tab.c"
    break;

  case 149: /* predicate: in_predicate  */
#line 1263 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_IN;
            predicate->in = (yyvsp[0].in_node);
            (yyval.predicate_node) = predicate;
        }
#line 3645 "y.tab.c"
    break;

  case 150: /* comparison_predicate: scalar_exp compare scalar_exp  */
#line 1272 "sql.y"
        {
            ComparisonNode *comparison_node = instance(ComparisonNode);
            comparison_node->left = (yyvsp[-2].scalar_exp_node);
            comparison_node->type = (yyvsp[-1].compare_type);
            comparison_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.comparison_node) = comparison_node;
        }
#line 3657 "y.tab.c"
    break;

  case 151: /* like_predicate: column LIKE value_item  */
#line 1282 "sql.y"
        {
            LikeNode *like_node = instance(LikeNode);
            like_node->column = (yyvsp[-2].column_node);
            like_node->value = (yyvsp[0].value_item_node);
            (yyval.like_node) = like_node;
        }
#line 3668 "y.tab.c"
    break;

  case 152: /* in_predicate: column IN '(' value_items ')'  */
#line 1291 "sql.y"
        {
            InNode *in_node = instance(InNode);
            in_node->column = (yyvsp[-4].column_node);
            in_node->value_list = (yyvsp[-1].list);
            (yyval.in_node) = in_node;
        }
#line 3679 "y.tab.c"
    break;

  case 153: /* limit_clause: %empty  */
#line 1300 "sql.y"
        {
            (yyval.limit_clause_node) = NULL;
        }
#line 3687 "y.tab.c"
    break;

  case 154: /* limit_clause: LIMIT INTVALUE  */
#line 1304 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = 0;
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3698 "y.tab.c"
    break;

  case 155: /* limit_clause: LIMIT INTVALUE ',' INTVALUE  */
#line 1311 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = (yyvsp[-2].intVal);
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3709 "y.tab.c"
    break;

  case 156: /* limit_clause: LIMIT INTVALUE OFFSET INTVALUE  */
#line 1318 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->rows = (yyvsp[-2].intVal);
            node->offset = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3720 "y.tab.c"
    break;

  case 157: /* compare: EQ  */
#line 1326 "sql.y"
            { (yyval.compare_type) = O_EQ; }
#line 3726 "y.tab.c"
    break;

  case 158: /* compare: NE  */
#line 1327 "sql.y"
            { (yyval.compare_type) = O_NE; }
#line 3732 "y.tab.c"
    break;

  case 159: /* compare: GT  */
#line 1328 "sql.y"
            { (yyval.compare_type) = O_GT; }
#line 3738 "y.tab.c"
    break;

  case 160: /* compare: GE  */
#line 1329 "sql.y"
            { (yyval.compare_type) = O_GE; }
#line 3744 "y.tab.c"
    break;

  case 161: /* compare: LT  */
#line 1330 "sql.y"
            { (yyval.compare_type) = O_LT; }
#line 3750 "y.tab.c"
    break;

  case 162: /* compare: LE  */
#line 1331 "sql.y"
            { (yyval.compare_type) = O_LE; }
#line 3756 "y.tab.c"
    break;

  case 163: /* function: MAX '(' non_all_function_value ')'  */
#line 1335 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MAX;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3767 "y.tab.c"
    break;

  case 164: /* function: MIN '(' non_all_function_value ')'  */
#line 1342 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MIN;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3778 "y.tab.c"
    break;

  case 165: /* function: COUNT '(' function_value ')'  */
#line 1349 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_COUNT;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3789 "y.tab.c"
    break;

  case 166: /* function: SUM '(' function_value ')'  */
#line 1356 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_SUM;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3800 "y.tab.c"
    break;

  case 167: /* function: AVG '(' function_value ')'  */
#line 1363 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_AVG;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3811 "y.tab.c"
    break;

  case 168: /* function_value: INTVALUE  */
#line 1372 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3822 "y.tab.c"
    break;

  case 169: /* function_value: column  */
#line 1379 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3833 "y.tab.c"
    break;

  case 170: /* function_value: '*'  */
#line 1386 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->value_type = V_ALL;
            (yyval.function_value_node) = node;
        }
#line 3843 "y.tab.c"
    break;

  case 171: /* non_all_function_value: INTVALUE  */
#line 1394 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3854 "y.tab.c"
    break;

  case 172: /* non_all_function_value: column  */
#line 1401 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3865 "y.tab.c"
    break;


#line 3869 "y.tab.c"

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

#line 1412 "sql.y"


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
