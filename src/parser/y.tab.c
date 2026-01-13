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

#line 364 "y.tab.c"

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
  YYSYMBOL_EXPRESS = 26,                   /* EXPRESS  */
  YYSYMBOL_FROM = 27,                      /* FROM  */
  YYSYMBOL_WHERE = 28,                     /* WHERE  */
  YYSYMBOL_INTO = 29,                      /* INTO  */
  YYSYMBOL_SET = 30,                       /* SET  */
  YYSYMBOL_VALUES = 31,                    /* VALUES  */
  YYSYMBOL_TABLE = 32,                     /* TABLE  */
  YYSYMBOL_INDEX = 33,                     /* INDEX  */
  YYSYMBOL_LIMIT = 34,                     /* LIMIT  */
  YYSYMBOL_OFFSET = 35,                    /* OFFSET  */
  YYSYMBOL_TABLES = 36,                    /* TABLES  */
  YYSYMBOL_PRIMARY = 37,                   /* PRIMARY  */
  YYSYMBOL_KEY = 38,                       /* KEY  */
  YYSYMBOL_UNIQUE = 39,                    /* UNIQUE  */
  YYSYMBOL_DEFAULT = 40,                   /* DEFAULT  */
  YYSYMBOL_CHECK = 41,                     /* CHECK  */
  YYSYMBOL_REFERENCES = 42,                /* REFERENCES  */
  YYSYMBOL_FOREIGN = 43,                   /* FOREIGN  */
  YYSYMBOL_MAX = 44,                       /* MAX  */
  YYSYMBOL_MIN = 45,                       /* MIN  */
  YYSYMBOL_COUNT = 46,                     /* COUNT  */
  YYSYMBOL_SUM = 47,                       /* SUM  */
  YYSYMBOL_AVG = 48,                       /* AVG  */
  YYSYMBOL_REF = 49,                       /* REF  */
  YYSYMBOL_TRUE = 50,                      /* TRUE  */
  YYSYMBOL_FALSE = 51,                     /* FALSE  */
  YYSYMBOL_NULLX = 52,                     /* NULLX  */
  YYSYMBOL_AS = 53,                        /* AS  */
  YYSYMBOL_COMMENT = 54,                   /* COMMENT  */
  YYSYMBOL_CHAR = 55,                      /* CHAR  */
  YYSYMBOL_INT = 56,                       /* INT  */
  YYSYMBOL_LONG = 57,                      /* LONG  */
  YYSYMBOL_VARCHAR = 58,                   /* VARCHAR  */
  YYSYMBOL_STRING = 59,                    /* STRING  */
  YYSYMBOL_BOOL = 60,                      /* BOOL  */
  YYSYMBOL_FLOAT = 61,                     /* FLOAT  */
  YYSYMBOL_DOUBLE = 62,                    /* DOUBLE  */
  YYSYMBOL_DATE = 63,                      /* DATE  */
  YYSYMBOL_TIMESTAMP = 64,                 /* TIMESTAMP  */
  YYSYMBOL_EQ = 65,                        /* EQ  */
  YYSYMBOL_NE = 66,                        /* NE  */
  YYSYMBOL_GT = 67,                        /* GT  */
  YYSYMBOL_GE = 68,                        /* GE  */
  YYSYMBOL_LT = 69,                        /* LT  */
  YYSYMBOL_LE = 70,                        /* LE  */
  YYSYMBOL_IN = 71,                        /* IN  */
  YYSYMBOL_LIKE = 72,                      /* LIKE  */
  YYSYMBOL_IS = 73,                        /* IS  */
  YYSYMBOL_NOT = 74,                       /* NOT  */
  YYSYMBOL_ALTER = 75,                     /* ALTER  */
  YYSYMBOL_COLUMN = 76,                    /* COLUMN  */
  YYSYMBOL_ADD = 77,                       /* ADD  */
  YYSYMBOL_RENAME = 78,                    /* RENAME  */
  YYSYMBOL_ON = 79,                        /* ON  */
  YYSYMBOL_BEFORE = 80,                    /* BEFORE  */
  YYSYMBOL_AFTER = 81,                     /* AFTER  */
  YYSYMBOL_SYSTEM = 82,                    /* SYSTEM  */
  YYSYMBOL_CONFIG = 83,                    /* CONFIG  */
  YYSYMBOL_MEMORY = 84,                    /* MEMORY  */
  YYSYMBOL_IDENTIFIER = 85,                /* IDENTIFIER  */
  YYSYMBOL_INTVALUE = 86,                  /* INTVALUE  */
  YYSYMBOL_FLOATVALUE = 87,                /* FLOATVALUE  */
  YYSYMBOL_STRINGVALUE = 88,               /* STRINGVALUE  */
  YYSYMBOL_89_ = 89,                       /* ','  */
  YYSYMBOL_90_ = 90,                       /* '.'  */
  YYSYMBOL_91_ = 91,                       /* '{'  */
  YYSYMBOL_92_ = 92,                       /* '}'  */
  YYSYMBOL_93_ = 93,                       /* ';'  */
  YYSYMBOL_YYACCEPT = 94,                  /* $accept  */
  YYSYMBOL_statements = 95,                /* statements  */
  YYSYMBOL_statement = 96,                 /* statement  */
  YYSYMBOL_begin_transaction_statement = 97, /* begin_transaction_statement  */
  YYSYMBOL_commit_transaction_statement = 98, /* commit_transaction_statement  */
  YYSYMBOL_rollback_transaction_statement = 99, /* rollback_transaction_statement  */
  YYSYMBOL_create_table_statement = 100,   /* create_table_statement  */
  YYSYMBOL_create_index_statement = 101,   /* create_index_statement  */
  YYSYMBOL_drop_table_statement = 102,     /* drop_table_statement  */
  YYSYMBOL_drop_index_statement = 103,     /* drop_index_statement  */
  YYSYMBOL_select_statement = 104,         /* select_statement  */
  YYSYMBOL_insert_statement = 105,         /* insert_statement  */
  YYSYMBOL_update_statement = 106,         /* update_statement  */
  YYSYMBOL_delete_statement = 107,         /* delete_statement  */
  YYSYMBOL_describe_statement = 108,       /* describe_statement  */
  YYSYMBOL_show_statement = 109,           /* show_statement  */
  YYSYMBOL_explain_statement = 110,        /* explain_statement  */
  YYSYMBOL_express_statement = 111,        /* express_statement  */
  YYSYMBOL_alter_table_statement = 112,    /* alter_table_statement  */
  YYSYMBOL_alter_table_action = 113,       /* alter_table_action  */
  YYSYMBOL_add_column_def = 114,           /* add_column_def  */
  YYSYMBOL_drop_column_def = 115,          /* drop_column_def  */
  YYSYMBOL_column_position_def = 116,      /* column_position_def  */
  YYSYMBOL_selection = 117,                /* selection  */
  YYSYMBOL_table_exp = 118,                /* table_exp  */
  YYSYMBOL_from_clause = 119,              /* from_clause  */
  YYSYMBOL_table_ref_commalist = 120,      /* table_ref_commalist  */
  YYSYMBOL_table_ref = 121,                /* table_ref  */
  YYSYMBOL_table = 122,                    /* table  */
  YYSYMBOL_index_name = 123,               /* index_name  */
  YYSYMBOL_range_variable = 124,           /* range_variable  */
  YYSYMBOL_opt_where_clause = 125,         /* opt_where_clause  */
  YYSYMBOL_where_clause = 126,             /* where_clause  */
  YYSYMBOL_values_or_query_spec = 127,     /* values_or_query_spec  */
  YYSYMBOL_opt_values = 128,               /* opt_values  */
  YYSYMBOL_query_spec = 129,               /* query_spec  */
  YYSYMBOL_scalar_exp_commalist = 130,     /* scalar_exp_commalist  */
  YYSYMBOL_scalar_exp = 131,               /* scalar_exp  */
  YYSYMBOL_calculate = 132,                /* calculate  */
  YYSYMBOL_columns = 133,                  /* columns  */
  YYSYMBOL_base_table_element_commalist = 134, /* base_table_element_commalist  */
  YYSYMBOL_base_table_element = 135,       /* base_table_element  */
  YYSYMBOL_column_def = 136,               /* column_def  */
  YYSYMBOL_column_def_name_commalist = 137, /* column_def_name_commalist  */
  YYSYMBOL_column_def_name = 138,          /* column_def_name  */
  YYSYMBOL_data_type = 139,                /* data_type  */
  YYSYMBOL_array_dim_clause = 140,         /* array_dim_clause  */
  YYSYMBOL_column_def_opt_list = 141,      /* column_def_opt_list  */
  YYSYMBOL_column_def_opt = 142,           /* column_def_opt  */
  YYSYMBOL_table_contraint_def = 143,      /* table_contraint_def  */
  YYSYMBOL_column = 144,                   /* column  */
  YYSYMBOL_value_items = 145,              /* value_items  */
  YYSYMBOL_value_item = 146,               /* value_item  */
  YYSYMBOL_atom = 147,                     /* atom  */
  YYSYMBOL_REFERVALUE = 148,               /* REFERVALUE  */
  YYSYMBOL_BOOLVALUE = 149,                /* BOOLVALUE  */
  YYSYMBOL_assignments = 150,              /* assignments  */
  YYSYMBOL_assignment = 151,               /* assignment  */
  YYSYMBOL_search_condition = 152,         /* search_condition  */
  YYSYMBOL_boolean_term = 153,             /* boolean_term  */
  YYSYMBOL_boolean_factor = 154,           /* boolean_factor  */
  YYSYMBOL_boolean_test = 155,             /* boolean_test  */
  YYSYMBOL_boolean_primary = 156,          /* boolean_primary  */
  YYSYMBOL_predicate = 157,                /* predicate  */
  YYSYMBOL_comparison_predicate = 158,     /* comparison_predicate  */
  YYSYMBOL_like_predicate = 159,           /* like_predicate  */
  YYSYMBOL_in_predicate = 160,             /* in_predicate  */
  YYSYMBOL_limit_clause = 161,             /* limit_clause  */
  YYSYMBOL_compare = 162,                  /* compare  */
  YYSYMBOL_function = 163,                 /* function  */
  YYSYMBOL_function_value = 164,           /* function_value  */
  YYSYMBOL_non_all_function_value = 165,   /* non_all_function_value  */
  YYSYMBOL_end = 166                       /* end  */
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
#define YYFINAL  78
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   509

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  94
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  73
/* YYNRULES -- Number of rules.  */
#define YYNRULES  178
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  354

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   335


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
       9,    10,     7,     5,    89,     6,    90,     8,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    93,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    11,     2,    12,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    91,     2,    92,     2,     2,     2,     2,
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
      83,    84,    85,    86,    87,    88
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   193,   193,   198,   205,   211,   217,   223,   230,   237,
     244,   251,   258,   265,   272,   279,   286,   293,   300,   307,
     316,   319,   322,   326,   336,   346,   359,   368,   377,   387,
     395,   407,   418,   424,   434,   443,   449,   459,   468,   477,
     485,   492,   501,   510,   519,   522,   529,   538,   545,   553,
     564,   567,   575,   581,   588,   594,   601,   610,   616,   622,
     629,   632,   638,   646,   653,   662,   667,   674,   683,   689,
     696,   703,   710,   717,   724,   728,   735,   743,   751,   759,
     769,   775,   782,   788,   795,   803,   826,   837,   843,   850,
     858,   864,   870,   876,   883,   889,   895,   901,   907,   913,
     919,   929,   932,   936,   943,   946,   951,   958,   964,   970,
     976,   983,   989,   996,  1003,  1012,  1019,  1026,  1034,  1043,
    1050,  1058,  1066,  1073,  1079,  1086,  1093,  1099,  1108,  1115,
    1122,  1129,  1136,  1146,  1154,  1163,  1167,  1173,  1179,  1186,
    1195,  1201,  1210,  1216,  1225,  1232,  1241,  1248,  1256,  1266,
    1274,  1283,  1290,  1297,  1306,  1316,  1325,  1335,  1338,  1345,
    1352,  1361,  1362,  1363,  1364,  1365,  1366,  1369,  1376,  1383,
    1390,  1397,  1406,  1413,  1420,  1428,  1435,  1444,  1445
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
  "DESCRIBE", "SHOW", "EXPLAIN", "EXPRESS", "FROM", "WHERE", "INTO", "SET",
  "VALUES", "TABLE", "INDEX", "LIMIT", "OFFSET", "TABLES", "PRIMARY",
  "KEY", "UNIQUE", "DEFAULT", "CHECK", "REFERENCES", "FOREIGN", "MAX",
  "MIN", "COUNT", "SUM", "AVG", "REF", "TRUE", "FALSE", "NULLX", "AS",
  "COMMENT", "CHAR", "INT", "LONG", "VARCHAR", "STRING", "BOOL", "FLOAT",
  "DOUBLE", "DATE", "TIMESTAMP", "EQ", "NE", "GT", "GE", "LT", "LE", "IN",
  "LIKE", "IS", "NOT", "ALTER", "COLUMN", "ADD", "RENAME", "ON", "BEFORE",
  "AFTER", "SYSTEM", "CONFIG", "MEMORY", "IDENTIFIER", "INTVALUE",
  "FLOATVALUE", "STRINGVALUE", "','", "'.'", "'{'", "'}'", "';'",
  "$accept", "statements", "statement", "begin_transaction_statement",
  "commit_transaction_statement", "rollback_transaction_statement",
  "create_table_statement", "create_index_statement",
  "drop_table_statement", "drop_index_statement", "select_statement",
  "insert_statement", "update_statement", "delete_statement",
  "describe_statement", "show_statement", "explain_statement",
  "express_statement", "alter_table_statement", "alter_table_action",
  "add_column_def", "drop_column_def", "column_position_def", "selection",
  "table_exp", "from_clause", "table_ref_commalist", "table_ref", "table",
  "index_name", "range_variable", "opt_where_clause", "where_clause",
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

#define YYPACT_NINF (-233)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-124)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     434,   -26,   -26,   -26,   192,    -3,   153,    67,    48,   116,
      48,    33,   126,   126,   125,   403,  -233,  -233,  -233,  -233,
    -233,  -233,  -233,  -233,  -233,  -233,  -233,  -233,  -233,  -233,
    -233,  -233,  -233,   161,  -233,  -233,  -233,    48,    76,   157,
      48,    76,  -233,   303,    30,   183,   200,   204,   208,   210,
     212,  -233,  -233,  -233,   -53,  -233,  -233,  -233,   199,   155,
     175,  -233,  -233,  -233,  -233,  -233,  -233,  -233,    48,  -233,
     202,    48,   -26,   218,   -26,  -233,  -233,    48,  -233,  -233,
    -233,   228,  -233,   167,    76,   -26,   -26,     0,   142,     3,
     171,    30,    21,  -233,    12,    12,    36,    36,    36,   203,
      54,   314,    48,   -26,   235,   314,   314,   314,   314,   314,
     180,    27,    54,   -17,  -233,    48,  -233,     8,   179,    48,
     188,  -233,  -233,   184,  -233,  -233,    30,  -233,   190,  -233,
    -233,   259,   266,  -233,  -233,  -233,   268,   269,   271,   283,
     328,   251,    53,   143,   278,  -233,  -233,   211,  -233,  -233,
    -233,  -233,  -233,    84,   194,  -233,    35,  -233,   203,   252,
    -233,   175,    15,    15,   234,   234,  -233,    54,   153,   284,
     -26,  -233,   230,   -16,  -233,   203,  -233,   -26,   220,   221,
     -26,  -233,  -233,   261,   291,   292,   264,  -233,    14,  -233,
    -233,   376,  -233,   294,    48,    54,  -233,   296,  -233,  -233,
    -233,  -233,  -233,   377,   148,  -233,  -233,  -233,  -233,  -233,
    -233,  -233,   314,   298,    30,   203,  -233,   203,    91,  -233,
      48,   223,  -233,  -233,   306,   224,  -233,    25,  -233,   199,
      30,   222,  -233,    30,    54,   -26,     6,  -233,   239,   241,
    -233,   327,   241,   203,   329,   -26,   179,  -233,  -233,  -233,
     331,  -233,  -233,  -233,  -233,  -233,  -233,  -233,   304,    54,
     332,  -233,  -233,   175,    30,  -233,   278,  -233,   105,  -233,
    -233,  -233,     5,    55,    54,  -233,    37,   333,  -233,  -233,
    -233,  -233,  -233,   106,   241,    38,  -233,   156,   241,  -233,
    -233,   258,   334,   231,    39,    54,    41,  -233,   270,   281,
     -26,  -233,  -233,    30,   301,   307,  -233,    43,  -233,   241,
    -233,    45,   371,  -233,   381,   356,  -233,   120,   386,    48,
     308,   345,   425,  -233,   -26,    47,  -233,  -233,  -233,  -233,
      51,  -233,  -233,  -233,  -233,   362,  -233,  -233,  -233,  -233,
    -233,   203,  -233,  -233,  -233,  -233,  -233,   -26,  -233,    48,
     181,  -233,  -233,  -233
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     2,     4,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    19,   177,    20,    21,    22,     0,     0,     0,
       0,     0,    48,     0,     0,     0,     0,     0,     0,     0,
       0,   135,   136,   126,   119,   128,   131,   130,    50,    47,
      68,    70,    71,    73,   125,   132,   129,    72,     0,    57,
       0,     0,     0,     0,     0,    37,    38,     0,     1,     3,
     178,     0,    58,     0,     0,     0,     0,   119,     0,     0,
      73,     0,     0,   123,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    60,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    34,     0,    35,     0,     0,     0,
       0,    26,    27,     0,    74,   133,     0,   127,     0,   175,
     176,     0,     0,   174,   172,   173,     0,     0,     0,     0,
       0,     0,    71,     0,   140,   142,   144,   146,   149,   151,
     152,   153,   122,     0,    51,    52,    54,    28,     0,   157,
      61,    69,    76,    77,    78,    79,    75,     0,     0,     0,
       0,    64,     0,    60,   137,     0,    32,     0,     0,     0,
       0,    40,    41,     0,     0,     0,     0,    89,     0,    82,
      84,     0,    85,     0,     0,     0,   124,     0,   167,   168,
     169,   170,   171,     0,     0,   145,   161,   162,   163,   164,
     165,   166,     0,     0,     0,     0,   134,     0,     0,   121,
       0,     0,    59,    55,    62,     0,    49,     0,    80,    50,
       0,    63,    29,     0,     0,     0,     0,    36,     0,     0,
      39,     0,     0,     0,     0,     0,     0,    92,    90,    91,
       0,    94,    95,    96,    97,    99,    98,   100,   101,     0,
       0,   120,   150,   154,     0,   155,   141,   143,     0,   147,
      53,    56,   158,     0,     0,    67,     0,     0,   139,   138,
      31,    33,    43,    44,     0,     0,    87,     0,     0,    23,
      83,     0,     0,   104,     0,     0,     0,   148,     0,     0,
       0,    81,    65,     0,     0,     0,    42,     0,   115,     0,
     118,     0,     0,   102,     0,     0,   108,     0,     0,     0,
       0,     0,    86,   105,     0,     0,   156,   160,   159,    30,
       0,    45,    46,   116,    88,     0,    93,   103,   109,   111,
     110,     0,   114,   112,   107,   106,    24,     0,    66,     0,
       0,    25,   117,   113
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -233,  -233,   383,  -233,  -233,  -233,  -233,  -233,  -233,  -233,
     217,  -233,  -233,  -233,  -233,  -233,  -233,  -233,  -233,  -233,
    -233,  -233,  -233,   237,   177,  -233,  -233,   187,    -6,     9,
     189,   236,  -233,   135,  -233,  -233,   310,    -1,  -233,  -231,
    -233,   166,   229,  -232,  -165,  -233,  -233,  -233,   119,  -233,
     -80,   -41,   -37,  -233,  -233,  -193,  -233,   238,  -131,   248,
     253,   335,  -233,  -233,  -233,  -233,  -233,  -233,  -233,  -233,
     138,   374,    -2
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,   180,
     181,   182,   306,    58,   103,   104,   154,   155,   156,    83,
     223,   159,   160,   170,   231,   171,    59,   141,    61,   227,
     188,   189,   190,   285,   191,   258,   293,   322,   323,   192,
      62,    89,    63,    64,    65,    66,   173,   174,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   226,   212,    67,
     136,   131,    34
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      35,    36,    70,    92,    72,    60,    90,    93,   204,   215,
     123,   175,   158,   125,   130,   130,   135,   135,   135,   142,
     152,   128,   108,   109,   245,   269,   178,   224,   294,    40,
      41,    81,   172,   127,    85,   273,   167,   100,   101,    91,
     298,    44,    88,   133,   236,   128,   168,   302,   308,   324,
      86,   326,   307,   333,    93,   335,   311,   347,   169,   142,
     142,   348,   111,   128,   325,   113,    73,    33,   110,    74,
     114,   117,   116,   234,   168,   297,    33,   286,   142,    50,
      51,    52,    53,   121,   122,   179,   169,   228,   221,   196,
     100,   101,   126,   120,   299,   142,    68,    54,   129,    33,
      60,   157,    90,   246,   161,   162,   163,   164,   165,   177,
     126,   176,   287,   193,   274,   261,    55,    56,    57,   286,
     222,    54,   134,   286,   213,   214,   126,   309,   274,    91,
     126,    44,   309,    69,   309,   142,   274,   142,   203,    54,
     126,    51,    52,    71,   334,     6,   215,   106,   107,   108,
     109,   215,   124,   216,   172,    51,    52,    77,   262,   215,
      42,    82,    43,   142,    44,   268,   310,    60,   232,    50,
      51,    52,   339,   105,    80,   237,   219,   265,   240,   228,
     106,   107,   108,   109,   215,   257,   304,   305,   260,   276,
      84,   353,    94,    93,   301,   110,   278,    45,    46,    47,
      48,    49,    50,    51,    52,    53,    55,    56,    57,    95,
     350,   263,   139,    96,    44,   228,   183,    97,   184,    98,
     185,    99,   186,   296,    37,    38,   102,    93,   110,    75,
      76,    39,   112,   280,   281,   137,   138,   118,    54,    55,
      56,    57,   314,   289,   105,   115,   119,    45,    46,    47,
      48,    49,    50,    51,    52,    53,   106,   107,   108,   109,
    -123,   142,   330,   158,   187,   166,    93,   194,   315,   198,
     316,   317,   318,   319,   195,   197,   199,   140,   200,   201,
     340,   202,   217,   220,   218,   320,   225,   110,    54,    55,
      56,    57,   139,   230,    44,   233,   238,   239,   329,   241,
     242,   243,   244,   259,   110,   321,   123,   264,   222,   215,
     272,   277,    43,   342,    44,   292,   206,   207,   208,   209,
     210,   211,   346,    43,   282,    44,   187,    45,    46,    47,
      48,    49,    50,    51,    52,    53,   284,   139,   288,    44,
     291,   295,   303,   352,   312,   351,   313,    45,    46,    47,
      48,    49,    50,    51,    52,    53,   327,   140,    45,    46,
      47,    48,    49,    50,    51,    52,    53,   328,    87,    55,
      56,    57,    45,    46,    47,    48,    49,    50,    51,    52,
      53,   336,   106,   107,   108,   109,   331,   124,    87,    55,
      56,    57,   332,   337,   338,   341,   343,   344,    79,    54,
      55,    56,    57,    78,   349,   229,   275,   270,   300,   235,
     271,   153,   290,    54,    55,    56,    57,     1,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
     110,   247,   248,   249,   250,   251,   252,   253,   254,   255,
     256,   345,   206,   207,   208,   209,   210,   211,     1,     2,
       3,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    69,   315,   266,   316,   317,   318,   319,   283,   132,
     267,     0,   279,     0,     0,   205,     0,     0,    14,   320,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   321,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    14
};

static const yytype_int16 yycheck[] =
{
       2,     3,     8,    44,    10,     6,    43,    44,   139,     3,
      10,    28,    28,    10,    94,    95,    96,    97,    98,    99,
     100,     9,     7,     8,    10,   218,    18,   158,   259,    32,
      33,    37,   112,    12,    40,    10,     9,    90,    91,     9,
      35,    11,    43,     7,   175,     9,    19,    10,    10,    10,
      41,    10,   284,    10,    91,    10,   288,    10,    31,   139,
     140,    10,    68,     9,   295,    71,    33,    93,    53,    36,
      72,    77,    74,    89,    19,   268,    93,   242,   158,    49,
      50,    51,    52,    85,    86,    77,    31,   167,    53,   126,
      90,    91,    89,    84,    89,   175,    29,    85,    86,    93,
     101,   103,   139,    89,   105,   106,   107,   108,   109,   115,
      89,   113,   243,   119,    89,   195,    86,    87,    88,   284,
      85,    85,    86,   288,    71,    72,    89,    89,    89,     9,
      89,    11,    89,    85,    89,   215,    89,   217,   139,    85,
      89,    50,    51,    27,   309,    19,     3,     5,     6,     7,
       8,     3,    10,    10,   234,    50,    51,    32,    10,     3,
       7,    85,     9,   243,    11,    74,    10,   168,   170,    49,
      50,    51,    52,    89,    13,   177,    92,   214,   180,   259,
       5,     6,     7,     8,     3,   191,    80,    81,   194,   230,
      33,    10,     9,   230,   274,    53,   233,    44,    45,    46,
      47,    48,    49,    50,    51,    52,    86,    87,    88,     9,
     341,   212,     9,     9,    11,   295,    37,     9,    39,     9,
      41,     9,    43,   264,    32,    33,    27,   264,    53,    12,
      13,    39,    30,   235,   236,    97,    98,     9,    85,    86,
      87,    88,    11,   245,    89,    27,    79,    44,    45,    46,
      47,    48,    49,    50,    51,    52,     5,     6,     7,     8,
      89,   341,   303,    28,    85,    85,   303,    79,    37,    10,
      39,    40,    41,    42,    90,    85,    10,    74,    10,    10,
     317,    10,     4,    89,    73,    54,    34,    53,    85,    86,
      87,    88,     9,     9,    11,    65,    76,    76,   300,    38,
       9,     9,    38,     9,    53,    74,    10,     9,    85,     3,
      86,    89,     9,   319,    11,    11,    65,    66,    67,    68,
      69,    70,   324,     9,    85,    11,    85,    44,    45,    46,
      47,    48,    49,    50,    51,    52,     9,     9,     9,    11,
       9,     9,     9,   349,    86,   347,    12,    44,    45,    46,
      47,    48,    49,    50,    51,    52,    86,    74,    44,    45,
      46,    47,    48,    49,    50,    51,    52,    86,    85,    86,
      87,    88,    44,    45,    46,    47,    48,    49,    50,    51,
      52,    10,     5,     6,     7,     8,    85,    10,    85,    86,
      87,    88,    85,    12,    38,     9,    88,    52,    15,    85,
      86,    87,    88,     0,    42,   168,   229,   220,   273,   173,
     221,   101,   246,    85,    86,    87,    88,    14,    15,    16,
      17,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      53,    55,    56,    57,    58,    59,    60,    61,    62,    63,
      64,   322,    65,    66,    67,    68,    69,    70,    14,    15,
      16,    17,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    85,    37,   215,    39,    40,    41,    42,   239,    95,
     217,    -1,   234,    -1,    -1,   140,    -1,    -1,    75,    54,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    74,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    75
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    75,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,    93,   166,   166,   166,    32,    33,    39,
      32,    33,     7,     9,    11,    44,    45,    46,    47,    48,
      49,    50,    51,    52,    85,    86,    87,    88,   117,   130,
     131,   132,   144,   146,   147,   148,   149,   163,    29,    85,
     122,    27,   122,    33,    36,   104,   104,    32,     0,    96,
      13,   122,    85,   123,    33,   122,   123,    85,   131,   145,
     146,     9,   145,   146,     9,     9,     9,     9,     9,     9,
      90,    91,    27,   118,   119,    89,     5,     6,     7,     8,
      53,   122,    30,   122,   166,    27,   166,   122,     9,    79,
     123,   166,   166,    10,    10,    10,    89,    12,     9,    86,
     144,   165,   165,     7,    86,   144,   164,   164,   164,     9,
      74,   131,   144,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   144,   130,   120,   121,   122,   166,    28,   125,
     126,   131,   131,   131,   131,   131,    85,     9,    19,    31,
     127,   129,   144,   150,   151,    28,   166,   122,    18,    77,
     113,   114,   115,    37,    39,    41,    43,    85,   134,   135,
     136,   138,   143,   122,    79,    90,   146,    85,    10,    10,
      10,    10,    10,   131,   152,   155,    65,    66,    67,    68,
      69,    70,   162,    71,    72,     3,    10,     4,    73,    92,
      89,    53,    85,   124,   152,    34,   161,   133,   144,   117,
       9,   128,   166,    65,    89,   125,   152,   166,    76,    76,
     166,    38,     9,     9,    38,    10,    89,    55,    56,    57,
      58,    59,    60,    61,    62,    63,    64,   122,   139,     9,
     122,   144,    10,   131,     9,   146,   153,   154,    74,   149,
     121,   124,    86,    10,    89,   118,   145,    89,   146,   151,
     166,   166,    85,   136,     9,   137,   138,   152,     9,   166,
     135,     9,    11,   140,   133,     9,   145,   149,    35,    89,
     127,   144,    10,     9,    80,    81,   116,   137,    10,    89,
      10,   137,    86,    12,    11,    37,    39,    40,    41,    42,
      54,    74,   141,   142,    10,   133,    10,    86,    86,   166,
     145,    85,    85,    10,   138,    10,    10,    12,    38,    52,
     146,     9,   122,    88,    52,   142,   166,    10,    10,    42,
     152,   166,   122,    10
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,    94,    95,    95,    96,    96,    96,    96,    96,    96,
      96,    96,    96,    96,    96,    96,    96,    96,    96,    96,
      97,    98,    99,   100,   101,   101,   102,   103,   104,   105,
     105,   106,   107,   107,   108,   109,   109,   110,   111,   112,
     113,   113,   114,   115,   116,   116,   116,   117,   117,   118,
     119,   119,   120,   120,   121,   121,   121,   122,   123,   124,
     125,   125,   126,   127,   127,   128,   128,   129,   130,   130,
     131,   131,   131,   131,   131,   131,   132,   132,   132,   132,
     133,   133,   134,   134,   135,   135,   136,   137,   137,   138,
     139,   139,   139,   139,   139,   139,   139,   139,   139,   139,
     139,   140,   140,   140,   141,   141,   141,   142,   142,   142,
     142,   142,   142,   142,   142,   143,   143,   143,   143,   144,
     144,   144,   144,   145,   145,   146,   146,   146,   147,   147,
     147,   147,   147,   148,   148,   149,   149,   150,   150,   151,
     152,   152,   153,   153,   154,   154,   155,   155,   155,   156,
     156,   157,   157,   157,   158,   159,   160,   161,   161,   161,
     161,   162,   162,   162,   162,   162,   162,   163,   163,   163,
     163,   163,   164,   164,   164,   165,   165,   166,   166
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       2,     2,     2,     7,     9,    10,     4,     4,     4,     5,
       8,     6,     4,     6,     3,     3,     5,     2,     2,     5,
       1,     1,     4,     3,     0,     2,     2,     1,     1,     3,
       0,     2,     1,     3,     1,     2,     3,     1,     1,     1,
       0,     1,     2,     2,     1,     3,     5,     3,     1,     3,
       1,     1,     1,     1,     3,     3,     3,     3,     3,     3,
       1,     3,     1,     3,     1,     1,     4,     1,     3,     1,
       1,     1,     1,     4,     1,     1,     1,     1,     1,     1,
       1,     0,     2,     3,     0,     1,     2,     2,     1,     2,
       2,     2,     2,     4,     2,     4,     5,     7,     4,     1,
       5,     4,     3,     1,     3,     1,     1,     3,     1,     1,
       1,     1,     1,     3,     4,     1,     1,     1,     3,     3,
       1,     3,     1,     3,     1,     2,     1,     3,     4,     1,
       3,     1,     1,     1,     3,     3,     5,     0,     2,     4,
       4,     1,     1,     1,     1,     1,     1,     4,     4,     4,
       4,     4,     1,     1,     1,     1,     1,     1,     2
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
#line 194 "sql.y"
        {
            append_list(states, (yyvsp[0].statement));
            (yyval.list) = states;
        }
#line 2158 "y.tab.c"
    break;

  case 3: /* statements: statements statement  */
#line 199 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].statement));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 2167 "y.tab.c"
    break;

  case 4: /* statement: begin_transaction_statement  */
#line 206 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = BEGIN_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2177 "y.tab.c"
    break;

  case 5: /* statement: commit_transaction_statement  */
#line 212 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = COMMIT_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2187 "y.tab.c"
    break;

  case 6: /* statement: rollback_transaction_statement  */
#line 218 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ROLLBACK_TRANSACTION_STMT;
            (yyval.statement) = statement;
        }
#line 2197 "y.tab.c"
    break;

  case 7: /* statement: create_table_statement  */
#line 224 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_TABLE_STMT;
            statement->create_table_node = (yyvsp[0].create_table_node);
            (yyval.statement) = statement;
        }
#line 2208 "y.tab.c"
    break;

  case 8: /* statement: create_index_statement  */
#line 231 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = CREATE_INDEX_STMT;
            statement->create_index_node = (yyvsp[0].create_index_node);
            (yyval.statement) = statement;
        }
#line 2219 "y.tab.c"
    break;

  case 9: /* statement: drop_table_statement  */
#line 238 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_TABLE_STMT;
            statement->drop_table_node = (yyvsp[0].drop_table_node);
            (yyval.statement) = statement;
        }
#line 2230 "y.tab.c"
    break;

  case 10: /* statement: drop_index_statement  */
#line 245 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DROP_INDEX_STMT;
            statement->drop_index_node = (yyvsp[0].drop_index_node);
            (yyval.statement) = statement;
        }
#line 2241 "y.tab.c"
    break;

  case 11: /* statement: select_statement  */
#line 252 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SELECT_STMT;
            statement->select_node = (yyvsp[0].select_node);
            (yyval.statement) = statement;
        }
#line 2252 "y.tab.c"
    break;

  case 12: /* statement: insert_statement  */
#line 259 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = INSERT_STMT;
            statement->insert_node = (yyvsp[0].insert_node);
            (yyval.statement) = statement;
        }
#line 2263 "y.tab.c"
    break;

  case 13: /* statement: update_statement  */
#line 266 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = UPDATE_STMT;
            statement->update_node = (yyvsp[0].update_node);
            (yyval.statement) = statement;
        }
#line 2274 "y.tab.c"
    break;

  case 14: /* statement: delete_statement  */
#line 273 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DELETE_STMT;
            statement->delete_node = (yyvsp[0].delete_node);
            (yyval.statement) = statement;
        }
#line 2285 "y.tab.c"
    break;

  case 15: /* statement: describe_statement  */
#line 280 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = DESCRIBE_STMT;
            statement->describe_node = (yyvsp[0].describe_node);
            (yyval.statement) = statement;
        }
#line 2296 "y.tab.c"
    break;

  case 16: /* statement: show_statement  */
#line 287 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = SHOW_STMT;
            statement->show_node = (yyvsp[0].show_node);
            (yyval.statement) = statement;
        }
#line 2307 "y.tab.c"
    break;

  case 17: /* statement: explain_statement  */
#line 294 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = EXPLAIN_STMT;
            statement->explain_node = (yyvsp[0].explain_node);
            (yyval.statement) = statement;
        }
#line 2318 "y.tab.c"
    break;

  case 18: /* statement: express_statement  */
#line 301 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = EXPRESS_STMT;
            statement->express_node = (yyvsp[0].express_node);
            (yyval.statement) = statement;
        }
#line 2329 "y.tab.c"
    break;

  case 19: /* statement: alter_table_statement  */
#line 308 "sql.y"
        {
            Statement *statement = instance(Statement);
            statement->statement_type = ALTER_TABLE_STMT;
            statement->alter_table_node = (yyvsp[0].alter_table_node);
            (yyval.statement) = statement;
        }
#line 2340 "y.tab.c"
    break;

  case 23: /* create_table_statement: CREATE TABLE table '(' base_table_element_commalist ')' end  */
#line 327 "sql.y"
        {
            CreateTableNode *create_table_node = instance(CreateTableNode);
            create_table_node->table_name = (yyvsp[-4].strVal);
            create_table_node->base_table_element_commalist = (yyvsp[-2].list);
            (yyval.create_table_node) = create_table_node;
        }
#line 2351 "y.tab.c"
    break;

  case 24: /* create_index_statement: CREATE INDEX index_name ON table '(' columns ')' end  */
#line 337 "sql.y"
        {
            CreateIndexNode *create_index_node = instance(CreateIndexNode);
            create_index_node->index_name = (yyvsp[-6].strVal);
            create_index_node->table_name = (yyvsp[-4].strVal);
            create_index_node->is_unique = false;
            create_index_node->type = BTREE_INDEX;
            create_index_node->columns = (yyvsp[-2].list);
            (yyval.create_index_node) = create_index_node;
        }
#line 2365 "y.tab.c"
    break;

  case 25: /* create_index_statement: CREATE UNIQUE INDEX index_name ON table '(' columns ')' end  */
#line 347 "sql.y"
        {
            CreateIndexNode *create_index_node = instance(CreateIndexNode);
            create_index_node->index_name = (yyvsp[-6].strVal);
            create_index_node->table_name = (yyvsp[-4].strVal);
            create_index_node->is_unique = true;
            create_index_node->type = BTREE_INDEX;
            create_index_node->columns = (yyvsp[-2].list);
            (yyval.create_index_node) = create_index_node;
        }
#line 2379 "y.tab.c"
    break;

  case 26: /* drop_table_statement: DROP TABLE table end  */
#line 360 "sql.y"
        {
            DropTableNode *drop_table_node = instance(DropTableNode);
            drop_table_node->table_name = (yyvsp[-1].strVal);
            (yyval.drop_table_node) = drop_table_node;
        }
#line 2389 "y.tab.c"
    break;

  case 27: /* drop_index_statement: DROP INDEX index_name end  */
#line 369 "sql.y"
        {
            DropIndexNode *drop_index_node = instance(DropIndexNode);
            drop_index_node->index_name = (yyvsp[-1].strVal);
            (yyval.drop_index_node) = drop_index_node;
        }
#line 2399 "y.tab.c"
    break;

  case 28: /* select_statement: SELECT selection table_exp end  */
#line 378 "sql.y"
        {
            SelectNode *select_node = instance(SelectNode);
            select_node->selection = (yyvsp[-2].selection_node);
            select_node->table_exp = (yyvsp[-1].table_exp_node);
            (yyval.select_node) = select_node;
        }
#line 2410 "y.tab.c"
    break;

  case 29: /* insert_statement: INSERT INTO table values_or_query_spec end  */
#line 388 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = true;
            node->table_name = (yyvsp[-2].strVal);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2422 "y.tab.c"
    break;

  case 30: /* insert_statement: INSERT INTO table '(' columns ')' values_or_query_spec end  */
#line 396 "sql.y"
        {
            InsertNode *node = instance(InsertNode);
            node->all_column = false;
            node->table_name = (yyvsp[-5].strVal);
            node->column_list = (yyvsp[-3].list);
            node->values_or_query_spec = (yyvsp[-1].values_or_query_spec_node);
            (yyval.insert_node) = node;
        }
#line 2435 "y.tab.c"
    break;

  case 31: /* update_statement: UPDATE table SET assignments opt_where_clause end  */
#line 408 "sql.y"
        {
            UpdateNode *node = instance(UpdateNode);
            node->table_name = (yyvsp[-4].strVal);
            node->assignment_list = (yyvsp[-2].list);
            node->where_clause = (yyvsp[-1].where_clause_node);
            (yyval.update_node) = node;
        }
#line 2447 "y.tab.c"
    break;

  case 32: /* delete_statement: DELETE FROM table end  */
#line 419 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.delete_node) = node;
        }
#line 2457 "y.tab.c"
    break;

  case 33: /* delete_statement: DELETE FROM table WHERE search_condition end  */
#line 425 "sql.y"
        {
            DeleteNode *node = instance(DeleteNode);
            node->table_name = (yyvsp[-3].strVal);
            node->condition_node = (yyvsp[-1].search_condition_node);
            (yyval.delete_node) = node;
        }
#line 2468 "y.tab.c"
    break;

  case 34: /* describe_statement: DESCRIBE table end  */
#line 435 "sql.y"
        {
            DescribeNode *node = instance(DescribeNode);
            node->table_name = (yyvsp[-1].strVal);
            (yyval.describe_node) = node;
        }
#line 2478 "y.tab.c"
    break;

  case 35: /* show_statement: SHOW TABLES end  */
#line 444 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_TABLES;
            (yyval.show_node) = node;
        }
#line 2488 "y.tab.c"
    break;

  case 36: /* show_statement: SHOW INDEX FROM table end  */
#line 450 "sql.y"
        {
            ShowNode *node = instance(ShowNode);   
            node->type = SHOW_IDNEXS;
            node->table_name = (yyvsp[-1].strVal);
            (yyval.show_node) = node;
        }
#line 2499 "y.tab.c"
    break;

  case 37: /* explain_statement: EXPLAIN select_statement  */
#line 460 "sql.y"
        {
            ExplainNode *node = instance(ExplainNode);
            node->select_node = (yyvsp[0].select_node);
            (yyval.explain_node) = node;
        }
#line 2509 "y.tab.c"
    break;

  case 38: /* express_statement: EXPRESS select_statement  */
#line 469 "sql.y"
        {
            ExpressNode *node = instance(ExpressNode);
            node->select_node = (yyvsp[0].select_node);
            (yyval.express_node) = node;
        }
#line 2519 "y.tab.c"
    break;

  case 39: /* alter_table_statement: ALTER TABLE table alter_table_action end  */
#line 478 "sql.y"
        {
            (yyval.alter_table_node) = instance(AlterTableNode);
            (yyval.alter_table_node)->table_name = (yyvsp[-2].strVal);
            (yyval.alter_table_node)->action = (yyvsp[-1].alter_table_action);
        }
#line 2529 "y.tab.c"
    break;

  case 40: /* alter_table_action: add_column_def  */
#line 486 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_ADD_COLUMN;
            action->action.add_column = (yyvsp[0].add_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2540 "y.tab.c"
    break;

  case 41: /* alter_table_action: drop_column_def  */
#line 493 "sql.y"
        {
            AlterTableAction *action = instance(AlterTableAction);
            action->type = ALTER_TO_DROP_COLUMN;
            action->action.drop_column = (yyvsp[0].drop_column_def);
            (yyval.alter_table_action) = action;
        }
#line 2551 "y.tab.c"
    break;

  case 42: /* add_column_def: ADD COLUMN column_def column_position_def  */
#line 502 "sql.y"
        {
            AddColumnDef *node = instance(AddColumnDef);
            node->column_def = (yyvsp[-1].column_def_node);
            node->position_def = (yyvsp[0].column_position_def);
            (yyval.add_column_def) = node;
        }
#line 2562 "y.tab.c"
    break;

  case 43: /* drop_column_def: DROP COLUMN IDENTIFIER  */
#line 511 "sql.y"
        {
            DropColumnDef *node = instance(DropColumnDef);
            node->column_name = (yyvsp[0].strVal);
            (yyval.drop_column_def) = node;
        }
#line 2572 "y.tab.c"
    break;

  case 44: /* column_position_def: %empty  */
#line 519 "sql.y"
    {
        (yyval.column_position_def) = NULL;
    }
#line 2580 "y.tab.c"
    break;

  case 45: /* column_position_def: BEFORE IDENTIFIER  */
#line 523 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_BEFORE;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2591 "y.tab.c"
    break;

  case 46: /* column_position_def: AFTER IDENTIFIER  */
#line 530 "sql.y"
        {
            ColumnPositionDef *pos = instance(ColumnPositionDef);
            pos->type = POS_AFTER;
            pos->column = (yyvsp[0].strVal);
            (yyval.column_position_def) = pos;
        }
#line 2602 "y.tab.c"
    break;

  case 47: /* selection: scalar_exp_commalist  */
#line 539 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = false;
            selection_node->scalar_exp_list = (yyvsp[0].list);
            (yyval.selection_node) = selection_node;
        }
#line 2613 "y.tab.c"
    break;

  case 48: /* selection: '*'  */
#line 546 "sql.y"
        {
            SelectionNode *selection_node = instance(SelectionNode);
            selection_node->all_column = true;
            (yyval.selection_node) = selection_node;
        }
#line 2623 "y.tab.c"
    break;

  case 49: /* table_exp: from_clause opt_where_clause limit_clause  */
#line 554 "sql.y"
        {
            TableExpNode *table_exp = instance(TableExpNode);
            table_exp->from_clause = (yyvsp[-2].from_clause_node);
            table_exp->where_clause = (yyvsp[-1].where_clause_node);
            table_exp->limit_clause = (yyvsp[0].limit_clause_node);
            (yyval.table_exp_node) = table_exp;
        }
#line 2635 "y.tab.c"
    break;

  case 50: /* from_clause: %empty  */
#line 564 "sql.y"
        {
            (yyval.from_clause_node) = NULL;
        }
#line 2643 "y.tab.c"
    break;

  case 51: /* from_clause: FROM table_ref_commalist  */
#line 568 "sql.y"
        {
            FromClauseNode *from_clause = instance(FromClauseNode);
            from_clause->from = (yyvsp[0].list);
            (yyval.from_clause_node) = from_clause;
        }
#line 2653 "y.tab.c"
    break;

  case 52: /* table_ref_commalist: table_ref  */
#line 576 "sql.y"
        {
            List *list = create_list(NODE_TABLE_REFER);
            append_list(list, (yyvsp[0].table_ref_node));
            (yyval.list) = list;
        }
#line 2663 "y.tab.c"
    break;

  case 53: /* table_ref_commalist: table_ref_commalist ',' table_ref  */
#line 582 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].table_ref_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2672 "y.tab.c"
    break;

  case 54: /* table_ref: table  */
#line 589 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2682 "y.tab.c"
    break;

  case 55: /* table_ref: table range_variable  */
#line 595 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-1].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2693 "y.tab.c"
    break;

  case 56: /* table_ref: table AS range_variable  */
#line 602 "sql.y"
        {
            TableRefNode *table_ref = instance(TableRefNode);
            table_ref->table = (yyvsp[-2].strVal);
            table_ref->range_variable = (yyvsp[0].strVal);
            (yyval.table_ref_node) = table_ref;
        }
#line 2704 "y.tab.c"
    break;

  case 57: /* table: IDENTIFIER  */
#line 611 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2712 "y.tab.c"
    break;

  case 58: /* index_name: IDENTIFIER  */
#line 617 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2720 "y.tab.c"
    break;

  case 59: /* range_variable: IDENTIFIER  */
#line 623 "sql.y"
        {
            (yyval.strVal) = (yyvsp[0].strVal);
        }
#line 2728 "y.tab.c"
    break;

  case 60: /* opt_where_clause: %empty  */
#line 629 "sql.y"
        {
            (yyval.where_clause_node) = NULL;
        }
#line 2736 "y.tab.c"
    break;

  case 61: /* opt_where_clause: where_clause  */
#line 633 "sql.y"
        {
            (yyval.where_clause_node) = (yyvsp[0].where_clause_node);
        }
#line 2744 "y.tab.c"
    break;

  case 62: /* where_clause: WHERE search_condition  */
#line 639 "sql.y"
        {
            WhereClauseNode *where_clause_node = instance(WhereClauseNode);
            where_clause_node->condition = (yyvsp[0].search_condition_node);
            (yyval.where_clause_node) = where_clause_node;
        }
#line 2754 "y.tab.c"
    break;

  case 63: /* values_or_query_spec: VALUES opt_values  */
#line 647 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_VALUES;
            values_or_query_spec->values = (yyvsp[0].list);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2765 "y.tab.c"
    break;

  case 64: /* values_or_query_spec: query_spec  */
#line 654 "sql.y"
        {
            ValuesOrQuerySpecNode *values_or_query_spec = instance(ValuesOrQuerySpecNode);
            values_or_query_spec->type = VQ_QUERY_SPEC;
            values_or_query_spec->query_spec = (yyvsp[0].query_spec_node);
            (yyval.values_or_query_spec_node) = values_or_query_spec;
        }
#line 2776 "y.tab.c"
    break;

  case 65: /* opt_values: '(' value_items ')'  */
#line 663 "sql.y"
        {
            (yyval.list) = create_list(NODE_LIST);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2785 "y.tab.c"
    break;

  case 66: /* opt_values: opt_values ',' '(' value_items ')'  */
#line 668 "sql.y"
        {
            (yyval.list) = (yyvsp[-4].list);
            append_list((yyval.list), (yyvsp[-1].list));
        }
#line 2794 "y.tab.c"
    break;

  case 67: /* query_spec: SELECT selection table_exp  */
#line 675 "sql.y"
        {
            QuerySpecNode *query_spec = instance(QuerySpecNode);
            query_spec->selection = (yyvsp[-1].selection_node);
            query_spec->table_exp = (yyvsp[0].table_exp_node);
            (yyval.query_spec_node) = query_spec;
        }
#line 2805 "y.tab.c"
    break;

  case 68: /* scalar_exp_commalist: scalar_exp  */
#line 684 "sql.y"
        {
            List *scalar_exp_list = create_list(NODE_SCALAR_EXP);
            append_list(scalar_exp_list, (yyvsp[0].scalar_exp_node));
            (yyval.list) = scalar_exp_list;
        }
#line 2815 "y.tab.c"
    break;

  case 69: /* scalar_exp_commalist: scalar_exp_commalist ',' scalar_exp  */
#line 690 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].scalar_exp_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2824 "y.tab.c"
    break;

  case 70: /* scalar_exp: calculate  */
#line 697 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_CALCULATE;
            scalar_exp_node->calculate = (yyvsp[0].calculate_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2835 "y.tab.c"
    break;

  case 71: /* scalar_exp: column  */
#line 704 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_COLUMN;
            scalar_exp_node->column = (yyvsp[0].column_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2846 "y.tab.c"
    break;

  case 72: /* scalar_exp: function  */
#line 711 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_FUNCTION;
            scalar_exp_node->function = (yyvsp[0].function_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2857 "y.tab.c"
    break;

  case 73: /* scalar_exp: value_item  */
#line 718 "sql.y"
        {
            ScalarExpNode *scalar_exp_node = instance(ScalarExpNode);
            scalar_exp_node->type = SCALAR_VALUE;
            scalar_exp_node->value = (yyvsp[0].value_item_node);
            (yyval.scalar_exp_node) = scalar_exp_node;
        }
#line 2868 "y.tab.c"
    break;

  case 74: /* scalar_exp: '(' scalar_exp ')'  */
#line 725 "sql.y"
        {
            (yyval.scalar_exp_node) = (yyvsp[-1].scalar_exp_node);
        }
#line 2876 "y.tab.c"
    break;

  case 75: /* scalar_exp: scalar_exp AS IDENTIFIER  */
#line 729 "sql.y"
        {
            (yyvsp[-2].scalar_exp_node)->alias = (yyvsp[0].strVal);
            (yyval.scalar_exp_node) = (yyvsp[-2].scalar_exp_node);
        }
#line 2885 "y.tab.c"
    break;

  case 76: /* calculate: scalar_exp '+' scalar_exp  */
#line 736 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_ADD;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2897 "y.tab.c"
    break;

  case 77: /* calculate: scalar_exp '-' scalar_exp  */
#line 744 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_SUB;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2909 "y.tab.c"
    break;

  case 78: /* calculate: scalar_exp '*' scalar_exp  */
#line 752 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_MUL;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2921 "y.tab.c"
    break;

  case 79: /* calculate: scalar_exp '/' scalar_exp  */
#line 760 "sql.y"
        {
            CalculateNode *calculate_node = instance(CalculateNode);
            calculate_node->type = CAL_DIV;
            calculate_node->left = (yyvsp[-2].scalar_exp_node);
            calculate_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.calculate_node) = calculate_node;
        }
#line 2933 "y.tab.c"
    break;

  case 80: /* columns: column  */
#line 770 "sql.y"
        {
            List *column_set_node = create_list(NODE_COLUMN);
            append_list(column_set_node, (yyvsp[0].column_node));
            (yyval.list) = column_set_node;
        }
#line 2943 "y.tab.c"
    break;

  case 81: /* columns: columns ',' column  */
#line 776 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].column_node));
        }
#line 2952 "y.tab.c"
    break;

  case 82: /* base_table_element_commalist: base_table_element  */
#line 783 "sql.y"
        {
            List *base_table_element_commalist = create_list(NODE_BASE_TABLE_ELEMENT);
            append_list(base_table_element_commalist, (yyvsp[0].base_table_element));
            (yyval.list) = base_table_element_commalist;
        }
#line 2962 "y.tab.c"
    break;

  case 83: /* base_table_element_commalist: base_table_element_commalist ',' base_table_element  */
#line 789 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].base_table_element));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 2971 "y.tab.c"
    break;

  case 84: /* base_table_element: column_def  */
#line 796 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = (yyvsp[0].column_def_node);
            node->table_contraint_def = NULL;
            node->type = TELE_COLUMN_DEF;
            (yyval.base_table_element) = node;
        }
#line 2983 "y.tab.c"
    break;

  case 85: /* base_table_element: table_contraint_def  */
#line 804 "sql.y"
        {
            BaseTableElementNode *node = instance(BaseTableElementNode);
            node->column_def = NULL;
            node->table_contraint_def = (yyvsp[0].table_contraint_def);
            node->type = TELE_TABLE_CONTRAINT_DEF;
            (yyval.base_table_element) = node;
        }
#line 2995 "y.tab.c"
    break;

  case 86: /* column_def: column_def_name data_type array_dim_clause column_def_opt_list  */
#line 827 "sql.y"
        {
            ColumnDefNode *column_def = instance(ColumnDefNode);
            column_def->column = (yyvsp[-3].column_def_name);
            column_def->data_type = (yyvsp[-2].data_type_node);
            column_def->array_dim = (yyvsp[-1].intVal);
            column_def->column_def_opt_list = (yyvsp[0].list);
            (yyval.column_def_node) = column_def;
        }
#line 3008 "y.tab.c"
    break;

  case 87: /* column_def_name_commalist: column_def_name  */
#line 838 "sql.y"
        {
            List *list = create_list(NODE_COLUMN_DEF_NAME);
            append_list(list, (yyvsp[0].column_def_name));
            (yyval.list) = list;
        }
#line 3018 "y.tab.c"
    break;

  case 88: /* column_def_name_commalist: column_def_name_commalist ',' column_def_name  */
#line 844 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].column_def_name));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 3027 "y.tab.c"
    break;

  case 89: /* column_def_name: IDENTIFIER  */
#line 851 "sql.y"
        {
            ColumnDefName *column_def_name = instance(ColumnDefName);
            column_def_name->column = (yyvsp[0].strVal);
            (yyval.column_def_name) = column_def_name;
        }
#line 3037 "y.tab.c"
    break;

  case 90: /* data_type: INT  */
#line 859 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_INT; 
            (yyval.data_type_node) = node;
        }
#line 3047 "y.tab.c"
    break;

  case 91: /* data_type: LONG  */
#line 865 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_LONG;  
            (yyval.data_type_node) = node;
        }
#line 3057 "y.tab.c"
    break;

  case 92: /* data_type: CHAR  */
#line 871 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_CHAR; 
            (yyval.data_type_node) = node;
        }
#line 3067 "y.tab.c"
    break;

  case 93: /* data_type: VARCHAR '(' INTVALUE ')'  */
#line 877 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_VARCHAR; 
            node->len = (yyvsp[-1].intVal);
            (yyval.data_type_node) = node;
        }
#line 3078 "y.tab.c"
    break;

  case 94: /* data_type: STRING  */
#line 884 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_STRING; 
            (yyval.data_type_node) = node;
        }
#line 3088 "y.tab.c"
    break;

  case 95: /* data_type: BOOL  */
#line 890 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_BOOL; 
            (yyval.data_type_node) = node;
        }
#line 3098 "y.tab.c"
    break;

  case 96: /* data_type: FLOAT  */
#line 896 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_FLOAT; 
            (yyval.data_type_node) = node;
        }
#line 3108 "y.tab.c"
    break;

  case 97: /* data_type: DOUBLE  */
#line 902 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DOUBLE; 
            (yyval.data_type_node) = node;
        }
#line 3118 "y.tab.c"
    break;

  case 98: /* data_type: TIMESTAMP  */
#line 908 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_TIMESTAMP; 
            (yyval.data_type_node) = node;
        }
#line 3128 "y.tab.c"
    break;

  case 99: /* data_type: DATE  */
#line 914 "sql.y"
        { 
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_DATE; 
            (yyval.data_type_node) = node;
        }
#line 3138 "y.tab.c"
    break;

  case 100: /* data_type: table  */
#line 920 "sql.y"
        {
            DataTypeNode *node = instance(DataTypeNode);                
            node->type = T_RID;
            node->table_name = (yyvsp[0].strVal);
            (yyval.data_type_node) = node;
        }
#line 3149 "y.tab.c"
    break;

  case 101: /* array_dim_clause: %empty  */
#line 929 "sql.y"
        {
            (yyval.intVal) = 0;
        }
#line 3157 "y.tab.c"
    break;

  case 102: /* array_dim_clause: '[' ']'  */
#line 933 "sql.y"
        {
            (yyval.intVal) = 1;
        }
#line 3165 "y.tab.c"
    break;

  case 103: /* array_dim_clause: array_dim_clause '[' ']'  */
#line 937 "sql.y"
        {
            (yyval.intVal)++;
        }
#line 3173 "y.tab.c"
    break;

  case 104: /* column_def_opt_list: %empty  */
#line 943 "sql.y"
        {
            (yyval.list) = NULL;
        }
#line 3181 "y.tab.c"
    break;

  case 105: /* column_def_opt_list: column_def_opt  */
#line 947 "sql.y"
        {
            (yyval.list) = create_list(NODE_COLUMN_DEF_OPT);
            append_list((yyval.list), (yyvsp[0].column_def_opt));
        }
#line 3190 "y.tab.c"
    break;

  case 106: /* column_def_opt_list: column_def_opt_list column_def_opt  */
#line 952 "sql.y"
        {
            append_list((yyvsp[-1].list), (yyvsp[0].column_def_opt));
            (yyval.list) = (yyvsp[-1].list);
        }
#line 3199 "y.tab.c"
    break;

  case 107: /* column_def_opt: NOT NULLX  */
#line 959 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_NOT_NULL; 
            (yyval.column_def_opt) = node;
        }
#line 3209 "y.tab.c"
    break;

  case 108: /* column_def_opt: UNIQUE  */
#line 965 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_UNIQUE; 
            (yyval.column_def_opt) = node;
        }
#line 3219 "y.tab.c"
    break;

  case 109: /* column_def_opt: PRIMARY KEY  */
#line 971 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_PRIMARY_KEY; 
            (yyval.column_def_opt) = node;
        }
#line 3229 "y.tab.c"
    break;

  case 110: /* column_def_opt: DEFAULT value_item  */
#line 977 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_VALUE;
            node->value = (yyvsp[0].value_item_node);
            (yyval.column_def_opt) = node;
        }
#line 3240 "y.tab.c"
    break;

  case 111: /* column_def_opt: DEFAULT NULLX  */
#line 984 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_DEFAULT_NULL;
            (yyval.column_def_opt) = node;
        }
#line 3250 "y.tab.c"
    break;

  case 112: /* column_def_opt: COMMENT STRINGVALUE  */
#line 990 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_COMMENT;
            node->comment = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3261 "y.tab.c"
    break;

  case 113: /* column_def_opt: CHECK '(' search_condition ')'  */
#line 997 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_CHECK_CONDITION;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.column_def_opt) = node;
        }
#line 3272 "y.tab.c"
    break;

  case 114: /* column_def_opt: REFERENCES table  */
#line 1004 "sql.y"
        {
            ColumnDefOptNode *node = instance(ColumnDefOptNode);
            node->opt_type = OPT_REFERENECS;
            node->refer_table = (yyvsp[0].strVal);
            (yyval.column_def_opt) = node;
        }
#line 3283 "y.tab.c"
    break;

  case 115: /* table_contraint_def: UNIQUE '(' column_def_name_commalist ')'  */
#line 1013 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_UNIQUE;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3294 "y.tab.c"
    break;

  case 116: /* table_contraint_def: PRIMARY KEY '(' column_def_name_commalist ')'  */
#line 1020 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_PRIMARY_KEY;
            node->column_commalist = (yyvsp[-1].list);
            (yyval.table_contraint_def) = node;
        }
#line 3305 "y.tab.c"
    break;

  case 117: /* table_contraint_def: FOREIGN KEY '(' column_def_name_commalist ')' REFERENCES table  */
#line 1027 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_FOREIGN_KEY;
            node->column_commalist = (yyvsp[-3].list);
            node->table = (yyvsp[0].strVal);
            (yyval.table_contraint_def) = node;
        }
#line 3317 "y.tab.c"
    break;

  case 118: /* table_contraint_def: CHECK '(' search_condition ')'  */
#line 1035 "sql.y"
        {
            TableContraintDefNode *node = instance(TableContraintDefNode);
            node->type = TCONTRAINT_CHECK;
            node->condition = (yyvsp[-1].search_condition_node);
            (yyval.table_contraint_def) = node;
        }
#line 3328 "y.tab.c"
    break;

  case 119: /* column: IDENTIFIER  */
#line 1044 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[0].strVal);
            column_node->has_sub_column = false;
            (yyval.column_node) = column_node;
        }
#line 3339 "y.tab.c"
    break;

  case 120: /* column: '(' IDENTIFIER ')' '.' column  */
#line 1051 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->sub_column = (yyvsp[0].column_node);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3351 "y.tab.c"
    break;

  case 121: /* column: IDENTIFIER '{' scalar_exp_commalist '}'  */
#line 1059 "sql.y"
        {
            ColumnNode *column_node = instance(ColumnNode);
            column_node->column_name = (yyvsp[-3].strVal);
            column_node->scalar_exp_list = (yyvsp[-1].list);
            column_node->has_sub_column = true;
            (yyval.column_node) = column_node;
        }
#line 3363 "y.tab.c"
    break;

  case 122: /* column: IDENTIFIER '.' column  */
#line 1067 "sql.y"
        {
            (yyval.column_node) = (yyvsp[0].column_node);
            (yyval.column_node)->range_variable = (yyvsp[-2].strVal);
        }
#line 3372 "y.tab.c"
    break;

  case 123: /* value_items: value_item  */
#line 1074 "sql.y"
        {
            List *value_list = create_list(NODE_VALUE_ITEM);
            append_list(value_list, (yyvsp[0].value_item_node));
            (yyval.list) = value_list;
        }
#line 3382 "y.tab.c"
    break;

  case 124: /* value_items: value_items ',' value_item  */
#line 1080 "sql.y"
        {
            (yyval.list) = (yyvsp[-2].list);
            append_list((yyval.list), (yyvsp[0].value_item_node));
        }
#line 3391 "y.tab.c"
    break;

  case 125: /* value_item: atom  */
#line 1087 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ATOM;
            node->value.atom = (yyvsp[0].atom_node);
            (yyval.value_item_node) = node;
        }
#line 3402 "y.tab.c"
    break;

  case 126: /* value_item: NULLX  */
#line 1094 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_NULL;
            (yyval.value_item_node) = node;
        }
#line 3412 "y.tab.c"
    break;

  case 127: /* value_item: '[' value_items ']'  */
#line 1100 "sql.y"
        {
            ValueItemNode *node = instance(ValueItemNode);
            node->type = V_ARRAY;
            node->value.value_list = (yyvsp[-1].list);
            (yyval.value_item_node) = node;
        }
#line 3423 "y.tab.c"
    break;

  case 128: /* atom: INTVALUE  */
#line 1109 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.intval = (yyvsp[0].intVal);
            node->type = A_INT;
            (yyval.atom_node) = node;
        }
#line 3434 "y.tab.c"
    break;

  case 129: /* atom: BOOLVALUE  */
#line 1116 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.boolval = (yyvsp[0].boolVal);
            node->type = A_BOOL;
            (yyval.atom_node) = node;
        }
#line 3445 "y.tab.c"
    break;

  case 130: /* atom: STRINGVALUE  */
#line 1123 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.strval = (yyvsp[0].strVal);
            node->type = A_STRING;
            (yyval.atom_node) = node;
        }
#line 3456 "y.tab.c"
    break;

  case 131: /* atom: FLOATVALUE  */
#line 1130 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.floatval = (yyvsp[0].floatVal);
            node->type = A_FLOAT;
            (yyval.atom_node) = node;
        }
#line 3467 "y.tab.c"
    break;

  case 132: /* atom: REFERVALUE  */
#line 1137 "sql.y"
        {
            AtomNode *node = instance(AtomNode);
            node->value.referval = (yyvsp[0].referVal);
            node->type = A_REFERENCE;
            (yyval.atom_node) = node;
        }
#line 3478 "y.tab.c"
    break;

  case 133: /* REFERVALUE: '(' value_items ')'  */
#line 1147 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = DIRECTLY;
            refer->nest_value_list = (yyvsp[-1].list);
            (yyval.referVal) = refer;
        }
#line 3489 "y.tab.c"
    break;

  case 134: /* REFERVALUE: REF '(' search_condition ')'  */
#line 1155 "sql.y"
        {
            ReferValue *refer = instance(ReferValue);
            refer->type = INDIRECTLY;
            refer->condition = (yyvsp[-1].search_condition_node);
            (yyval.referVal) = refer;
        }
#line 3500 "y.tab.c"
    break;

  case 135: /* BOOLVALUE: TRUE  */
#line 1164 "sql.y"
        {
            (yyval.boolVal) = true;
        }
#line 3508 "y.tab.c"
    break;

  case 136: /* BOOLVALUE: FALSE  */
#line 1168 "sql.y"
        {
            (yyval.boolVal) = false;
        }
#line 3516 "y.tab.c"
    break;

  case 137: /* assignments: assignment  */
#line 1174 "sql.y"
        {
            List *list = create_list(NODE_ASSIGNMENT);
            append_list(list, (yyvsp[0].assignment_node));
            (yyval.list) = list;
        }
#line 3526 "y.tab.c"
    break;

  case 138: /* assignments: assignments ',' assignment  */
#line 1180 "sql.y"
        {
            append_list((yyvsp[-2].list), (yyvsp[0].assignment_node));
            (yyval.list) = (yyvsp[-2].list);
        }
#line 3535 "y.tab.c"
    break;

  case 139: /* assignment: column EQ value_item  */
#line 1187 "sql.y"
        {
            AssignmentNode *node = instance(AssignmentNode);
            node->column = (yyvsp[-2].column_node);
            node->value = (yyvsp[0].value_item_node);
            (yyval.assignment_node) = node;
        }
#line 3546 "y.tab.c"
    break;

  case 140: /* search_condition: boolean_term  */
#line 1196 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3556 "y.tab.c"
    break;

  case 141: /* search_condition: search_condition OR boolean_term  */
#line 1202 "sql.y"
        {
            SearchConditionNode *condition = instance(SearchConditionNode);
            condition->or_search_condition = (yyvsp[-2].search_condition_node);
            condition->boolean_term = (yyvsp[0].boolean_term_node);
            (yyval.search_condition_node) = condition;
        }
#line 3567 "y.tab.c"
    break;

  case 142: /* boolean_term: boolean_factor  */
#line 1211 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3577 "y.tab.c"
    break;

  case 143: /* boolean_term: boolean_term AND boolean_factor  */
#line 1217 "sql.y"
        {
            BooleanTermNode *term_node = instance(BooleanTermNode);
            term_node->and_boolean_term = (yyvsp[-2].boolean_term_node);
            term_node->boolean_factor = (yyvsp[0].boolean_factor_node);
            (yyval.boolean_term_node) = term_node;
        }
#line 3588 "y.tab.c"
    break;

  case 144: /* boolean_factor: boolean_test  */
#line 1226 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = false;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3599 "y.tab.c"
    break;

  case 145: /* boolean_factor: NOT boolean_test  */
#line 1233 "sql.y"
        {
            BooleanFactorNode *factor_node = instance(BooleanFactorNode);
            factor_node->boolean_test = (yyvsp[0].boolean_test_node);
            factor_node->is_not = true;
            (yyval.boolean_factor_node) = factor_node;
        }
#line 3610 "y.tab.c"
    break;

  case 146: /* boolean_test: boolean_primary  */
#line 1242 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[0].boolean_primary_node);
            test_node->type = NONE_TRUE_VALUE;
            (yyval.boolean_test_node) = test_node;
        }
#line 3621 "y.tab.c"
    break;

  case 147: /* boolean_test: boolean_primary IS BOOLVALUE  */
#line 1249 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-2].boolean_primary_node);
            test_node->type = IS_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[0].boolVal);
            (yyval.boolean_test_node) = test_node;
        }
#line 3633 "y.tab.c"
    break;

  case 148: /* boolean_test: boolean_primary IS NOT BOOLVALUE  */
#line 1257 "sql.y"
        {
            BooleanTestNode *test_node = instance(BooleanTestNode);
            test_node->boolean_primary = (yyvsp[-3].boolean_primary_node);
            test_node->type = IS_NOT_TRUTH_VALUE;
            test_node->truth_value = (yyvsp[-1].keyword);
            (yyval.boolean_test_node) = test_node;
        }
#line 3645 "y.tab.c"
    break;

  case 149: /* boolean_primary: predicate  */
#line 1267 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = PREDICATE_BOOLEAN_PRIMAYR;
            primary_node->predicate = (yyvsp[0].predicate_node);
            primary_node->search_condition = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3657 "y.tab.c"
    break;

  case 150: /* boolean_primary: '(' search_condition ')'  */
#line 1275 "sql.y"
        {
            BooleanPrimaryNode *primary_node = instance(BooleanPrimaryNode);
            primary_node->type = SEARCH_CONDITION_BOOLEAN_PRIMAYR;
            primary_node->search_condition = (yyvsp[-1].search_condition_node);
            primary_node->predicate = NULL;
            (yyval.boolean_primary_node) = primary_node;
        }
#line 3669 "y.tab.c"
    break;

  case 151: /* predicate: comparison_predicate  */
#line 1284 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_COMPARISON;
            predicate->comparison = (yyvsp[0].comparison_node);
            (yyval.predicate_node) = predicate;
        }
#line 3680 "y.tab.c"
    break;

  case 152: /* predicate: like_predicate  */
#line 1291 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_LIKE;
            predicate->like = (yyvsp[0].like_node);
            (yyval.predicate_node) = predicate;
        }
#line 3691 "y.tab.c"
    break;

  case 153: /* predicate: in_predicate  */
#line 1298 "sql.y"
        {
            PredicateNode *predicate = instance(PredicateNode);
            predicate->type = PRE_IN;
            predicate->in = (yyvsp[0].in_node);
            (yyval.predicate_node) = predicate;
        }
#line 3702 "y.tab.c"
    break;

  case 154: /* comparison_predicate: scalar_exp compare scalar_exp  */
#line 1307 "sql.y"
        {
            ComparisonNode *comparison_node = instance(ComparisonNode);
            comparison_node->left = (yyvsp[-2].scalar_exp_node);
            comparison_node->type = (yyvsp[-1].compare_type);
            comparison_node->right = (yyvsp[0].scalar_exp_node);
            (yyval.comparison_node) = comparison_node;
        }
#line 3714 "y.tab.c"
    break;

  case 155: /* like_predicate: column LIKE value_item  */
#line 1317 "sql.y"
        {
            LikeNode *like_node = instance(LikeNode);
            like_node->column = (yyvsp[-2].column_node);
            like_node->value = (yyvsp[0].value_item_node);
            (yyval.like_node) = like_node;
        }
#line 3725 "y.tab.c"
    break;

  case 156: /* in_predicate: column IN '(' value_items ')'  */
#line 1326 "sql.y"
        {
            InNode *in_node = instance(InNode);
            in_node->column = (yyvsp[-4].column_node);
            in_node->value_list = (yyvsp[-1].list);
            (yyval.in_node) = in_node;
        }
#line 3736 "y.tab.c"
    break;

  case 157: /* limit_clause: %empty  */
#line 1335 "sql.y"
        {
            (yyval.limit_clause_node) = NULL;
        }
#line 3744 "y.tab.c"
    break;

  case 158: /* limit_clause: LIMIT INTVALUE  */
#line 1339 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = 0;
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3755 "y.tab.c"
    break;

  case 159: /* limit_clause: LIMIT INTVALUE ',' INTVALUE  */
#line 1346 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->offset = (yyvsp[-2].intVal);
            node->rows = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3766 "y.tab.c"
    break;

  case 160: /* limit_clause: LIMIT INTVALUE OFFSET INTVALUE  */
#line 1353 "sql.y"
        {
            LimitClauseNode *node = instance(LimitClauseNode);
            node->rows = (yyvsp[-2].intVal);
            node->offset = (yyvsp[0].intVal);
            (yyval.limit_clause_node) = node;
        }
#line 3777 "y.tab.c"
    break;

  case 161: /* compare: EQ  */
#line 1361 "sql.y"
            { (yyval.compare_type) = O_EQ; }
#line 3783 "y.tab.c"
    break;

  case 162: /* compare: NE  */
#line 1362 "sql.y"
            { (yyval.compare_type) = O_NE; }
#line 3789 "y.tab.c"
    break;

  case 163: /* compare: GT  */
#line 1363 "sql.y"
            { (yyval.compare_type) = O_GT; }
#line 3795 "y.tab.c"
    break;

  case 164: /* compare: GE  */
#line 1364 "sql.y"
            { (yyval.compare_type) = O_GE; }
#line 3801 "y.tab.c"
    break;

  case 165: /* compare: LT  */
#line 1365 "sql.y"
            { (yyval.compare_type) = O_LT; }
#line 3807 "y.tab.c"
    break;

  case 166: /* compare: LE  */
#line 1366 "sql.y"
            { (yyval.compare_type) = O_LE; }
#line 3813 "y.tab.c"
    break;

  case 167: /* function: MAX '(' non_all_function_value ')'  */
#line 1370 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MAX;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3824 "y.tab.c"
    break;

  case 168: /* function: MIN '(' non_all_function_value ')'  */
#line 1377 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_MIN;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3835 "y.tab.c"
    break;

  case 169: /* function: COUNT '(' function_value ')'  */
#line 1384 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_COUNT;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3846 "y.tab.c"
    break;

  case 170: /* function: SUM '(' function_value ')'  */
#line 1391 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_SUM;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3857 "y.tab.c"
    break;

  case 171: /* function: AVG '(' function_value ')'  */
#line 1398 "sql.y"
        {
            FunctionNode *function_node = instance(FunctionNode);        
            function_node->type = F_AVG;
            function_node->value = (yyvsp[-1].function_value_node);
            (yyval.function_node) = function_node;
        }
#line 3868 "y.tab.c"
    break;

  case 172: /* function_value: INTVALUE  */
#line 1407 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3879 "y.tab.c"
    break;

  case 173: /* function_value: column  */
#line 1414 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3890 "y.tab.c"
    break;

  case 174: /* function_value: '*'  */
#line 1421 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->value_type = V_ALL;
            (yyval.function_value_node) = node;
        }
#line 3900 "y.tab.c"
    break;

  case 175: /* non_all_function_value: INTVALUE  */
#line 1429 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->i_value = (yyvsp[0].intVal);
            node->value_type = V_INT;
            (yyval.function_value_node) = node;
        }
#line 3911 "y.tab.c"
    break;

  case 176: /* non_all_function_value: column  */
#line 1436 "sql.y"
        {
            FunctionValueNode *node = instance(FunctionValueNode);
            node->column = (yyvsp[0].column_node);
            node->value_type = V_COLUMN;
            (yyval.function_value_node) = node;
        }
#line 3922 "y.tab.c"
    break;


#line 3926 "y.tab.c"

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

#line 1447 "sql.y"


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
