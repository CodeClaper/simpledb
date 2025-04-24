/********************************* Statement Module ************************************
 * Auth:        JerryZhou
 * Created:     2023/08/31
 * Modify:      2024/11/26
 * Locataion:   src/backend/stmt.c
 * Description: Statement module is the entry to execute statements.
 * Now supported statments:
 * (1)  SELECT
 * (2)  UPDATE
 * (3)  INSERT
 * (4)  DELETE
 * (5)  CREATE TABLE
 * (6)  DEROP TABLE
 * (6)  SHOW TABLES AND SHOW MEMORY
 * (7)  DESCRIBE TABLE
 * (8)  BEGIN TRANSACTION
 * (9)  COMMIT TRANSACTION
 * (10) ALTER TABLE ADD or DROP COLUMN.
 *********************************************************************************
 */
#include <bits/types/struct_timeval.h>
#include <stdbool.h>
#include <stdint.h>
#include <setjmp.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "stmt.h"
#include "list.h"
#include "defs.h"
#include "mmgr.h"
#include "common.h"
#include "asserts.h"
#include "parser.h"
#include "table.h"
#include "create.h"
#include "select.h"
#include "insert.h"
#include "update.h"
#include "delete.h"
#include "drop.h"
#include "desc.h"
#include "show.h"
#include "alter.h"
#include "trans.h"
#include "utils.h"
#include "session.h"
#include "log.h"
#include "free.h"
#include "jsonwriter.h"
#include "instance.h"
#include "xlog.h"
#include "timer.h"

/* Begin tranasction statement. */
static void ExecuteBeginTransactionStmt(Statement *stmt, DBResult *result) {
    Assert(stmt->statement_type == BEGIN_TRANSACTION_STMT);
    BeginTransaction();
    result->success = true;
    result->message = format("Begin new transaction successfully." );
}

/* Commit tranasction statement. */
static void ExecuteCommitTransactionStmt(Statement *stmt, DBResult *result) {
    Assert(stmt->statement_type == COMMIT_TRANSACTION_STMT);
    CommitTransaction();
    result->success = true;
    result->message = format("Commit the transaction successfully.");
}

/* Rollback tranasction statement. */
static void ExecuteRoolbackTransactionStmt(Statement *stmt, DBResult *result) {
    Assert(stmt->statement_type == ROLLBACK_TRANSACTION_STMT);
    RollbackTransaction();
    result->success = true;
    result->message = format("Rollback and commit the transaction successfully.");
}

/* Create table Statement. */
static void ExecuteCreateTableStmt(Statement *stmt, DBResult *result) {
    Assert(stmt->statement_type == CREATE_TABLE_STMT);
    AutoBeginTransaction();
    exec_create_table_statement(stmt->create_table_node, result);
}

/* Drop table statement. */
static void ExecuteDropTableStmt(Statement *stmt, DBResult *result) {
    Assert(stmt->statement_type == DROP_TABLE_STMT);
    char *table_name = stmt->drop_table_node->table_name;
    AutoBeginTransaction();
    exec_drop_table_statement(table_name, result);
}

/* Alter table statement. */
static void ExecuteAlterTableStmt(Statement *stmt, DBResult *result) {
    Assert(stmt->statement_type == ALTER_TABLE_STMT);
    AutoBeginTransaction();
    exec_alter_statement(stmt->alter_table_node, result);
}

/*Insert Statment*/
static void ExecuteInsertStmt(Statement *stmt, DBResult *result) {
    Assert(stmt->statement_type == INSERT_STMT);
    AutoBeginTransaction();
    List *list = exec_insert_statement(stmt->insert_node);
    if (list) {
        result->success = true;
        result->rows = len_list(list);
        result->message = format("Insert %d rows data to table '%s' successfully.",
                                 result->rows, stmt->insert_node->table_name);
        db_log(SUCCESS, "Insert %d row data to table '%s' successfully.",
               result->rows, stmt->insert_node->table_name);
        free_list_deep(list);
    }
}

/*Select Statement*/
static void ExecuteSelectStmt(Statement *statement, DBResult *result) {
    Assert(statement->statement_type == SELECT_STMT);
    AutoBeginTransaction();
    exec_select_statement(statement->select_node, result); 
}

/*Update statemetn*/
static void ExecuteUpdateStmt(Statement *statement, DBResult *result) {
    Assert(statement->statement_type == UPDATE_STMT);
    AutoBeginTransaction();
    exec_update_statment(statement->update_node, result);
}

/*Delete Statement*/
static void ExecuteDeleteStmt(Statement *statement, DBResult *result) {
    Assert(statement->statement_type == DELETE_STMT);
    AutoBeginTransaction();
    exec_delete_statement(statement->delete_node, result);
}

/*Describe Statement*/
static void ExecuteDescribeStmt(Statement *statement, DBResult *result) {
    Assert(statement->statement_type == DESCRIBE_STMT);
    AutoBeginTransaction();
    List *list = exec_describe_statement(statement->describe_node);
    if (list) {
        /* Success resule. */
        result->success = true;
        result->data = list;
        result->message = dstrdup("Describe executed successfully.");
        db_log(SUCCESS, "Describe statement is executed successfully.");
    }
}

/*Show tables Statment*/
static void ExecuteShowStmt(Statement *statement, DBResult *result) {
    Assert(statement->statement_type == SHOW_STMT);
    AutoBeginTransaction();
    exec_show_statement(statement->show_node, result);
}

/* Execute statment. */
static void ExecuteStatement(Statement *statement, DBResult *result) {
    /* Execute statment */
    if (statement) {
        result->stmt_type = statement->statement_type;
        switch(statement->statement_type) {
            case BEGIN_TRANSACTION_STMT:
                ExecuteBeginTransactionStmt(statement, result);
                break;
            case COMMIT_TRANSACTION_STMT:
                ExecuteCommitTransactionStmt(statement, result);
                break;
            case ROLLBACK_TRANSACTION_STMT:
                ExecuteRoolbackTransactionStmt(statement, result);
                break;
            case CREATE_TABLE_STMT:
                ExecuteCreateTableStmt(statement, result);
                break;
            case INSERT_STMT:
                ExecuteInsertStmt(statement, result); 
                break; 
            case SELECT_STMT:
                ExecuteSelectStmt(statement, result); 
                break; 
            case UPDATE_STMT:
                ExecuteUpdateStmt(statement, result); 
                break; 
            case DELETE_STMT:
                ExecuteDeleteStmt(statement, result); 
                break; 
            case DESCRIBE_STMT:
                ExecuteDescribeStmt(statement, result);
                break;
            case SHOW_STMT:
                ExecuteShowStmt(statement, result);
                break;
            case DROP_TABLE_STMT:
                ExecuteDropTableStmt(statement, result);
                break;
            case ALTER_TABLE_STMT:
                ExecuteAlterTableStmt(statement, result);
                break;
            default:
                UNEXPECTED_VALUE(statement->statement_type);
                break;
        }
    } 

    /* Calulate duration. */
    gettimeofday(&result->end_time, NULL);
    result->duration = time_span(result->end_time, result->start_time);
    db_log(INFO, "Duration: %lfs", result->duration);
}


/* Execute SQL
 * Now supported statments:
 * (1)  SELECT
 * (2)  UPDATE
 * (3)  INSERT
 * (4)  DELETE
 * (5)  CREATE TABLE
 * (6)  DEROP TABLE
 * (6)  SHOW TABLES AND SHOW MEMORY
 * (7)  DESCRIBE TABLE
 * (8)  BEGIN TRANSACTION
 * (9)  COMMIT TRANSACTION
 * (10) ALTER TABLE ADD or DROP COLUMN.
 * */
void Execute(char *sql) {

    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);

    List *result_list = create_list(NODE_DB_RESULT);
    List *statements = NULL;

    /* Check empty sql. */
    if (!is_empty(sql)) {
        /* Catch Error. */
        if (setjmp(errEnv) == 0) {
            statements = parse(sql);
            /* Execute each statement. */
            ListCell *lc;
            foreach (lc, statements) {
                DBResult *result = new_db_result();
                append_list(result_list, result);
                Statement *statement = lfirst(lc);
                ExecuteStatement(statement, result);
            }
        } else {
            /* Catch routine. */
            /* If the set is empty, which means sql syntax error, 
             * put an error result to the set. */
            if (list_empty(result_list)) {
                DBResult *errResult = new_db_result();
                /* For error catch, result is false. */
                errResult->success = false;
                append_list(result_list, errResult);
            }

            /* If last result is error, it lack duration, make up. */
            DBResult *last_result = lfirst(last_cell(result_list));
            if (last_result->success == false) {
                /* Calulate duration. */
                gettimeofday(&last_result->end_time, NULL);
                last_result->duration = time_span(last_result->end_time, last_result->start_time);
                db_log(INFO, "Duration: %lfs", last_result->duration);
            }
        }
    } else {
        DBResult *emptyResult = new_db_result();
        emptyResult->success = false;
        emptyResult->message = format("Input nothing");
        append_list(result_list, emptyResult);
    }

    /* Json result list. */
    json_list(result_list);

    /* Free statements. */
    free_list_deep(statements);

    /* Commit transction manually. */
    AutoCommitTransaction();

    /* Calulate whole duration. */
    gettimeofday(&end_time, NULL);
    db_log(INFO, "Whole duration: %lfs", time_span(end_time, start_time));
}
