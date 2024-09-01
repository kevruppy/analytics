import logging
import os
from typing import List
import json

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_statements(statements_path: str) -> List[str]:
    """
    Loads a SQL script to be found under specified path

    Params:
        sql_dir (str): Path where SQL script is located

    Returns:
        statements (list[str]): List of SQL statements
    """
    try:
        with open(statements_path, "r", encoding="utf-8") as f:
            statements: List[str] = f.read().strip().split(";")[:-1]
        return statements
    except FileNotFoundError as e:
        logging.error(f'Could not find specified SQL file: "{statements_path}"')
        raise e


def prep_statements(secret_file: str, stmt_list: List[str]) -> List[str]:
    """
    Replaces placeholders in SQL script with secret values

    Params:
        secret_file (str): Path to file containing secret
        stmt_list (list[str]): List of SQL statements

    Returns:
        prepped_stmt_list (list[str]): List of prepped SQL statements
    """
    try:
        with open(secret_file, "r", encoding="utf-8") as f:
            secret = json.load(f)
    except FileNotFoundError as e:
        logging.error(f"Secret file not found: {secret_file}")
        raise e
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON format in secret file: {secret_file}")
        raise e

    prepped_stmt_list = []
    for stmt in stmt_list:
        for k, v in secret.items():
            stmt = stmt.replace(k, v)
        prepped_stmt_list.append(stmt)

    return prepped_stmt_list


def execute_statements(db_path: str, stmt_list: List[str]):
    """
    Executes SQL statements in DuckDB

    Params:
        db_path (str): Path to DuckDB
        stmt_list (list[str]): List of SQL statements
    """
    with duckdb.connect(db_path) as con:
        if not stmt_list:
            logging.error("No SQL statements provided")
            raise ValueError("Please provide a list with SQL statements")
        for i, stmt in enumerate(stmt_list, start=1):
            try:
                logging.info(f"Executing statement {i} of {len(stmt_list)}")
                con.execute(stmt)
                logging.info(f"Execution of statement {i} finished")
            except Exception as e:
                logging.error(f"Execution of statement {i} failed")
                raise e

    logging.info("All statements executed")


def main():
    """Main function to execute script"""
    statements_path = os.getenv("SQL_UPD_DIR")
    db_path = os.getenv("DB_PATH")
    aws_secret = os.getenv("AWS_SECRET")

    if not statements_path or not db_path or not aws_secret:
        logging.error(
            "Environment variables for statements_path, db_path & aws_secret must be set"
        )
        raise ValueError("Missing required environment variables")

    statements = load_statements(statements_path)
    prepped_statements = prep_statements(aws_secret, statements)
    execute_statements(db_path, prepped_statements)


if __name__ == "__main__":
    main()
