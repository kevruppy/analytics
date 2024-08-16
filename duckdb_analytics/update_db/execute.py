import logging
import os
from typing import List

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
        with open(statements_path, "r") as f:
            statements: List[str] = f.read().strip().split(";")[:-1]
        return statements
    except FileNotFoundError as e:
        logging.error(f'Could not find specified SQL file: "{statements_path}"')
        raise e


def execute_statements(db_path: str, statements: List[str]):
    """
    Executes SQL statements in DuckDB

    Params:
        db_path (str): Path to DuckDB
        statements (list[str]): List of SQL statements
    """
    with duckdb.connect(db_path) as con:
        if not statements:
            logging.error("No SQL statements provided")
            raise ValueError("Please provide a list with SQL statements")
        for i, stmt in enumerate(statements, start=1):
            try:
                logging.info(f"Executing stmt {i} of {len(statements)}")
                con.execute(stmt)
                logging.info(f"Execution of statement {i} finished")
            except Exception as e:
                logging.error(f"Execution of statement {i} failed")
                raise e

    logging.info("All statements executed")


def main():
    statements_path = os.getenv("SQL_UPD_DIR")
    db_path = os.getenv("DB_PATH")

    if not statements_path or not db_path:
        logging.error(
            "Environment variables for statements_path and db_path must be set"
        )
        raise ValueError("Missing required environment variables")

    statements = load_statements(statements_path)
    execute_statements(db_path, statements)


if __name__ == "__main__":
    main()
