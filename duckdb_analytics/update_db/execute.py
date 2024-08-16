import logging
import os

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_statements(statements_path: str):
    """
    TBD

    Params:
        sql_dir (str): ...

    Returns:
        stmt_list (list[str]): ...
    """
    try:
        with open(statements_path, "r") as f:
            stmt_list = f.read().strip().split(";")[:-1]
        return stmt_list
    except FileNotFoundError as e:
        logging.error(f'Could not find specified SQL file: "{statements_path}"')
        raise e


def execute_statements(db_path, db_name, statements):
    """
    TBD

    Params:
        sql_dir (str): TBD
    """
    with duckdb.connect(db_path) as con:
        cnt_stmt_list = len(statements)
        for i, stmt in enumerate(statements):
            try:
                con.execute(stmt)
            except Exception as e:
                logging.error(f"Execution of statement {i+1} failed")
                raise e

    logging.info("Done")


def main():
    statements_path = (
        "/workspaces/analytics/duckdb_analytics/update_db/sql/001_update_tables.sql"
    )
    db_path = os.getenv("DB_PATH")
    db_name = os.getenv("DB_NAME")
    statements = load_statements(statements_path)
    execute_statements(db_path, db_name, statements)


if __name__ == "__main__":
    main()
