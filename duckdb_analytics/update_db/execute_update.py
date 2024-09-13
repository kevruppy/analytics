import logging
import os
from pathlib import Path

from db_utils.utils import (
    execute_stmt_list,
    get_duckb_conn,
    get_environment,
    get_motherduck_conn,
    load_secret_json,
    load_stmt_list,
    prep_stmt_list,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Main function to execute script"""

    env = get_environment()
    logging.info(f"Executing in environment: {env}")

    sql_file_dir = Path(__file__).parent / "sql"
    # Dir contains only one SQL file
    sql_file = sql_file_dir / sorted(os.listdir(sql_file_dir))[0]

    if not os.getenv("AWS_SECRET"):
        logging.error("Env variable `AWS_SECRET` must be set")
        raise ValueError("Missing required env variable")

    _ = os.getenv("AWS_SECRET")
    aws_secret = load_secret_json(Path(_)) if env == "LOCAL" else _

    stmt_list = load_stmt_list(sql_file)
    prepped_stmt_list = prep_stmt_list(aws_secret, stmt_list)

    conn = get_duckb_conn(env) if env == "LOCAL" else get_motherduck_conn(env)

    logging.info(f"### Executing SQL script '{sql_file.name}' ###")

    try:
        _ = execute_stmt_list(conn, prepped_stmt_list)
        logging.info(
            f"### All SQL statements of script '{sql_file.name}' executed successfully ###"
        )
    finally:
        conn.close()

    logging.info("SUCCESS")


if __name__ == "__main__":
    main()
