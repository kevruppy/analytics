import json
import logging
import os
from pathlib import Path

from db_utils.utils import (
    create_db,
    execute_stmt_list,
    get_duckb_conn,
    get_environment,
    get_motherduck_conn,
    get_sql_files,
    load_secret_json,
    load_stmt_list,
    prep_stmt_list,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    """Main function to execute script"""

    env = get_environment()
    logging.info(f"Executing in environment: {env}")

    # pylint: disable=duplicate-code
    if not os.getenv("AWS_SECRET"):
        logging.error("Env variable `AWS_SECRET` must be set")
        raise ValueError("Missing required env variable")

    # Locally `AWS_SECRET` is a path (as string)
    _ = os.getenv("AWS_SECRET")
    aws_secret = load_secret_json(Path(_)) if env == "LOCAL" else json.loads(_)
    # pylint: enable=duplicate-code

    try:
        sql_dir = Path(__file__).parent / "sql"
        sql_files = get_sql_files(env, sql_dir)
        if not sql_files:
            raise FileNotFoundError(f"No SQL files found in directory: '{sql_dir}'")

        # If not executed locally `create_db()` will return None
        _ = create_db(env)
        conn = get_duckb_conn(env) if env == "LOCAL" else get_motherduck_conn(env)

        try:
            for sf in sql_files:
                logging.info(f"### Executing SQL script '{sf.name}' ###")
                stmt_list = load_stmt_list(sf)
                # pylint: disable=duplicate-code
                prepped_stmt_list = prep_stmt_list(aws_secret, stmt_list)
                _ = execute_stmt_list(conn, prepped_stmt_list)
                logging.info(
                    f"### All SQL statements of script '{sf.name}' executed successfully ###"
                )
        finally:
            conn.close()
            # pylint: enable=duplicate-code

        logging.info("Removing temp files...")
        temp_files = Path.home().glob("TMP_DUCK_DB_EXPORT_*")
        if temp_files:  # pylint: disable=using-constant-test
            for tf in temp_files:
                logging.info(f"Deleting file: '{tf.name}'")
                tf.unlink()
        else:
            logging.info("No temp files found")
        logging.info("SUCCESS: Database setup finished")
    except Exception as e:
        logging.error(f"An error occurred during database setup: {e}")
        raise


if __name__ == "__main__":
    main()
