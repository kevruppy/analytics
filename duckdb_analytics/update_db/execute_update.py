import logging
from pathlib import Path

from db_utils.utils import (
    execute_sql_files,
    get_duckb_conn,
    get_environment,
    get_motherduck_conn,
    get_sql_files,
    load_aws_secret,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    """Main function to execute script (update db)"""

    env = get_environment()
    aws_secret = load_aws_secret(env)
    conn = get_duckb_conn(env) if env == "LOCAL" else get_motherduck_conn(env)
    sql_dir = Path(__file__).parent / "sql"

    try:
        sql_files = get_sql_files(env, sql_dir)
        _res_exec_sql = execute_sql_files(sql_files, aws_secret, conn)
        logging.info(f"### Update db: {_res_exec_sql} ###")
    except Exception as e:
        logging.error(f"ERROR: {e}")
        raise


if __name__ == "__main__":
    main()
