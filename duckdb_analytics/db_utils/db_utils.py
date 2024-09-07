import logging
import os
import platform
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def get_environment() -> str:
    """
    Determines current environment (LOCAL,PROD).
    Local development uses WSL2 whereas prod uses Linux

    Returns:
        str: Current environment
    """
    return "LOCAL" if platform.release().endswith("WSL2") else "PROD"


def create_db() -> str | None:
    """
    (Re-)Creates DuckDB
    NOTE: If not executed locally `None` will be returned

    Returns:
        str | None: File path of the created DuckDB | None
    """
    try:
        if get_environment() == "LOCAL":
            db_dir = os.getenv("DB_DIR")
            db_name = os.getenv("DB_NAME")

            db_dir_path = Path(db_dir)
            db_dir_path.mkdir(parents=True, exist_ok=True)
            full_db_name = db_dir_path / f"{db_name}.duckdb"

            if not db_dir or not db_name:
                logger.error("Env variables for db_dir and db_name must be set")
                raise ValueError("Missing required env variables")
            if full_db_name.exists():
                logger.info(f'Database "{db_name}" already exists - deleting...')
                full_db_name.unlink()
                logger.info(f'Database "{db_name}" deleted')

            with duckdb.connect(str(full_db_name)):
                logger.info(f'Database "{db_name}" created')
                return str(full_db_name)

    except Exception as e:
        logger.error(f'Failed to create database "{db_name}": {e}')
        raise
    return None


def get_duckb_conn() -> duckdb.DuckDBPyConnection | None:
    """
    Creates connection to DuckDB (local)
    If DuckDB file does not exist `None` is returned

    Returns:
        duckdb.DuckDBPyConnection | None: Connection to DuckDB (local) | None
    """
    if get_environment() == "LOCAL":
        _ = create_db()
        db_path = os.getenv("DB_PATH")
        if os.path.exists(db_path):
            return duckdb.connect(db_path)
    return None


def get_motherduck_conn() -> duckdb.DuckDBPyConnection:
    """
    Creates connection to MotherDuck (prod)

    Returns:
        duckdb.DuckDBPyConnection: Connection to MotherDuck (prod)
    """
    secret = os.getenv("MOTHERDUCK_SECRET")
    if get_environment() == "LOCAL":
        with open(secret, "r", encoding="utf-8") as f:
            token = f.read()
    else:
        token = secret

    connection_string = f"md:?motherduck_token={token}"
    return duckdb.connect(connection_string)
