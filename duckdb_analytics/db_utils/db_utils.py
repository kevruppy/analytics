import os
import platform

import duckdb


def get_environment() -> str:
    """
    Determines current environment (LOCAL,PROD).
    Local development uses WSL whereas prod uses Linux

    Returns:
        str: Current environment
    """

    return "LOCAL" if platform.release().endswith("WSL2") else "PROD"


def get_duckb_conn() -> duckdb.DuckDBPyConnection | None:
    """
    Creates connection to DuckDB (local)
    If DuckDB file does not exist `None` is returned

    Returns:
        duckdb.DuckDBPyConnection | None: Connection to DuckDB (local) | None
    """
    if get_environment() == "LOCAL":
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
