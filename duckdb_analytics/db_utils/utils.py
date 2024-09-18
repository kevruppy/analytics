import json
import logging
import os
import platform
from pathlib import Path
from typing import Dict, List

import duckdb

logger = logging.getLogger(__name__)


def get_environment() -> str:
    """
    Determines current environment (local,prod)
    Local environment uses WSL2 whereas prod uses Linux

    Returns:
        env (str): Current environment
    """
    env = "LOCAL" if platform.release().endswith("WSL2") else "PROD"
    logger.info(f"Executing in environment: {env}")
    return env


def create_db(env: str, db_path: Path = None) -> str | None:
    """
    (Re-)Creates DuckDB (local)
    If not executed locally: None will be returned

    Params:
        env (str): Current environment (local, prod)
        db_path (Path, optional): Path for DuckDB to be (re-)created (default: None -> env var)

    Returns:
        str | None: Path (str) to (re-)created DuckDB (local) | None
    """
    if env == "LOCAL":
        if not db_path:
            if not os.getenv("DB_PATH"):
                logger.error("Env variable `DB_PATH` must be set")
                raise ValueError("Missing required env variable")

        full_db_name = db_path if db_path else Path(os.getenv("DB_PATH"))
        full_db_name_str = str(full_db_name)

        if full_db_name.exists():
            logger.info(f"Database '{full_db_name.name}' already exists - deleting...")
            full_db_name.unlink()
            logger.info(f"Database '{full_db_name.name}' deleted")

        try:
            with duckdb.connect(full_db_name_str):
                logger.info(f"Database '{full_db_name.name}' created")
                return full_db_name_str
        except Exception as e:
            logger.error(f"Failed to create database '{full_db_name.name}': {e}")
            raise
    return None


def get_duckb_conn(env: str, in_memory: bool = False) -> duckdb.DuckDBPyConnection | None:
    """
    Creates connection to DuckDB (local)
    Per default env var is used to retrieve file path to DuckDB
    If not executed locally: None will be returned

    Params:
        env (str): Current environment (local, prod)
        in_memory (bool, optional): If in-memory DuckDB shall be used (default: False -> env var)

    Returns:
        duckdb.DuckDBPyConnection | None: Connection to DuckDB (local) | None
    """
    if env == "LOCAL":
        if not in_memory:
            if not os.getenv("DB_PATH"):
                logger.error("Env variable `DB_PATH` must be set")
                raise ValueError("Missing required env variable")

        return duckdb.connect(":memory:") if in_memory else duckdb.connect(os.getenv("DB_PATH"))

    return None


def get_motherduck_conn(env: str) -> duckdb.DuckDBPyConnection:
    """
    Creates connection to MotherDuck (prod)
    Locally env var `MOTHERDUCK_TOKEN` is a string (path) pointing to file with token
    On prod env var `MOTHERDUCK_TOKEN` equals a token

    Params:
        env (str): Current environment (local, prod)

    Returns:
        duckdb.DuckDBPyConnection: Connection to MotherDuck (prod)
    """
    secret = os.getenv("MOTHERDUCK_TOKEN")
    if not secret:
        logger.error("Env variable `MOTHERDUCK_TOKEN` must be set")
        raise ValueError("Missing required env variable")

    if env == "LOCAL":
        with open(secret, "r", encoding="utf-8") as f:
            token = f.read()
    else:
        token = secret

    connection_string = f"md:?motherduck_token={token}"
    return duckdb.connect(connection_string)


def get_sql_files(env: str, sql_dir: Path) -> List[Path]:
    """
    Retrieves paths of SQL files (scripts) to be executed
    Note:
        If executed locally there is no need to create db using SQL (nor switch db)
        For this reason first script (prefixed with "000") is ignored

    Params:
        env (str): Current environment (local, prod)
        sql_dir (Path): Path under which SQL files can be found

    Returns:
        file_names (List[Path]): List of paths to SQL files
    """
    file_names: List[str] = sorted(sql_dir.glob("*.sql"))

    if env == "LOCAL":
        file_names.pop(0)

    if file_names:
        logger.info("Found SQL files:")
        for file_name in file_names:
            logger.info(f"'{file_name.name}'")
    else:
        raise FileNotFoundError(f"No SQL files found in directory: '{sql_dir}'")

    return file_names


def load_stmt_list(sql_file: Path) -> List[str]:
    """
    Loads statements of SQL file (script) to be found under specified path

    Params:
        sql_dir (Path): Path to SQL file (script)

    Returns:
        statements (list[str]): List of SQL statements
    """
    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            statements: List[str] = f.read().strip().split(";")[:-1]
        return statements
    except FileNotFoundError as e:
        logger.error(f"Could not find SQL file: '{sql_file.name}'")
        raise e


def load_secret_json(secret_file: Path) -> Dict[str, str]:
    """
    Attempts to load JSON containing secret

    Params:
        secret_file (Path): Path to secret file

    Returns:
        secret (dict[str, str]): Dict containing secret
    """
    try:
        with open(secret_file, "r", encoding="utf-8") as f:
            secret = json.load(f)
            return secret
    except FileNotFoundError as e:
        logger.error(f"Secret file not found: '{secret_file.name}'")
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in secret file: '{secret_file.name}'")
        raise e


def load_aws_secret(env: str) -> Dict[str, str]:
    """
    Loads AWS Secret
    Loading approach differs between environments

    Params:
        env(str): Current environment (local, prod)

    Returns:
        dict[str, str]: Loaded AWS secret
    """
    if not os.getenv("AWS_SECRET"):
        logger.error("Env variable `AWS_SECRET` must be set")
        raise ValueError("Missing required env variable")

    # Locally `AWS_SECRET` is a string
    _ = os.getenv("AWS_SECRET")

    return load_secret_json(Path(_)) if env == "LOCAL" else json.loads(_)


def prep_stmt_list(aws_secret: Dict[str, str], stmt_list: List[str]) -> List[str]:
    """
    Replaces placeholders in SQL statements with AWS secret values

    Params:
        aws_secret (dict[str, str]): AWS credentials to allow reads from S3
        stmt_list (list[str]): List of SQL statements

    Returns:
        prepped_stmt_list (list[str]): List of prepped SQL statements
    """
    prepped_stmt_list = []
    for stmt in stmt_list:
        for k, v in aws_secret.items():
            stmt = stmt.replace(k, v)
        prepped_stmt_list.append(stmt)

    return prepped_stmt_list


def execute_stmt_list(conn: duckdb.DuckDBPyConnection, stmt_list: List[str]) -> str | None:
    """
    Iterates over list of SQL statements & executes them in DuckDB/ MotherDuck (local, prod)

    Params:
        conn (duckdb.DuckDBPyConnection): Connection to DuckDB/ MotherDuck (local, prod)
        stmt_list (list[str]): List of SQL statements to be executed

    Returns:
        str | None: 'SUCCESS' or None in case of failure
    """
    for i, stmt in enumerate(stmt_list):
        stmt = stmt.strip()
        stmt_num = i + 1
        if stmt:
            try:
                logger.info(f"Executing statement {stmt_num} of {len(stmt_list)}")
                conn.execute(stmt)
                logger.info(f"Execution of statement {stmt_num} finished")
            except Exception as e:
                conn.close()
                logger.error(f"Execution of statement {stmt_num} failed: {stmt}\nError: {e}")
                raise e
    return "SUCCESS"


def execute_sql_files(
    sql_files: List[Path], aws_secret: Dict[str, str], conn: duckdb.DuckDBPyConnection
) -> str:
    """
    Executes SQL files (scripts) in DuckDB/ MotherDuck

    Params:
        sql_files (List[Path]): SQL scripts to be executed
        aws_secret (dict[str, str]): AWS secret (to allow reads from S3)
        conn (duckdb.DuckDBPyConnection): Connection to DuckDB/ MotherDuck (local, prod)

    Returns:
        str | None: 'SUCCESS' or None in case of failure
    """
    try:
        for sf in sql_files:
            logger.info(f"## Executing SQL script '{sf.name}' ##")
            stmt_list = load_stmt_list(sf)
            prepped_stmt_list = prep_stmt_list(aws_secret, stmt_list)
            _ = execute_stmt_list(conn, prepped_stmt_list)
            logger.info(f"## All SQL statements of script '{sf.name}' executed successfully ##")
    finally:
        conn.close()

    return "SUCCESS"


def cleanup() -> str:
    """
    Removes temp files created during db load process

    Params:
        None

    Returns:
        str: 'SUCCESS'
    """
    tmp_file_pattern = "TMP_DUCK_DB_EXPORT_*"
    logger.info("Removing temp files...")
    tmp_files = Path.home().glob(tmp_file_pattern)
    if tmp_files:  # pylint: disable=using-constant-test
        for tf in tmp_files:
            logger.info(f"Deleting file: '{tf.name}'")
            tf.unlink()
    else:
        logger.info("No temp files found")

    return "SUCCESS"
