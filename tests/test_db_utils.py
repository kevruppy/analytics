import json
from pathlib import Path

import duckdb
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


def test_get_environment():
    """
    Test getting environment (dummy)
    """
    assert True


def test_create_db():
    """
    Test creating a DuckDB database
    """
    # For simplicity assume local execution
    env = "LOCAL"
    db_path = Path("/tmp/test.duckdb")

    try:
        db = create_db(env, db_path)
        assert db == str(db_path)
    finally:
        if db_path.exists():
            db_path.unlink()


def test_get_duckb_conn():
    """
    Test getting DuckDB connection
    """
    # For simplicity assume local execution
    env = "LOCAL"
    db_path = Path("/tmp/test.duckdb")

    try:
        db = create_db(env, db_path)
        conn = get_duckb_conn(env, db)
        assert conn.query("SELECT 1").to_df().iloc[0, 0] == 1
    finally:
        if conn:
            conn.close()
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_get_motherduck_conn():
    """
    Test getting MotherDuck connection
    """
    env = get_environment()

    try:
        conn = get_motherduck_conn(env)
        assert conn.query("SELECT 1").to_df().iloc[0, 0] == 1
    finally:
        if conn:
            conn.close()


def test_get_sql_files():
    """
    Test retrieving names of SQL scripts
    """
    # For simplicity assume local execution
    env = "LOCAL"

    try:
        tmp_dir = Path("/tmp")
        file1 = tmp_dir / "script1.sql"
        file2 = tmp_dir / "script2.sql"
        file3 = tmp_dir / "script3.sql"

        file1.write_text("SELECT 1;")
        file2.write_text("SELECT 2;")
        file3.write_text("SELECT 3;")

        file_names = get_sql_files(env, tmp_dir)

        # First file shall be ignored if executed locally
        assert [str(f) for f in file_names] == [str(file2), str(file3)]
    finally:
        for f in [file1, file2, file3]:
            if f.exists():
                f.unlink()


def test_load_stmt_list():
    """
    Test loading SQL statements
    """
    try:
        tmp_file = Path("/tmp/script.sql")
        tmp_file.write_text(" SELECT 1; SELECT 2;", encoding="utf-8")

        # NOTE: LEADING WHITESPACE IS NOT REMOVED!
        assert load_stmt_list(tmp_file) == ["SELECT 1", " SELECT 2"]
    finally:
        if tmp_file.exists():
            tmp_file.unlink()


def test_load_secret_json():
    """
    Test loading secret JSON
    """
    try:
        secret_file = Path("/tmp/tmp_secret.json")

        secret = {"KEY": "VALUE"}

        with open(secret_file, "w", encoding="utf-8") as f:
            json.dump(secret, f)

        assert load_secret_json(secret_file) == secret
    finally:
        if secret_file.exists():
            secret_file.unlink()


def test_prep_stmt_list():
    """
    Test prepping SQL statements
    """
    secret = {
        "K_VAL": "KKK",
        "S_VAL": "SSS",
        "R_VAL": "RRR",
    }

    stmt_list = [
        "CREATE SECRET _X (TYPE S3, KEY_ID 'K_VAL', SECRET 'S_VAL', REGION 'R_VAL');",
        "SELECT 1;",
    ]

    prepped_stmt_list = [
        "CREATE SECRET _X (TYPE S3, KEY_ID 'KKK', SECRET 'SSS', REGION 'RRR');",
        "SELECT 1;",
    ]

    assert prep_stmt_list(secret, stmt_list) == prepped_stmt_list


def test_execute_stmt_list():
    """
    Test executing statements (uses DuckDB, not MotherDuck)
    """
    try:
        db_path_str = "/tmp/test_db.duckdb"

        conn = duckdb.connect(db_path_str)

        statements = ["SELECT 1;", "SELECT 2;"]

        result = execute_stmt_list(conn, statements)
        assert result == "SUCCESS"
    finally:
        if Path(db_path_str).exists():
            Path(db_path_str).unlink()
