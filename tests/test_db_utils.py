import os

from db_utils.utils import (
    create_db,
    get_duckb_conn,
    get_environment,
    get_motherduck_conn,
)


def test_create_db():
    """
    Test creating a DuckDB database (only executed locally)
    """
    if get_environment() == "LOCAL":
        db = create_db()
        assert db == os.getenv("DB_PATH")
    else:
        assert True


def test_get_duckb_conn():
    """
    Test DuckDB connection (only executed locally)
    NOTE: TEST ASSUMES THAT DUCKDB TO CONNECT TO EXISTS!
    """
    if get_environment() == "LOCAL":
        try:
            conn = get_duckb_conn()
            assert conn.query("SELECT 1").to_df().iloc[0, 0] == 1
        finally:
            if conn:
                conn.close()
    else:
        assert True


def test_get_motherduck_conn():
    """
    Test MotherDuck connection
    """
    try:
        conn = get_motherduck_conn()
        assert conn.query("SELECT 1").to_df().iloc[0, 0] == 1
    finally:
        if conn:
            conn.close()
