from db_utils.db_utils import get_duckb_conn, get_motherduck_conn


def test_get_duckb_conn():
    """
    Test DuckDB connection
    Note: Test assumes that DuckDB to connect to exists!
    """
    with get_duckb_conn() as conn:
        assert conn.query("SELECT 1").to_df().iloc[0, 0] == 1


def test_get_motherduck_conn():
    """
    Test MotherDuck connection
    """
    with get_motherduck_conn() as conn:
        assert conn.query("SELECT 1").to_df().iloc[0, 0] == 1
