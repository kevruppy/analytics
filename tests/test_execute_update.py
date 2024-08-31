import os
from pathlib import Path

import duckdb
from update_db.execute_update import execute_statements, load_statements


def test_load_statements():
    """Test loading SQL statements"""
    try:
        sql_file = Path("/tmp") / "test.sql"
        sql_file.write_text("SELECT 1; SELECT 2;")

        statements = load_statements(str(sql_file))
        # NOTE: LEADING WHITESPACE IS NOT REMOVED
        assert statements == ["SELECT 1", " SELECT 2"]
    finally:
        if os.path.exists(sql_file):
            os.remove(sql_file)


def test_execute_statements():
    """Test executing statements"""
    sql_file = Path("/tmp") / "test.sql"
    sql_file.write_text("CREATE TABLE TEST (ID INT);")

    db_path = "/tmp/test_db"

    with duckdb.connect(db_path):
        pass

    statements = ["SELECT 1", "SELECT 2"]

    try:
        execute_statements(db_path, statements)
        assert True
    finally:
        for f in [sql_file, db_path]:
            if os.path.exists(db_path):
                os.remove(f)
