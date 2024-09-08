import json
import os
from pathlib import Path

import duckdb
from update_db.execute_update import (
    execute_statements,
    load_statements,
    prep_statements,
)


def test_load_statements():
    """
    Test loading SQL statements
    """
    try:
        sql_file = Path("/tmp") / "test.sql"
        sql_file.write_text("SELECT 1; SELECT 2;")

        statements = load_statements(str(sql_file))
        # NOTE: LEADING WHITESPACE IS NOT REMOVED
        assert statements == ["SELECT 1", " SELECT 2"]
    finally:
        if os.path.exists(sql_file):
            os.remove(sql_file)


def test_prep_statements():
    """
    Test prepping SQL statements
    """
    try:
        secret = {
            "K_VAL": "KKK",
            "S_VAL": "SSS",
            "R_VAL": "RRR",
        }

        temp_secret = "/tmp/tmp_secret.json"
        with open(temp_secret, "w", encoding="utf-8") as f:
            json.dump(secret, f)

        stmt_list = [
            "CREATE SECRET _X (TYPE S3, KEY_ID 'K_VAL', SECRET 'S_VAL', REGION 'R_VAL');",
            "SELECT 42;",
        ]

        prepped_stmt_list = [
            "CREATE SECRET _X (TYPE S3, KEY_ID 'KKK', SECRET 'SSS', REGION 'RRR');",
            "SELECT 42;",
        ]

        assert prep_statements(temp_secret, stmt_list) == prepped_stmt_list
    finally:
        if os.path.exists(temp_secret):
            os.remove(temp_secret)


def test_execute_statements():
    """
    Test executing statements
    """
    sql_file = Path("/tmp") / "test.sql"
    sql_file.write_text("CREATE TEMP TABLE TEST (ID INT);")

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
