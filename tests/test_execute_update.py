import os
import json
from pathlib import Path

import duckdb
from update_db.execute_update import (
    load_statements,
    prep_statements,
    execute_statements,
)


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


def test_prep_statements():
    """Test prepping SQL statements"""

    secret = {
        "K_VAL": "FAKE_K",
        "S_VAL": "FAKE_S",
        "R__VAL": "FAKE_R",
    }

    try:
        tmp_secret_file = "/tmp/tmp_secret.json"
        with open(tmp_secret_file, "w", encoding="utf-8") as f:
            json.dump(secret, f)

        stmt_list = [
            "CREATE SECRET _S (TYPE S3, KEY_ID 'K_VAL', SECRET 'S_VAL', REGION 'R__VAL');",
            "SELECT 1;",
        ]

        prepped_stmt_list = [
            "CREATE SECRET _S (TYPE S3, KEY_ID 'FAKE_K', SECRET 'FAKE_S', REGION 'FAKE_R');",
            "SELECT 1;",
        ]

        assert prep_statements(tmp_secret_file, stmt_list) == prepped_stmt_list
    finally:
        if os.path.exists(tmp_secret_file):
            os.remove(tmp_secret_file)


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
