import json
import os
from pathlib import Path

from db_utils.db_utils import get_environment
from initialize_db.execute_initialization import (
    execute_sql_statements,
    get_file_names,
    prep_stmt_list,
)


def test_get_file_names():
    """
    Test retrieving names of SQL scripts.
    """
    try:
        # for simplicity assume local execution
        env = "LOCAL"

        sql_dir = Path("/tmp")
        file1 = sql_dir / "script1.sql"
        file2 = sql_dir / "script2.sql"
        file3 = sql_dir / "script3.sql"

        file1.write_text("SELECT 1;")
        file2.write_text("SELECT 2;")
        file3.write_text("SELECT 3;")

        file_names = get_file_names(env, str(sql_dir))

        # first file shall be ignored if executed locally
        assert file_names == [str(file2), str(file3)]
    finally:
        for f in [file1, file2, file3]:
            if os.path.exists(f):
                os.remove(f)


def test_prep_statements():
    """
    Test prepping SQL statements
    """

    secret = {
        "K_VAL": "FAKE_K",
        "S_VAL": "FAKE_S",
        "R_VAL": "FAKE_R",
    }

    try:
        tmp_secret_file = "/tmp/tmp_secret.json"
        with open(tmp_secret_file, "w", encoding="utf-8") as f:
            json.dump(secret, f)

        stmt_list = [
            "CREATE SECRET _S (TYPE S3, KEY_ID 'K_VAL', SECRET 'S_VAL', REGION 'R_VAL');",
            "SELECT 1;",
        ]

        prepped_stmt_list = [
            "CREATE SECRET _S (TYPE S3, KEY_ID 'FAKE_K', SECRET 'FAKE_S', REGION 'FAKE_R');",
            "SELECT 1;",
        ]

        assert prep_stmt_list(tmp_secret_file, stmt_list) == prepped_stmt_list
    finally:
        if os.path.exists(tmp_secret_file):
            os.remove(tmp_secret_file)


def test_execute_sql_statements():
    """
    Test executing SQL statements on DuckDB.
    """
    try:
        env = get_environment()
        sql_dir = Path("/tmp")
        sql_file = sql_dir / "script.sql"

        sql_file.write_text(
            "SELECT 'K_VAL' AS K, 'S_VAL' AS S, 'R_VAL' AS R; SELECT 1;"
        )

        secret = {
            "K_VAL": "FAKE_K",
            "S_VAL": "FAKE_S",
            "R_VAL": "FAKE_R",
        }

        tmp_secret_file = "/tmp/tmp_secret.json"
        with open(tmp_secret_file, "w", encoding="utf-8") as f:
            json.dump(secret, f)

        assert (
            execute_sql_statements(env, [str(sql_file)], tmp_secret_file) == "SUCCESS"
        )
    finally:
        for f in [sql_file, tmp_secret_file]:
            if os.path.exists(f):
                os.remove(f)
