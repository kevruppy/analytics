import os
from pathlib import Path

import duckdb
from initialize_db.execute_initialization import (
    create_db,
    execute_sql_statements,
    get_file_names,
)


def test_create_db():
    """
    Test creating a DuckDB database.
    """
    db_dir = "/tmp"
    db_name = "test_db"
    db_file = f"{db_dir}/{db_name}.duckdb"

    create_db(db_dir, db_name)

    try:
        with duckdb.connect(db_file) as con:
            assert con.execute("SELECT 1").fetchone()[0] == 1
    finally:

        if os.path.exists(db_file):
            os.remove(db_file)


def test_get_file_names():
    """
    Test retrieving names of SQL scripts.
    """
    sql_dir = Path("/tmp")

    try:
        file1 = sql_dir / "script1.sql"
        file2 = sql_dir / "script2.sql"

        file1.write_text("SELECT 1;")
        file2.write_text("SELECT 2;")

        file_names = get_file_names(str(sql_dir))

        assert file_names == [str(file1), str(file2)]
    finally:
        for f in [file1, file2]:
            if os.path.exists(f):
                os.remove(f)


def test_execute_sql_statements():
    """
    Test executing SQL statements on DuckDB.
    """
    db_dir = "/tmp"
    db_name = "test_db"
    db_file = f"{db_dir}/{db_name}"

    sql_dir = Path("/tmp")
    sql_file = sql_dir / "script.sql"

    sql_file.write_text("CREATE TABLE TEST (ID INT); INSERT INTO TEST VALUES (1);")

    execute_sql_statements(db_file, [str(sql_file)])

    try:
        with duckdb.connect(db_file) as con:
            assert con.execute("SELECT * FROM TEST").fetchone()[0] == 1
    finally:
        for f in [db_file, sql_file]:
            if os.path.exists(f):
                os.remove(f)
