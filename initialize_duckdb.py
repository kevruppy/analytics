import os
import duckdb

def recreate_duckdb(filename: str):
    """
    Deletes the specified DuckDB database file if it exists and creates a new one.

    Args:
        filename (str): The name of the DuckDB file to create or recreate.
    """
    if os.path.exists(filename):
        os.remove(filename)
        print(f"{filename} has been deleted.")

    with duckdb.connect(filename) as con:
        print(f"{filename} has been created successfully.")
        con.execute("CREATE TABLE ORDERS AS SELECT * FROM READ_JSON_AUTO('./sample_data/provision_rules.json')")

if __name__ == "__main__":
    db_filename = "analytics.duckdb"
    recreate_duckdb(db_filename)
