import argparse
import datetime
import logging
import os

import duckdb
import gspread


def main():
    """
    Executes a query in DuckDB & stores results in Google Sheets
    If argument for `--query` is not provided:
        - In-memory DuckDB is used to execute dummy query
        - Result set is written to Google Sheets
        - Spreadsheet is removed
        -> Simplifies testing this function
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Query for data to be written to sheets", required=False)

    args = parser.parse_args()

    if args.query:
        if not os.getenv("DB_PATH"):
            logging.error("Env var `DB_PATH` is not set")
            raise ValueError("Aborted due to missing env var")
        db_path = os.getenv("DB_PATH")
        query = args.query
        dry_run = False
    else:
        db_path = None
        query = "SELECT 1 AS X"
        dry_run = True

    if not os.getenv("GCP_SERVICE_ACCOUNT") or not os.getenv("GOOGLE_MAIL_ADDRESS"):
        logging.error("At least one env var (`GCP_SERVICE_ACCOUNT`,`GOOGLE_MAIL_ADDRESS`) not set")
        raise ValueError("Aborted due to missing env var(s)")

    if dry_run:
        logging.info("Executing dry run...")

    name = f"ADHOC_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    gc = gspread.service_account(os.getenv("GCP_SERVICE_ACCOUNT"))
    gc.create(name)

    try:
        sh = gc.open(name)
        sh.share(os.getenv("GOOGLE_MAIL_ADDRESS"), perm_type="user", role="writer")
        sh.add_worksheet(title=name, rows=1000, cols=50)
        sh.del_worksheet(sh.sheet1)

        ws = sh.worksheet(name)

        with duckdb.connect(":memory:") if dry_run else duckdb.connect(db_path) as conn:
            df = conn.execute(query).df()

        num_rows, num_cols = df.shape

        if not dry_run:
            logging.info(f"Writing {num_rows} row(s) & {num_cols} col(s) to sheet '{name}'")

        ws.update([df.columns.values.tolist()] + df.values.tolist())
        logging.info("DONE")

    except Exception as e:  # pylint: disable=broad-exception-caught
        gc.del_spreadsheet(sh.id)
        logging.error(f"Execution failed! Error: {e}")
    finally:
        if dry_run:
            gc.del_spreadsheet(sh.id)


if __name__ == "__main__":
    main()
