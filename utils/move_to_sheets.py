import argparse
import datetime
import logging
import os

import duckdb
import gspread


def main():
    """
    Executes a query in DuckDB and stores results in Google Sheets
    """

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query", help="Query for data to be written to sheets", required=True
    )
    args = parser.parse_args()

    try:
        name = f"ADHOC_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

        gc = gspread.service_account(os.getenv("GCP_SERVICE_ACCOUNT"))

        gc.create(name)
        sh = gc.open(name)
        sh.share(os.getenv("GOOGLE_MAIL_ADDRESS"), perm_type="user", role="writer")
        sh.add_worksheet(title=name, rows=1000, cols=50)
        sh.del_worksheet(sh.sheet1)

        ws = sh.worksheet(name)

        with duckdb.connect(os.getenv("DB_PATH")) as con:
            res = con.execute(args.query)
            df = res.df()

        num_rows, num_cols = df.shape
        logging.info(
            f"Writing {num_rows} row(s) and {num_cols} col(s) to sheet '{name}'"
        )

        ws.update([df.columns.values.tolist()] + df.values.tolist())
        logging.info("SUCCESS")
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(f"Execution failed! Error: {e}")


if __name__ == "__main__":
    main()
