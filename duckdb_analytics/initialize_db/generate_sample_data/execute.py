# You are an expert Data Engineer and Pythonista that writes nothing but high quality Python code that follows industry standards and best practices and is all the way pythonic. You received this Python script from a Junior. Optimize it. Here is the script:

import calendar
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import jsonschema
import requests
from jsonschema import validate

API_URL = "https://api.frankfurter.app"
SCHEMA_PATH = "/workspaces/analytics/duckdb_analytics/initialize_db/generate_sample_data/schema.json"
OUTPUT_FILE = "/workspaces/analytics/duckdb_analytics/initialize_db/sample_data/exchange_rates.json"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def generate_dates(start_date: datetime, end_date: datetime) -> List[str]:
    """
    Generates a list of month-end dates between start_date and end_date.

    Args:
        start_date (datetime): Starting date of range.
        end_date (datetime): Ending date of range.

    Returns:
        List[str]: List of strings representing the month-end dates ('YYYY-MM-DD').
    """
    dates = []
    current_date = start_date.replace(day=1)

    while current_date <= end_date:
        month_end = current_date + timedelta(
            days=calendar.monthrange(current_date.year, current_date.month)[1] - 1
        )
        dates.append(month_end.strftime("%Y-%m-%d"))
        current_date = month_end + timedelta(days=1)

    return dates


def fetch_exchange_rate(date: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Fetches exchange rates from Frankfurter API for a specific date using retries.

    Args:
        date (str): Date for which to fetch exchange rate ('YYYY-MM-DD').
        max_retries (int, optional): Maximum number of retries in case of request failure, defaults to 3.

    Returns:
        Optional[Dict[str, Any]]: Dict containing exchange rate if request is successful, None otherwise.
    """
    url = f"{API_URL}/{date}"
    params = {"amount": 1, "from": "EUR", "to": "USD"}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.debug(f"Attempt {attempt + 1} failed for {date}: {e}")
            if attempt == max_retries - 1:
                logging.error(
                    f"Error fetching exchange rate for {date} after {max_retries} attempts: {e}"
                )

    return None


def write_results_to_json(results: List[Dict[str, Any]], filename: str) -> None:
    """
    Writes the given results to a JSON file.

    Args:
        results (List[Dict[str, Any]]): List of exchange rates.
        filename (str): Name of JSON file.
    """
    try:
        with open(filename, "w") as f:
            json.dump(results, f, indent=2)
            logging.info(f"Results written to {filename}")
    except IOError as e:
        logging.error(f"Failed to write results to {filename}: {e}")


def main() -> None:
    """
    Main function to generate dates & fetch exchange rate for each date.
    """
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31)
    dates = generate_dates(start_date, end_date)

    results = [result for date in dates if (result := fetch_exchange_rate(date))]

    logging.info(f"Fetched {len(results)} exchange rates.")

    with open(SCHEMA_PATH, "r") as f:
        schema = json.load(f)

    try:
        validate(instance=results, schema=schema)
        logging.info("Validation successful: Data is valid.")
    except jsonschema.exceptions.ValidationError as e:
        logging.error("Validation error:", e)

    write_results_to_json(results, OUTPUT_FILE)


if __name__ == "__main__":
    main()
