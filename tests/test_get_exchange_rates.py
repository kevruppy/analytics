import json
from datetime import datetime
from pathlib import Path

import pytest
import requests
from initialize_db.generate_sample_data.get_exchange_rates import (
    fetch_exchange_rate,
    generate_dates,
    write_results_to_json,
)


def test_generate_dates():
    """
    Test for function generate_dates
    """
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    dates = generate_dates(start_date, end_date)
    assert len(dates) == 12
    assert dates[0] == "2023-01-31"
    assert dates[-1] == "2023-12-31"

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 31)
    dates = generate_dates(start_date, end_date)
    assert len(dates) == 1
    assert dates[0] == "2023-01-31"


def test_fetch_exchange_rate_success():
    """
    Test to check function fetch_exchange_rate in case of success
    """
    date = "2023-01-01"
    result = fetch_exchange_rate(date)
    assert result is not None
    assert "rates" in result
    assert "USD" in result["rates"]


def test_fetch_exchange_rate_failure():
    """
    Test to check function fetch_exchange_rate in case of failure
    """
    date = "INVALID_DATE"
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_exchange_rate(date)


def test_write_results_to_json():
    """
    Test to check function write_results_to_json
    """
    try:
        tmp_file = Path("/tmp/test_results.json")
        results = [{"date": "2023-01-01", "rates": {"USD": 1.05}}]
        write_results_to_json(results, tmp_file)

        assert tmp_file.exists()
        with open(tmp_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            assert data == results
    finally:
        if tmp_file.exists():
            tmp_file.unlink()
