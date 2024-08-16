import logging
import os
import json
import pandas as pd
from typing import List

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

print(os.getcwd())
"""
with open("/sample_data/nps.json", "r") as f:
    file = json.load(f)

df = pd.DataFrame.from_dict(file, dtype=str)

df.columns = [col.upper() for col in df.columns]

with duckdb.connect(os.getenv("DB_PATH")) as con:
    con.execute(
        "INSERT INTO ANALYTICS.RAW_DATA.NET_PROMOTOR_SCORES (TRANSACTION_ID, RATING_DATE, RATING, TOOL) SELECT COLUMNS(*) FROM df"
    )
"""
