import logging
import os
import json
import pandas as pd
from typing import List

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

with open("../json/nps.json", "r") as f:
    file = json.loads(f.read())

df = pd.DataFrame.from_dict(file, dtype=str)

print(df.dtypes)
