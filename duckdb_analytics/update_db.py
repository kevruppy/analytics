import logging
import os
from typing import List

import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


nps = [{"mw-136592","2023-12-31",11,"surveyape"},
{"info-183383","2023-12-31",8,"surveyape"},
{"glass-183611","2023-12-31",10,"surveyape"},
{"clh-183307","2023-12-31",10,"surveyape"}]


try:
    with duckdb.connect(db):

def main():
    db_dir = os.getenv('DB_DIR')
    db_name = os.getenv('DB_NAME')