import os
import logging
from pathlib import Path
from typing import List, Optional
import duckdb


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_db(db_dir: str, db_name: str) -> str:
    '''
    (Re-)Creates DuckDB

    params:
        db_dir (str): Directory under which the DB shall be created
        db_name (str): Name of the DB to be created
    
    Returns:
        db_file_path (str): File path of the created DuckDB
    '''
    db_dir_path = Path(db_dir)
    db_dir_path.mkdir(parents=True, exist_ok=True)
    full_db_name = db_dir_path / f'{db_name}.duckdb'

    try:
        if full_db_name.exists():
            logging.info(f'Database "{db_name}" already exists - deleting...')
            full_db_name.unlink()
            logging.info(f'Database "{db_name}" deleted')

        with duckdb.connect(str(full_db_name)):
            logging.info(f'Creating Database "{db_name}"...')
            logging.info(f'Database "{db_name}" created')

    except Exception as e:
        logging.error(f'Failed to create database "{db_name}": {e}')
        raise

    return str(full_db_name)


def get_file_names(sql_dir: str) -> List[str]:
    '''
    Retrieve names of SQL scripts to be executed

    Params:
        sql_dir (str): Directory in which SQL scripts can be found
    
    Returns:
        file_names (list[str]): List of absolute file names 
    '''
    sql_dir_path = Path(sql_dir)
    file_names = sorted(sql_dir_path.glob('*.sql'))

    if file_names:
        logging.info('Found SQL files:')
        for file_name in file_names:
            logging.info(file_name.name)

    return [str(f) for f in file_names]


def execute_sql_statements(db_path: str, sql_files: List[str]):
    '''
    Execute SQL statements on the provided DuckDB

    Params:
        db_path (str): Path to the DuckDB
        sql_files (list[str]): List of files containing SQL to be executed
    '''
    logging.info(f'Connecting to DuckDB "{db_path}"...')
    with duckdb.connect(db_path) as con:
        for file in sql_files:
            logging.info(f'Executing SQL script: "{file.split('/')[-1]}"')
            with open(file, 'r', encoding='utf-8') as f:
                stmt_list = f.read().strip().split(';')
                for stmt in stmt_list:
                    if stmt.strip():
                        stmt = stmt.strip()
                        try:
                            con.execute(stmt)
                        except Exception as e:
                            raise e
        logging.info('All SQL statements executed')


def main():
    db_dir = os.getenv('DB_DIR')
    db_name = os.getenv('DB_NAME')
    sql_dir = os.getenv('SQL_DIR')

    try:
        db = create_db(db_dir=db_dir, db_name=db_name)
        sql_files = get_file_names(sql_dir=sql_dir)
        execute_sql_statements(db_path=db, sql_files=sql_files)
    except Exception as e:
        logging.error(f'An error occurred during database setup: {e}')
        raise


if __name__ == '__main__':
    main()
