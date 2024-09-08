import json
import logging
import os
from pathlib import Path
from typing import Dict, List

from db_utils.utils import get_duckb_conn, get_environment, get_motherduck_conn

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_file_names(env: str, sql_dir: str) -> List[str]:
    '''
    Retrieve names of SQL scripts to be executed
    Note:
        If executed locally there is no need to create db via SQL
        For this reason the script prefixed with `000` is ignored

    Params:
        env (str): Environment (local, prod)
        sql_dir (str): Directory in which SQL scripts can be found

    Returns:
        file_names (list[str]): List of absolute file names
    '''
    sql_dir_path = Path(sql_dir)
    file_names: List[str] = sorted(sql_dir_path.glob('*.sql'))

    if env == 'LOCAL':
        file_names.pop(0)

    if file_names:
        logging.info('Found SQL files:')
        for file_name in file_names:
            logging.info(file_name.name)

    return [str(f) for f in file_names]


def prep_stmt_list(secret_file: str, stmt_list: List[str]) -> List[str]:
    '''
    Replaces placeholders in SQL statements with secret values

    Params:
        secret_file (str): Path to secret file
        stmt_list (list[str]): List of SQL statements

    Returns:
        prepped_stmt_list (list[str]): List of prepped SQL statements
    '''
    try:
        with open(secret_file, 'r', encoding='utf-8') as f:
            secret = json.load(f)
    except FileNotFoundError as e:
        logging.error(f'Secret file not found: {secret_file}')
        raise e
    except json.JSONDecodeError as e:
        logging.error(f'Invalid JSON format in secret file: {secret_file}')
        raise e

    prepped_stmt_list = []
    for stmt in stmt_list:
        for key, val in secret.items():
            stmt = stmt.replace(key, val)
        prepped_stmt_list.append(stmt)

    return prepped_stmt_list


def execute_sql_statements(env:str, sql_files: List[str], aws_secret: Dict[str, str]) -> str | None:
    '''
    Execute SQL statements on the provided DuckDB

    Params:
        env (str): Environment (local, prod)
        sql_files (list[str]): List of files containing SQL to be executed
        aws_secret (dict[str, str]): AWS credentials to allow reads from S3

    Returns:
        str | None: 'SUCCESS' or None in case of failure
    '''

    con = get_duckb_conn() if env == 'LOCAL' else get_motherduck_conn()

    try:
        for file in sql_files:
            logging.info(f'Executing SQL script: "{file.split('/')[-1]}"')
            with open(file, 'r', encoding='utf-8') as f:
                stmt_list = f.read().strip().split(';')
                prepped_stmt_list = prep_stmt_list(aws_secret, stmt_list)
                for stmt in prepped_stmt_list:
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            con.execute(stmt)
                        except Exception as e:
                            con.close()
                            logging.error(f'Execution of statement failed: {stmt}\nError: {e}')
                            raise e
        logging.info('All SQL statements executed')
        return 'SUCCESS'
    finally:
        con.close()


def main():
    '''Main function to execute script'''

    env = get_environment()

    logging.info(f'Executing in environment: {env}')

    aws_secret = os.getenv('AWS_SECRET')

    if not aws_secret:
        logging.error('Env variable "aws_secret" must be set')
        raise ValueError('Missing required env variable')

    try:
        sql_dir = Path(__file__).parent / "sql"
        sql_files = get_file_names(env, sql_dir)
        if not sql_files:
            raise FileNotFoundError(f'No SQL files found in directory: "{sql_dir}"')
        _ = execute_sql_statements(env, sql_files, aws_secret)
        logging.info('Removing temp files...')
        temp_files = Path('.').glob('TMP_DUCK_DB_EXPORT_*')
        if temp_files: # pylint: disable=using-constant-test
            for file in temp_files:
                logging.info(f'Deleting file: "{file}"')
                os.remove(file)
        else:
            logging.info('No temp files found')
        logging.info('SUCCESS: Database setup finished')
    except Exception as e:
        logging.error(f'An error occurred during database setup: {e}')
        raise


if __name__ == '__main__':
    main()
