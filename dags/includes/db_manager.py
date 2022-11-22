import psycopg2
import logging
from sqlalchemy import create_engine


sql_script_file = f"/opt/airflow/dags/includes/sql_scripts/create_tables.sql"


def execute_file(conn: psycopg2, file_name: str):
    """
    Will execute commands inside a sql file. Will raise an Error if commands are not valid.
    :param conn: SQLite Connection object
    :param file_name: file path to sql script
    :return: None
    """

    with open(file_name,'r') as file:
        sql_commands = file.read()

    cursor = conn.cursor()
    cursor.execute(sql_commands)
    conn.commit()


def setup_db():
    conn = get_connection()

    # Create tables
    logging.info('Creating tables if not exist')
    execute_file(conn, sql_script_file)
    logging.info('Tables ready')


def get_connection() -> psycopg2:
    """
    create a database connection to a SQLite database.
    SQL script found on 'sql_scripts/create_tables.sql' will always be executed to ensure tables are found.
    If file doesn't exists, it'll be created with no data.
    If file already exists no changes will be applied.
    :return: Connection object
    """

    conn = None

    # Get connection
    conn_string = "host='postgres' dbname='airflow' user='airflow' password='airflow'"
    conn = psycopg2.connect(conn_string)
    return conn


def get_pandas_db_engine():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    return engine