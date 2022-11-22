import pandas as pd
import logging
import requests
import zlib
import json
import psycopg2
from datetime import datetime, date
from includes.db_manager import *
from pathlib import Path


source_table_name = "pronosticopormunicipiosgz_raw"
latest_source_table_name = "pronosticopormunicipiosgz_latest"
target_table_name = "latest_temp_avg_by_mun"


def create_path(path_name):
    path = Path(path_name)
    path.mkdir(parents=True, exist_ok=True)

    return path


# Retrieve data from API into DataFrame
def get_data_to_df():

    # Request data from API and decompress it
    x = requests.get('https://smn.conagua.gob.mx/webservices/?method=1')
    uncompressed_data = zlib.decompress(x.content, 15+32)
    data_dict = json.loads(uncompressed_data)

    # Load data into a DF
    df = pd.DataFrame().from_dict(data_dict)

    return df


def get_latest_data(conn:psycopg2):
    today = date.today()
    select_today = f"SELECT * FROM {source_table_name} WHERE DATE(log_timestamp) >= CURRENT_DATE;"
    df = pd.read_sql_query(select_today, conn)

    return df


# Write API Data into CSV File
def dump_data_to_file(conn):
    # Get latest data
    df = get_latest_data(conn)

    # Create if not exists path
    today = date.today()
    path_name = f"/opt/airflow/data/data_csv/{today.year}{today.month}{today.day}"
    path = create_path(path_name)

    # Write file
    df.to_csv(path.joinpath('data.csv'), index=False)


def write_latest_data():
    engine = get_pandas_db_engine()
    conn = get_connection()
    cursor = conn.cursor()

    # Get latest data
    logging.info('Querying API data into a DataFrame')
    df = get_data_to_df()

    # Write latest data and make it available in postgres
    logging.info('Writting csv file')
    path = create_path(f'/opt/airflow/data/data_csv')
    df.to_csv(path.joinpath("latest.csv"), index=False)

    # Empty latest table and add latest data
    logging.info('Writting latest data')
    cursor.execute("truncate table pronosticopormunicipiosgz_latest;")
    conn.commit()
    df.to_sql(latest_source_table_name, engine, if_exists='replace', index=False)

    # Insert data into source raw table and latest table
    logging.info('Writting data to raw table')
    df.to_sql(source_table_name, engine, if_exists='append', index=False)


# 1st Step
def get_api_data_into_db():
    # Get connection
    conn = get_connection()

    # Write data into sql tables
    logging.info('Getting data and writting it')
    write_latest_data()

    # Dump data into csv
    dump_data_to_file(conn)

    conn.close()


# 2nd Step
def write_aggregated_data():
    # Get connection
    conn = get_connection()
    cursor = conn.cursor()

    # Add data to target table
    # Restore target_table
    cursor.execute(f"TRUNCATE TABLE {target_table_name}")
    conn.commit()

    # Add aggregated data into target table
    select_statement = f"select idmun, avg(tmin) as tmin, avg(tmax) as tmax from  {source_table_name} " \
                       f"where log_timestamp >= NOW() - INTERVAL '2 HOURS' group by idmun"
    cursor.execute(f"INSERT INTO {target_table_name} (idmun, tmin, tmax) {select_statement}")
    conn.commit()
    conn.close()


# 3rd Step
def join_csv_data_and_latest():
    # Get connection
    conn = get_connection()
    engine = get_pandas_db_engine()

    # Retrieve latest data
    df = get_latest_data(conn)

    # Join csv data with sql data
    df_1 = pd.read_csv('/opt/airflow/data/data_municipios/20220501/data.csv')
    df_2 = pd.read_csv('/opt/airflow/data/data_municipios/20220503/data.csv')
    df_municipios = pd.concat([df_1,df_2], ignore_index=True)
    df_municipios.rename(columns = {'Cve_Ent':'ides', 'Cve_Mun':'idmun', 'Value':'value'}, inplace = True)

    # Upload data
    df_municipios.to_sql('datos_municipios', engine, index=False, if_exists='replace')

    # Prepare join query
    # columns_municipios = list(df_municipios.columns)
    # columns_latest = list(df.columns)
    # for c in columns_municipios:
    #     if c in columns_latest:
    #         columns_latest.remove(c)
    #
    # columns_latest.remove('id')
    # columns_latest.extend(['D.id','D.ides','D.idmun','value'])

    columns = ['cc','desciel','dh','dirvienc','dirvieng','dloc','ides','idmun','lat','lon','ndia','nes','nmun',
               'prec','probprec','raf','tmax','tmin','velvien']

    columns = [f'P.{c}' for c in columns]
    columns.append('D.value')
    columns_str = ''
    for c in columns:
        columns_str += f'{c},'
    columns_str = columns_str[:-1]

    # Build SQL query
    sql_join = f"select " \
               f"{columns_str} " \
               f"from datos_municipios as D " \
               f"join pronosticopormunicipiosgz_latest as P " \
               f"on P.idmun::integer=D.idmun::integer"

    # Get DF and write file
    join_df = pd.read_sql_query(sql_join, conn)
    path = create_path(f'/opt/airflow/data/current')
    join_df.to_csv(path.joinpath("data.csv"), index=False)

    logging.info(f"DF with {len(join_df)} rows")