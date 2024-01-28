#!/usr/bin/env python
"""Data ingestion script."""
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    """Take argument and ingest details."""
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db_name = params.db_name
    table_name = params.table_name
    file_url = params.file_url
    gzip_file = 'output.csv.gz'
    csv_name = 'output.csv'

    os.system(f'wget {file_url} -O {gzip_file}')
    os.system(f'gunzip -c {gzip_file} > {csv_name}')

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    df_iter = pd.read_csv(f'{csv_name}',
                          iterator=True, chunksize=100000)

    df = next(df_iter)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.head(n=0).to_sql(con=engine, name=f'{table_name}',
                        if_exists='replace')
    df.to_sql(con=engine, name=f'{table_name}', if_exists='append')

    while True:
        try:
            t_start = time()
            df = next(df_iter)

            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

            df.to_sql(con=engine, name=f'{table_name}', if_exists='append')
            t_end = time()
            print(f'inserted new chunk..., took {t_end - t_start} seconds')
        except Exception as e:
            print(e)

            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest CSV data to Postgres.')

    # user
    # password
    # host
    # port
    # db_name
    # table
    # url of csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='databse host name/url')
    parser.add_argument('--port', help='database port')
    parser.add_argument('--db_name', help='name of the database')
    parser.add_argument('--table_name', help='database table name')
    parser.add_argument('--file_url', help='path to the CSV or Parquet file')

    args = parser.parse_args()

    main(args)
