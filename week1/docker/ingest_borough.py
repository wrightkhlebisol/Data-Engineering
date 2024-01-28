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
    csv_name = 'taxi+_zone_lookup.csv'

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    df = pd.read_csv(f'{csv_name}')

    df.head(n=0).to_sql(con=engine, name=f'{table_name}',
                        if_exists='replace')
    df.to_sql(con=engine, name=f'{table_name}', if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest CSV data to Postgres.')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='databse host name/url')
    parser.add_argument('--port', help='database port')
    parser.add_argument('--db_name', help='name of the database')
    parser.add_argument('--table_name', help='database table name')

    args = parser.parse_args()

    main(args)
