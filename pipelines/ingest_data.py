#!/usr/bin/env python
# coding: utf-8

# In[11]:


#In this Jupyter notebook, we create code to:
# - Download the CSV file
# - Read it in chunks with pandas
# - Convert datetime columns
# - Insert data into PostgreSQL using SQLAlchemy

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import os

@click.command()
@click.option('--user', default=f'{os.getenv("DB_USER")}', help='PostgreSQL user')
@click.option('--password', default=f'{os.getenv("DB_PASSWORD")}', help='PostgreSQL password')
@click.option('--host', default=f'{os.getenv("DB_HOST")}', help='PostgreSQL host')
@click.option('--port', default=int(os.getenv("DB_PORT")), type=int, help='PostgreSQL port')
@click.option('--db', default=f'{os.getenv("DB_NAME")}', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')

def ingest_data(user, password, host, port, db, table):
        # Ingestion logic here
        # import db vars from environment variables

    print(f'Ingesting data into {user}:{password}@{host}:{port}/{db}, table: {table}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read a sample of the data
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    df = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        nrows=100,
        dtype=dtype,
        parse_dates=parse_dates
    )

    # Display first rows
    print(df.head())

    # Check data types
    print(df.dtypes)

    # Check data shape
    print(df.shape)

    # Get DDL schema
    print(pd.io.sql.get_schema(df, name=table, con=engine))

    # Create the table in the database
    # n=0 ensures only the schema is created without inserting data
    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')

    # Ingest data in chunks
    df_iter = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )

    first = True

    # iterate over chunks and insert into the database
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
            first = False
            print("Table created.")

        # insert chunk
        df_chunk.to_sql(name=table, con=engine, if_exists='append')
        print("Inserted:", len(df_chunk))

if __name__ == '__main__':
    ingest_data()
# In[ ]:
