#!/usr/bin/env python
# coding: utf-8
import pandas as pd
print(pd.__version__)

# Parse command line arguments
import argparse
# Download CSV file
import os 

from sqlalchemy import create_engine

import argparse

from urllib import request


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_nm = params.table_nm
    url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_nm = 'output.csv.gz'
    else:
        csv_nm = 'output.csv'
    print("CSV name : ", csv_nm)

    # wget did not work for me. So I am using urllib package
    request.urlretrieve(url, csv_nm)
    print("url : ", url)

    # Download the CSV from the URL and store it as csv_nm
    # The below command is not working for me now. So I am going to hard code it for now.
    # os.system("wget {url} -o {csv_nm}")

    # csv_nm="yellow_tripdata_2021-01.csv"


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print("Engine : ", engine)

    # df = pd.read_csv("yellow_tripdata_2021-01.csv")
    df = pd.read_csv(csv_nm)
    # df.describe()
    # df.info()

    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #Convert a df into a SQL schema and check the table structure
    #print(pd.io.sql.get_schema(df, name="yelow_taxi_data"))
    print(pd.io.sql.get_schema(df, name=table_nm))

    # Convert a df into a SQL schema (this time with Postgresql as engine)
    # print(pd.io.sql.get_schema(df, name="yelow_taxi_data", con=engine))
    # Get the data in chunks of 100000 records at a time as the file is very huge using an iterator
    # df_iter = pd.read_csv("yellow_tripdata_2021-01.csv", iterator=True, chunksize = 100000)
    # df = next(df_iter)
    # df
    # len(df)

    #df.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists='replace') 
    df.head(n=0).to_sql(name=table_nm, con=engine, if_exists='replace') 
    # Get the data in chunks of 100000 records at a time as the file is very huge using an iterator
    # Set up the iterator again
    df_iter = pd.read_csv(csv_nm, iterator=True, chunksize = 100000)
    # print(df_iter)

    # Read the CSV file iteratively as checks of 100K records and append the records to a table "yellow_taxi_data"
    from time import time
    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        # df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        df.to_sql(name=table_nm, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

if __name__ == '__main__' :

    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    # user, password, host name, port, database name, table name
    # URL of the CSV
    parser.add_argument('--user',required=True,help='user name for postgres')
    parser.add_argument('--password',required=True,help='password for postgres')
    parser.add_argument('--host',required=True,help='host name for postgres')
    parser.add_argument('--port',required=True,help='port # for postgres') # leave as string even though it's a positive integer
    parser.add_argument('--db',required=True,help='DB name for postgres')
    parser.add_argument('--table_nm',required=True,help='Tabele name where we will write the data to in postgres')
    parser.add_argument('--url',required=True,help='URL link of the CSV file')

    # parser.add_argument('integers', metavar='N', type=int, nargs='+',
    #                     help='an integer for the accumulator')
    # parser.add_argument('--sum', dest='accumulate', action='store_const',
    #                     const=sum, default=max,
    #                     help='sum the integers (default: find the max)')

    args = parser.parse_args()
    print(args)
    # print(args.accumulate(args.integers))
    main(args)


    

# Use this if we want to extract the data for the lookup. We still need to direct the output to a file while running on Linux
# !curl https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv



