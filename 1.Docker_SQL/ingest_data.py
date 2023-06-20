import argparse, os, shutil, gzip
from time import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name_gz = 'output_file.csv.gz'

    # source_file = url

    os.system(f"wget {url} -O {csv_name_gz}")

    input_file = csv_name_gz

    csv_name = 'output.csv'
    # Open the .gz file for reading
    with gzip.open(input_file, 'rb') as gz_file:
        # Extract the .gz file contents to a temporary file
        with open('output.csv', 'wb') as temp_file:
            shutil.copyfileobj(gz_file, temp_file)

    

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)


    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime =  pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace") #creates tables and inserts all the rows # to get first 5 rows


    while True:
        t_start = time()
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime =  pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists="append")
        
        t_end = time()
        
        print(f'Inserted another chunk.........., it took {(t_end - t_start)}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Csv data to postgres')

    # user, password, host, port and db name, table name, url of csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for placing results')
    parser.add_argument('--url', help='url of the csv file')


    args = parser.parse_args()
    main(args)








