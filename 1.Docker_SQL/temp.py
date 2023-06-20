import argparse, os, shutil, gzip, glob
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








