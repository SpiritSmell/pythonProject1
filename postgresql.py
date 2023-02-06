import os
import re

import psycopg2
from psycopg2 import sql
from contextlib import closing
from sqlalchemy import create_engine
import pandas as pd

POSTGRES_HOST = "5.53.125.79"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "Saharnica1"
POSTGRES_DATABASE = "binance"

FOLDER = ".data"

CURRENCIES = ("AUD","BIDR","BNB","BRL","BTC","BUSD","DAI","DOT","ETH","EUR","GBP","IDRT","NGN","RUB",
              "TRY","TRX","UAH","USDT","XRP","ZAR")

#CREATE TABLE public.orders
#(
#    "ID" serial,
#    "Currency" character varying(255),
#    "Subcurrency" character varying(255),
#    "Open Time" bigint,
#    "Open" double precision,
#    "High" double precision,
#    "Low" double precision,
#    "Close" double precision,
#    "Volume" double precision,
#    "Close Time" bigint,
#    "Quote Asset Volume" double precision,
#    "Number of Trades" bigint,
#    "TB Base Volume" double precision,
#    "TB Quote Volume" double precision,
#    "Ignore" boolean,
#    PRIMARY KEY ("ID")
#);

#ALTER TABLE IF EXISTS public.orders
 #   OWNER to postgres;


def create_tables():
    """ create tables in the PostgreSQL database"""

    commands = (
        """ CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                pair_name VARCHAR(255) NOT NULL,
                time INTEGER NOT NULL,
                bid double precision NOT NULL,
                ask double precision NOT NULL
                )
        """
        )


    conn = None
    try:
        # read the connection parameters
        #params = config()
        # connect to the PostgreSQL server
        #conn = psycopg2.connect(**params)
        with closing(psycopg2.connect(dbname='binance', user='postgres',
                                 password='Saharnica1', host='5.53.125.79')) as conn:
            with conn.cursor() as cur:
                #cur = conn.cursor()
                # create table one by one
                #for command in commands:
                cur.execute(commands)
                # close communication with the PostgreSQL database server
                cur.close()
                # commit the changes
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def create_tables_2():
    """ create tables in the PostgreSQL database"""

    commands = (
        """ CREATE TABLE orders
            (
                "ID" serial,
                "Currency" character varying(255),
                "Subcurrency" character varying(255),
                "Open Time" bigint,
                "Open" double precision,
                "High" double precision,
                "Low" double precision,
                "Close" double precision,
                "Volume" double precision,
                "Close Time" bigint,
                "Quote Asset Volume" double precision,
                "Number of Trades" bigint,
                "TB Base Volume" double precision,
                "TB Quote Volume" double precision,
                "Ignore" int,
                PRIMARY KEY ("ID")
            )
        """
        )


    conn = None
    try:
        # read the connection parameters
        #params = config()
        # connect to the PostgreSQL server
        #conn = psycopg2.connect(**params)
        with closing(psycopg2.connect(dbname='binance', user='postgres',
                                 password='Saharnica1', host='5.53.125.79')) as conn:
            with conn.cursor() as cur:
                #cur = conn.cursor()
                # create table one by one
                #for command in commands:
                cur.execute(commands)
                # close communication with the PostgreSQL database server
                cur.close()
                # commit the changes
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
def open_database():
    conn = psycopg2.connect(dbname='binance', user='postgres',
                                 password='Saharnica1', host=POSTGRES_HOST)
    conn.autocommit = True
    cursor = conn.cursor()

    return cursor

def add_order(cursor,ticker,time,bid,ask):

    values = [
        (ticker, time, bid, ask)
    ]
    insert = sql.SQL('INSERT INTO orders (pair_name, time, bid, ask) VALUES {}').format(
        sql.SQL(',').join(map(sql.Literal, values))
    )

    try:
        # read the connection parameters
        #params = config()
        # connect to the PostgreSQL server
        #conn = psycopg2.connect(**params)

        with cursor as cur:
            # create table one by one
            #for command in commands:
            cursor.execute(insert)
            # close communication with the PostgreSQL database server
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def add_order(cursor,ticker,time,bid,ask):

    values = [
        (ticker, time, bid, ask)
    ]
    insert = sql.SQL('INSERT INTO orders (pair_name, time, bid, ask) VALUES {}').format(
        sql.SQL(',').join(map(sql.Literal, values))
    )

    try:
        # read the connection parameters
        #params = config()
        # connect to the PostgreSQL server
        #conn = psycopg2.connect(**params)

        with cursor as cur:
            # create table one by one
            #for command in commands:
            cursor.execute(insert)
            # close communication with the PostgreSQL database server
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def csv_to_sql(directory, Subcurrency,Currency):

    # Create dataframe

    path = os.path.join(directory,f"{Subcurrency}{Currency}.csv")

    csv_df = pd.read_csv(path,index_col=0)
    csv_df["Subcurrency"] = Subcurrency
    csv_df["Currency"] = Currency

    # create sqlalchemy engine
    engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DATABASE}")
    csv_df.to_sql('orders', con=engine, if_exists='append', chunksize=1000,index=False)

def csv_files_to_sql(directory):

    # iterate over files in
    # that directory
    # Check whether the specified path exists or not
    isExist = os.path.exists(directory)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(directory)
        print("The new directory is created!")

    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            symbol = os.path.splitext(filename)[0]
            print(symbol)
            # определяем основную валюту
            for currency in CURRENCIES:
                if (re.search(f".*{currency}$",symbol)):
                    # определяем доп. валюту
                    subcurrency = re.sub(f"{currency}$","", symbol)
                    # print(f"{currency} : {subcurrency}")
                    csv_to_sql(directory,subcurrency,currency)
                    print(f"{subcurrency}{currency} processed")

if __name__ == '__main__':
  #  create_tables()
  #  create_tables_2()
  #  cur = open_database()
  #  add_order(cur,'test', 1234, 1.0, 2.0)
  #  csv_to_sql(FOLDER,"ACM","USDT")
  csv_files_to_sql(FOLDER)