import psycopg2
from psycopg2 import sql
from contextlib import closing


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
def open_database():
    conn = psycopg2.connect(dbname='binance', user='postgres',
                                 password='Saharnica1', host='5.53.125.79')
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


if __name__ == '__main__':
  #  create_tables()
    cur = open_database()
    add_order(cur,'test', 1234, 1.0, 2.0)