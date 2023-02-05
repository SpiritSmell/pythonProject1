# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
apikey='YkGK5vA0KrLAkfebSr1vkPO9ui7O8x6u0SfuukDbzehshRs0AFNx9BNXFPWTEEPq'
secretkey='VvxQrsb2J6VuFQasbSRAMo8IbtHKQrojONEAMKP7ORSu3GEpi2wMOjUtPS3BDprG'

from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from tabulate import tabulate
import pandas as pd
import time
import sys, getopt
import psycopg2
from psycopg2 import sql
import logging



import os
#import mplfinance as mpf

FOLDER = ".\\data\\"

SUBCURRENCY = "BEL"
CURRENCY1 = "ETH"
CURRENCY2 = "BUSD"



def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    client = Client(apikey, secretkey)
    tickers = client.get_all_tickers()

    ticker_df = pd.DataFrame(tickers)
    print(tabulate(ticker_df))
    ticker_df.to_excel("ticker.xlsx")

    depth = client.get_order_book(symbol='BTCUSDT')

    depth_df = pd.DataFrame(depth['asks'])
    depth_df.columns = ['Price', 'Volume']
    depth_df.head()

    styler = depth_df.style
    depth_df.to_excel("depth.xlsx")

    styler.to_excel("styler.xlsx")

    print(tabulate(depth_df))

    symbol = 'ETHBTC'

    historical = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, '24 Jan 2023')

    hist_df = pd.DataFrame(historical)

    hist_df.columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume',
                       'Number of Trades', 'TB Base Volume', 'TB Quote Volume', 'Ignore']

    print(tabulate(hist_df))
    #hist_df.to_excel("historic.xlsx", sheet_name=symbol)
    hist_df.to_csv(f"{FOLDER}{symbol}.csv")

def print_time():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print(current_time)

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
        cursor.execute(insert)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"================ {error} ==================")


def read_orders(name):

    client = Client(apikey, secretkey)
    bid = ""
    ask = ""
    while True:
        # в бесконечном цикле печатаем предложения и спрос
        orders = client.get_order_book(symbol=name)
        if ((bid != orders['bids'][0][0]) or (ask != orders['asks'][0][0])):
            bid = orders['bids'][0][0]
            ask = orders['asks'][0][0]
            print(f"{name} bid {orders['bids'][0][0]} asks {orders['asks'][0][0]} time {orders['lastUpdateId']}", end ="\r")


def monitor_loop(Subcurrency, Currency1, Currency2):
    logging.warning('Started loop')
    cursor = open_database()

    client = Client(apikey, secretkey)
    bid = ""
    ask = ""
    orders ={}
    Shoulder1 = Subcurrency + Currency1
    Shoulder2 = Subcurrency + Currency2
    Direct = Currency1 + Currency2
    Start = False

    while True:
        # в бесконечном цикле печатаем предложения и спрос

        orders[Shoulder1] = client.get_order_book(symbol=Shoulder1)
        # добавляем в SQL
        add_order(cursor,Shoulder1,int(orders[Shoulder1]['lastUpdateId']),
                  float(orders[Shoulder1]['bids'][0][0]),
                  float(orders[Shoulder1]['asks'][0][0]))
        orders[Shoulder2] = client.get_order_book(symbol=Shoulder2)
        add_order(cursor, Shoulder2, int(orders[Shoulder2]['lastUpdateId']),
                  float(orders[Shoulder2]['bids'][0][0]),
                  float(orders[Shoulder2]['asks'][0][0]))
        orders[Direct] = client.get_order_book(symbol=Direct)
        add_order(cursor, Direct, int(orders[Direct]['lastUpdateId']),
                  float(orders[Direct]['bids'][0][0]),
                  float(orders[Direct]['asks'][0][0]))

        #if ((bid != orders['bids'][0][0]) or (ask != orders['asks'][0][0])):
        #    bid = orders['bids'][0][0]
        #    ask = orders['asks'][0][0]
        #    print(f"{name} bid {orders['bids'][0][0]} asks {orders['asks'][0][0]} time {orders['lastUpdateId']}", end ="\r")
        #print(f"{Shoulder1} bid {orders[Shoulder1]['bids'][0][0]}")
        print(f" {Shoulder1} bid {orders[Shoulder1]['bids'][0][0]} asks {orders[Shoulder1]['asks'][0][0]}"
              f" {Shoulder2} bid {orders[Shoulder2]['bids'][0][0]} asks {orders[Shoulder2]['asks'][0][0]}"
              f" {Direct} bid {orders[Direct]['bids'][0][0]} asks {orders[Direct]['asks'][0][0]}"
              f"")
        IndirectCost = 1/float((orders[Shoulder2]['asks'][0][0])) # покупаем BEL на 1 BUSD
        print(f"Покупаем BEL на 1 BUSD {IndirectCost} ",end="")
        IndirectCost = IndirectCost * float((orders[Shoulder1]['bids'][0][0]))
        print(f"Продаем BEL за ETH {IndirectCost} ",end="")
        IndirectCost = IndirectCost * float((orders[Direct]['bids'][0][0]))
        print(f"Продаем ETH за BUSD {IndirectCost} ",end="")
        IndirectCost = IndirectCost / 1.003003
        print(f"- комиссия  {IndirectCost} ")

        if (IndirectCost>1):
            if (not(Start)):
                print_time()
                logging.warning(f"Found profit : {IndirectCost}")
                Start = True
            logging.warning(f" {Shoulder1} bid {orders[Shoulder1]['bids'][0][0]} asks {orders[Shoulder1]['asks'][0][0]}"
              f" {Shoulder2} bid {orders[Shoulder2]['bids'][0][0]} asks {orders[Shoulder2]['asks'][0][0]}"
              f" {Direct} bid {orders[Direct]['bids'][0][0]} asks {orders[Direct]['asks'][0][0]}"
              f"")
            logging.warning(f"Profit {IndirectCost} ")
        else:
            if (Start):
                print_time()
                logging.warning(f"Found lost : {IndirectCost}")
                break




#   hist_df['Open Time'] = pd.to_datetime(hist_df['Open Time'] / 1000, unit='s')
#   hist_df['Close Time'] = pd.to_datetime(hist_df['Close Time'] / 1000, unit='s')
#   numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume', 'TB Base Volume',
#                      'TB Quote Volume']
#   hist_df[numeric_columns] = hist_df[numeric_columns].apply(pd.to_numeric, axis=1)


#   hist_df.set_index('Close Time')

#   mpf.plot(hist_df.set_index('Close Time'),
#            type='candle', style='charles',
#            volume=True,
#            title=symbol,
#            mav=(10, 20, 30))

def main(argv):
    global SUBCURRENCY
    global CURRENCY1
    global CURRENCY2
    try:
        opts, args = getopt.getopt(argv,"hs:1:2:",["subcurrency=","currency1=","currency2="])
    except getopt.GetoptError:
        print ('test.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('test.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-s", "--subcurrency"):
            SUBCURRENCY = arg
        elif opt in ("-1", "--currency1"):
            CURRENCY1 = arg
        elif opt in ("-2", "--currency2"):
            CURRENCY2 = arg
    print('Subcurrency ', SUBCURRENCY)
    print ('Currency 1 ', CURRENCY1)
    print ('Currency 2 ', CURRENCY2)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main(sys.argv[1:])
    logging.basicConfig(filename=f'{SUBCURRENCY,CURRENCY1,CURRENCY2}.log', filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s')
    monitor_loop(SUBCURRENCY,CURRENCY1,CURRENCY2)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
