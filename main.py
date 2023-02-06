# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
apikey='YkGK5vA0KrLAkfebSr1vkPO9ui7O8x6u0SfuukDbzehshRs0AFNx9BNXFPWTEEPq'
secretkey='VvxQrsb2J6VuFQasbSRAMo8IbtHKQrojONEAMKP7ORSu3GEpi2wMOjUtPS3BDprG'

from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from tabulate import tabulate
import pandas as pd
import os
import mplfinance as mpf

FOLDER = "./data/"


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

def read_symbols(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    client = Client(apikey, secretkey)
    tickers = client.get_all_tickers()

    ticker_df = pd.DataFrame(tickers).sort_values(by=['symbol'])

    print(tabulate(ticker_df))

    for row in ticker_df.iterrows():
        symbol = row[1]['symbol']
        path = f"{FOLDER}{symbol}.csv"
        print(symbol)

        if (os.path.isfile(path)):
            print("File exists "+path)
            continue

        historical = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, '05 Feb 2023')

        if not historical:
            print("No data for symbol "+symbol)
            continue

        hist_df = pd.DataFrame(historical)



        hist_df.columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume',
                            'Number of Trades', 'TB Base Volume', 'TB Quote Volume', 'Ignore']

        #print(tabulate(hist_df))
        #hist_df.to_excel("historic.xlsx", sheet_name=symbol)

        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume', 'TB Base Volume',
                           'TB Quote Volume']
        hist_df[numeric_columns] = hist_df[numeric_columns].apply(pd.to_numeric, axis=1)

        hist_df.to_csv(f"{FOLDER}{symbol}.csv")

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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    read_symbols('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
