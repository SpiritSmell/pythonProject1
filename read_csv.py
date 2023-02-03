# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
apikey='YkGK5vA0KrLAkfebSr1vkPO9ui7O8x6u0SfuukDbzehshRs0AFNx9BNXFPWTEEPq'
secretkey='VvxQrsb2J6VuFQasbSRAMo8IbtHKQrojONEAMKP7ORSu3GEpi2wMOjUtPS3BDprG'

from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from tabulate import tabulate
import pandas as pd
import mplfinance as mpf
import re


def read_csv(name):

    #hist_df = pd.read_csv(f".\\data\\{symbol}.csv")
    hist_df = pd.read_csv(name)
    hist_df.set_index('Open Time')

    return hist_df

    # print(tabulate(open_time))
    # hist_df['Open Time'] = pd.to_datetime(hist_df['Open Time'] / 1000, unit='s')
    # hist_df['Close Time'] = pd.to_datetime(hist_df['Close Time'] / 1000, unit='s')
    #print(tabulate(hist_df["Open Time"]))





    hist_df.set_index('Close Time')

  #  mpf.plot(hist_df.set_index('Close Time'),
  #           type='candle', style='charles',
  #           volume=True,
  #           title=symbol,
  #           mav=(10, 20, 30))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    read_csv(f".\\data\\ACMBUSD.csv")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
