# import required module
import os, re
import pandas as pd
from tabulate import tabulate

# assign directory
directory = '.\\data_01.29_2\\'

CURRENCIES = ("AUD","BIDR","BNB","BRL","BTC","BUSD","DAI","DOT","ETH","EUR","GBP","IDRT","NGN","RUB",
              "TRY","TRX","UAH","USDT","XRP","ZAR")

COLUMNS = ("Subcurrency","AUD","BIDR","BNB","BRL","BTC","BUSD","DAI","DOT","ETH","EUR","GBP","IDRT","NGN","RUB",
              "TRY","TRX","UAH","USDT","XRP","ZAR")

COLUMNS_TIME = ("Time","Type","Subcurrency","Currency","Value")

TIME = "1672531260000"

LAST_RECORD = 2257

def read_csv(name):
    # hist_df = pd.read_csv(f".\\data\\{symbol}.csv")
    hist_df = pd.read_csv(name)
    hist_df.set_index('Open Time')

    return hist_df

def add_currency_pair(df,Subcurrency, Currency,value):

    if (df[df["Subcurrency"] == Subcurrency].empty):
        print(f"Not found {Subcurrency}")
        df2 = pd.DataFrame({'Subcurrency': [Subcurrency], Currency: [value]})
        df = pd.concat([df, df2])
    else:
        print(f"found {Subcurrency}")
        # добавление значения
        condition = df["Subcurrency"] == Subcurrency
        df.loc[condition, Currency] = value
        print(f"Inserted {value} into {Subcurrency}:{Currency}")
    return df

def add_time_type_currency_pair(df,Time,Type,Subcurrency, Currency,Value):

    df2 = pd.DataFrame({'Time': [Time],'Type': [Type], "Subcurrency": [Subcurrency],
                            "Currency":[Currency],"Value":[Value]})
    df = pd.concat([df, df2])

    return df


def read_files_time(directory):

    df_time = pd.DataFrame(columns=COLUMNS_TIME)
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
                    # читаем данные из файла
                    file_df = read_csv(f"{directory}{symbol}.csv")
                    # Читаем элементы в цикле
                    df_temp = pd.DataFrame()
                    for i in range (500):
                        value = file_df.loc[i:i+1,"Open"]
                        if (value.empty):
                            break
                        value = value.iat[0]
                        Time = file_df.loc[i:i + 1, "Open Time"].iat[0]
                        df_temp = add_time_type_currency_pair(df_temp,Time,"Open", subcurrency,currency,value)
                    df_time = pd.concat([df_time, df_temp])
                    break
    print(df_time)
    df_time.to_excel(f"result_{LAST_RECORD+2}_column.xlsx")

if __name__ == '__main__':
    read_files_time(directory)