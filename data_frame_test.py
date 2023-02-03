import pandas as pd

COLUMNS = ("Subcurrency","AUD","BIDR","BNB","BRL","BTC","BUSD","DAI","DOT","ETH","EUR","GBP","IDRT","NGN","RUB",
              "TRY","TRX","UAH","USDT","XRP","ZAR")

COLUMNS_TIME = ("Time","Type","Subcurrency","Currency","Value")

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

    print(f"Inserted {df2}")

    return df

#df = pd.DataFrame(columns=COLUMNS)
#df = add_currency_pair(df,"DAI","BTC",987654)
#df = add_currency_pair(df,"USDT","BIDR",654)
#df = add_currency_pair(df,"USDT","BNB",54)
#print(df)

df_time = pd.DataFrame(columns=COLUMNS_TIME)
df_time = add_time_type_currency_pair(df_time,"123123","Open","USDT","BTC",123)
print(df_time)





