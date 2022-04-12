import requests
import pandas as pd
import time

url = "https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC&count=200"


#url = "https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC&to=2018-06-08 20:00:00&count=200"

headers = {"Accept": "application/json"}

response = requests.request("GET", url, headers=headers)

df = pd.read_json(response.text)


df['candle_date_time_kst'] = df.candle_date_time_kst.str.split('T').str[0] + ' ' + df.candle_date_time_kst.str.split('T').str[1]

df = df[['candle_date_time_kst','opening_price','high_price','low_price','trade_price','candle_acc_trade_volume']]
df.columns = ['date','open','high','low','close','volume']
df.set_index('date', inplace=True, drop=True)

dfs = []
dfs.append(df)
a = 1

for i in range(16):
    url_fixed = "https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC&to=" + df.index[-1] + "&count=200"

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url_fixed, headers=headers)

    df = pd.read_json(response.text)

    df['candle_date_time_kst'] = df.candle_date_time_kst.str.split('T').str[0] + ' ' + df.candle_date_time_kst.str.split('T').str[1]

    df = df[['candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'candle_acc_trade_volume']]
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    df.set_index('date', inplace=True, drop=True)

    a += 1
    dfs.append(df)
    time.sleep(0.5)
    print(a)


df = pd.concat(dfs)
df.to_csv("업비트btc1시간봉(202201~).csv")
print(df)



