import requests
import pandas as pd
import time
import pymysql
from datetime import datetime



def data_download(coin):

    conn = pymysql.connect(host='localhost', user='root', passwd='0000', db='highlow', port=3306, charset='utf8')
    cursor = conn.cursor()

    query = f"SELECT * FROM highlow_date_tb ORDER BY date DESC"

    cursor.execute(query)
    data = cursor.fetchall()

    last_time = data[0][1]
    now = datetime.now()
    diff_hour = (now.hour) - (last_time.hour)

    if diff_hour > 1:
        url = "https://api.upbit.com/v1/candles/minutes/60?market=" + coin + f"&count={12 + diff_hour}"

    else:
        url = "https://api.upbit.com/v1/candles/minutes/60?market=" + coin + "&count=12"

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    df = pd.read_json(response.text)

    df['candle_date_time_kst'] = df.candle_date_time_kst.str.split('T').str[0] + ' ' + \
                                 df.candle_date_time_kst.str.split('T').str[1]

    df = df[['candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'candle_acc_trade_volume']]
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    df.set_index('date', inplace=True, drop=True)


    df = df.reset_index()  # date를 column에 넣기 위해

    return df


def data_download_15m(coin):

    url = "https://api.upbit.com/v1/candles/minutes/15?market=" + coin + "&count=200"

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    df = pd.read_json(response.text)

    df['candle_date_time_kst'] = df.candle_date_time_kst.str.split('T').str[0] + ' ' + \
                                 df.candle_date_time_kst.str.split('T').str[1]

    df = df[
        ['candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'candle_acc_trade_volume']]
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    df.set_index('date', inplace=True, drop=True)


    df = df.reset_index()  # date를 column에 넣기 위해

    return df


