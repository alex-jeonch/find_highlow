import requests
import pandas as pd
import time




def data_download(coin):

    url = "https://api.upbit.com/v1/candles/minutes/60?market=" + coin + "&count=48"

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




