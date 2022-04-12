import json

import pandas as pd
import numpy as np
import matplotlib as plt
import plotly.graph_objects as go
from pandas import json_normalize

import upbit

# df = upbit.candle_refer('minutes','60','KRW-BTC','200')
# #df = pd.DataFrame.from_dict(df, orient='index')
# info = json.loads(df)
# df = json_normalize(info)
# df = df[['candle_date_time_kst','opening_price','high_price','low_price','trade_price','candle_acc_trade_volume']] # 'candle_acc_trade_volume'
# df.columns = ['date','open','high','low','close','volume']

# df = pd.read_csv('업비트btc1시간봉(20180101-20220412.csv')
# df.to_json('업비트btc1시간봉(2018~202204).json',orient='values')
# print(df)

df = pd.read_json('업비트btc1시간봉(2018~202204).json')
df.columns = ['date','open','high','low','close','volume']

def high_point(period):

    lst_high = []

    for i in range(len(df)):

        lst = df.iloc[i - period:i + (period + 1)]

        if (i + (period - 1)) == len(df):
            break

        elif df.iloc[i]['high'] == lst['high'].max():
            lst_high.append(df.iloc[i].values.tolist())


    df_high = pd.DataFrame(lst_high, columns=['date','open','high','low','close','volume'])
    df_high['point_type'] = 'high'
    df_high = df_high.rename(columns={'high' : 'amount'})
    df_high = df_high[['date','amount','point_type']]

    return df_high




def low_point(period):

    lst_low = []

    for i in range(len(df)):

        lst = df.iloc[i - period:i + (period + 1)]

        if (i + (period - 1)) == len(df):
            break

        elif df.iloc[i]['low'] == lst['low'].min():
            lst_low.append(df.iloc[i].values.tolist())

    df_low = pd.DataFrame(lst_low, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    df_low['point_type'] = 'low'
    df_low = df_low.rename(columns={'low': 'amount'})
    df_low = df_low[['date', 'amount', 'point_type']]

    return df_low



# # plotly 캔들스틱 그래프 그리기
# fig = go.Figure(data=[go.Candlestick(x=df['date'],
#                                     open=df['open'],
#                                     high=df['high'],
#                                     low=df['low'],
#                                     close=df['close'])])


# for a in range(len(low_point(3))):
#     fig.add_annotation(x=low_point(3).iloc[a]['date'],
#                    y=low_point(3).iloc[a]['low'],
#                    text="▲",
#                    showarrow=False,
#                    font=dict(size=16, color='blue'
#                    ))
#
# for b in range(len(high_point(3))):
#     fig.add_annotation(x=high_point(3).iloc[b]['date'],
#                    y=high_point(3).iloc[b]['high'],
#                    text="▲",
#                    showarrow=False,
#                    font=dict(size=16, color='red'
#                    ))
#
#
# fig.show()
