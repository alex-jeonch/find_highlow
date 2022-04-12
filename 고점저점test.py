import json

import pandas as pd
import numpy as np
import matplotlib as plt
import plotly.graph_objects as go
from pandas import json_normalize

import upbit

df = upbit.candle_refer('minutes','60','KRW-BTC','200')
#df = pd.DataFrame.from_dict(df, orient='index')
info = json.loads(df)
df = json_normalize(info)
df = df[['candle_date_time_kst','opening_price','high_price','low_price','trade_price','candle_acc_trade_volume']] # 'candle_acc_trade_volume'
df.columns = ['date','open','high','low','close','volume']


def highlow_point(period):

    lst_low = []
    lst_high = []

    for i in range(len(df)):

        lst = df.iloc[i - period:i + (period + 1)]

        if (i + (period - 1)) == len(df):
            break

        elif df.iloc[i]['high'] == lst['high'].max():
            lst_high.append(df.iloc[i].values.tolist())

        elif df.iloc[i]['low'] == lst['low'].min():
            lst_low.append(df.iloc[i].values.tolist())

    df_low = pd.DataFrame(lst_low, columns=['date','open','high','low','close','volume'])

    return df_low


print(highlow_point(10))


# # plotly 캔들스틱 그래프 그리기
# stock_name = '비트코인 4시간봉'
fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                    open=df['open'],
                                    high=df['high'],
                                    low=df['low'],
                                    close=df['close'])])


for a in range(len(highlow_point(20))):
    fig.add_annotation(x=highlow_point(20).iloc[a]['date'],
                   y=highlow_point(20).iloc[a]['low'],
                   text="▲",
                   showarrow=False,
                   font=dict(size=16, color='blue'
                   ))

#fig.update_layout(xaxis_rangeslider_visible=False)
fig.show()
