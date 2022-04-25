from flask import Flask, request
import redis
import pymysql
import pandas as pd
import time
import asyncio

from upbit import market_volume, coinlist, coindict

rd = redis.StrictRedis(host='localhost', port=6379, db=0)


def except_list():

    df = pd.read_json(market_volume())
    kr_name = coindict()

    df_kr = pd.DataFrame(kr_name)
    df_kr = df_kr.rename(columns={'english': 'market'})

    df = pd.merge(df, df_kr, how='inner', on=None)

    # 시가에 비해 현재가가 50%이상 상승하지 않은 코인들
    df_upcoin = df.loc[
        ((df['trade_price'] - df['opening_price']) / df['opening_price']) * 100 < 50
        ]

    df_total = df_upcoin[['market','korean','trade_date','acc_trade_price_24h']]

    # 24시간 기준 누적 거래대금으로 상위 30개 코인 정렬
    df_total = df_total.sort_values(by=['acc_trade_price_24h'], ascending=False)
    df_total = df_total.head(30)

    c_lst = df_total['market'].tolist()

    return c_lst


def find_market_percent():

    coin_lst = except_list()

    lst = []
    for j in coin_lst:
        # redis에서 coin의 현재가를 불러옴
        result = rd.get(j)
        price = float(result.decode()) # decode를 해주고 float으로 불러옴

        conn = pymysql.connect(host='localhost', user='gmc', passwd='Gmc1234!', db='upbit', port=3306, charset='utf8')
        cursor = conn.cursor()

        # db에서 선택된 코인의 현재가보다 낮은 저점들을 날짜순으로 불러옴
        query = f"SELECT * FROM highlow_point_tb where coin_type = '{j}' AND price <= {price} AND point_type = 'low' ORDER BY date DESC"


        cursor.execute(query)
        data = cursor.fetchall()

        per_lst = []

        for i in range(len(data)):
            # 시간순으로 정렬되어 있기 때문에 현재가보다 낮은 저점을 low_point에 담음
            if i == 0:
                if data[i][2] < data[i+1][2]:
                    low_point = data[i][2]
                    if (((price - low_point)/low_point) * 100) >= 10:
                        percent = ((price - low_point)/low_point) * 100
                        per_lst.append(percent)
                        if per_lst[0] >= 50:
                            lst.append(data[i][4])
                            break
                        else:
                            pass
                    else:
                        pass
                else:
                    pass
            else:
                if data[i-1][2] > data[i][2]:
                    low_point = data[i][2]
                    # 그 저점 중에서 현재가 대비 10% 이상 차이가 나는 포인트를 찾음.
                    if (((price - low_point)/low_point) * 100) >= 10:
                        percent = ((price - low_point) / low_point) * 100
                        per_lst.append(percent)
                        if per_lst[0] >= 50:
                            lst.append(data[i][4])
                            break
                        else:
                            pass
                    else:
                        pass
                else:
                    pass

        conn.close()

        return lst


# async def main():
#     lst = except_list()
#     futures = [asyncio.ensure_future(find_market_percent(j)) for j in lst]
#     await asyncio.gather(*futures)


