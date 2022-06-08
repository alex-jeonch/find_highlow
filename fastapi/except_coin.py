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

        # 코인이 없을 경우 (ex 신규상장코인) pass 한다.
        if result is None:
            pass

        else:
            price = float(result.decode()) # decode를 해주고 float으로 불러옴

            conn = pymysql.connect(host='localhost', user='gmc', passwd='Gmc1234!', db='upbit', port=3306, charset='utf8')
            cursor = conn.cursor()

            # db에서 선택된 코인의 현재가보다 낮은 저점들을 날짜순으로 불러옴
            query = f"SELECT * FROM highlow_point_tb where coin_type = '{j}' AND price < {price} AND point_type = 'low' ORDER BY date DESC"


            cursor.execute(query)
            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=['id', 'date', 'price', 'point_type', 'coin_type', 'etc'])
            df = df[['date', 'price', 'coin_type']]
            for i in range(len(df)):
                # 시간순으로 정렬되어 있기 때문에 현재가보다 낮은 저점을 low_point에 담음
                if (((price - df.iloc[i]['price']) / price) * 100) >= 4:
                    price_low = df.iloc[i]['price']
                    # 저점 price_low에 담음 그리고 그 저점보다 낮은 가격 데이터프레임 df_low를 만듬
                    df_low = df.loc[
                        df['price'] < price_low - (price_low * 0.03)
                        ]
                    if len(df_low) == 0:
                        pass

                    else:
                        if ((((price - price_low) / price) * 100) + (((price - df_low.iloc[0]['price']) / price) * 100)) / 2 >= 50:
                            print(j)
                            print('저점 : ', (((price - price_low) / price) * 100))
                            print('전 저점 : ', (((price - df_low.iloc[0]['price']) / price) * 100))
                            print(((((price - price_low) / price) * 100) + (((price - df_low.iloc[0]['price']) / price) * 100)) / 2)
                            lst.append(df.iloc[i]['coin_type'])
                            break
                        else:
                            pass

                break

            conn.close()

    print(lst)

    return lst



# async def main():
#     lst = except_list()
#     futures = [asyncio.ensure_future(find_market_percent(j)) for j in lst]
#     await asyncio.gather(*futures)

# if __name__ == '__main__':
#     print(test_find_percent())
