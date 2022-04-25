import json

from flask import Flask, request
import redis
import pymysql
import pandas as pd
import time
import asyncio

from upbit import market_volume, coinlist, coindict
from except_coin import find_market_percent

app = Flask(__name__)
rd = redis.StrictRedis(host='localhost', port=6379, db=0)

@app.route('/')
def index():
    return 'online'


@app.route('/api/lowpercent')
def calculate_percent():

    # url 뒤에 ?market= 뒤에 오는 코인을 가져온다.
    coin = request.args.get('market')

    # redis에서 coin의 현재가를 불러옴
    result = rd.get(coin)
    price = float(result.decode()) # decode를 해주고 float으로 불러옴

    conn = pymysql.connect(host='localhost', user='gmc', passwd='Gmc1234!', db='upbit', port=3306, charset='utf8')
    cursor = conn.cursor()

    # db에서 선택된 코인의 현재가보다 낮은 저점들을 날짜순으로 불러옴
    query = f"SELECT * FROM highlow_point_tb where coin_type = '{coin}' AND price <= {price} AND point_type = 'low' ORDER BY date DESC"


    cursor.execute(query)
    data = cursor.fetchall()

    dict = {}

    # 저점이 없을시 기본값 10%
    if not data:
       dict = {'low_price': 'no low_point', 'percent': 10, 'current_price' : price}
    else:
        for i in range(len(data)):
            # 시간순으로 정렬되어 있기 때문에 현재가보다 낮은 저점을 low_point에 담음
            if i == 0:
                if data[i][2] < data[i+1][2]:
                    low_point = data[i][2]
                    if (((price - low_point)/low_point) * 100) >= 4:
                        percent = ((price - low_point)/low_point) * 100
                        dict = {'date': data[i][1],'low_price': low_point, 'current_price' : price, 'percent' : round(percent, 1)}
                        break
                    else:
                        pass

                else:
                    pass
            else:
                if data[i-1][2] > data[i][2]:
                    low_point = data[i][2]
                    # 그 저점 중에서 현재가 대비 10% 이상 차이가 나는 포인트를 찾음.
                    if (((price - low_point)/low_point) * 100) >= 4:
                        percent = ((price - low_point)/low_point) * 100
                        dict = {'date': data[i][1], 'low_price': low_point, 'current_price' : price, 'percent' : round(percent, 1)}
                        break
                    else:
                        # percent = 10
                        # dict = {'date': data[i][1], 'low_price': low_point, 'current_price': price, 'percent': round(percent, 1)}
                        # break
                        pass
                else:
                    pass

    conn.close()

    return dict


@app.route('/api/marketlist')
def market_list():

    df = pd.read_json(market_volume())
    kr_name = coindict()
    except_lst = find_market_percent()

    df_kr = pd.DataFrame(kr_name)
    df_kr = df_kr.rename(columns={'english': 'market'})

    df = pd.merge(df, df_kr, how='inner', on=None)
    except_market = df['market'].isin(except_lst)

    # 시가에 비해 현재가가 50%이상 상승하지 않은 코인들
    df_upcoin = df.loc[
        ((df['trade_price'] - df['opening_price']) / df['opening_price']) * 100 < 50
        ]

    df_total = df_upcoin[['market','korean','trade_date','acc_trade_price_24h']]
    df_total = df_total.rename(columns={'market': 'english'})

    # 24시간 기준 누적 거래대금으로 상위 30개 코인 정렬
    df_total = df_total.sort_values(by=['acc_trade_price_24h'], ascending=False)
    df_total = df_total[~except_market].head(30) # ~를 포함하면 except_market 값을 제외함

    # 리스트에 [{english : 'KRW-MTL', korea : '메탈'},{```}] 형식으로 담기기
    df_dict = df_total[['english','korean']]
    lst_dict = df_dict.to_dict('records')

    # json 형식으로 response 할 떄 한글깨짐 방지
    res = json.dumps(lst_dict, ensure_ascii=False).encode('utf8')

    return res


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
