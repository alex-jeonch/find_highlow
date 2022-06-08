import sqlalchemy
from fastapi import FastAPI
import json
from datetime import datetime
import redis
import pymysql
import pandas as pd
import time
import uvicorn

from sqlalchemy import Column, String, INT, FLOAT
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


from upbit import market_volume, coinlist, coindict
from except_coin import find_market_percent, percent

app = FastAPI()

rd = redis.StrictRedis(host='localhost', port=6379, db=0)

Base = declarative_base()


class Highlow(Base):
    __tablename__ = 'highlow_point_tb'
    id = Column(INT,nullable=False, autoincrement=True, primary_key=True)
    date = Column(String, nullable=False)
    price = Column(FLOAT, nullable=False)
    point_type = Column(String, nullable=False)
    coin_type = Column(String, nullable=False)
    etc = Column(String, nullable=False, unique=True)

@app.get('/')
def index():
    return 'online'


@app.get('/api/lowpercent')
def percent_test(market):

    dict = {}

    # redis에서 coin의 현재가를 불러옴
    result = rd.get(market)
    price = float(result.decode()) # decode를 해주고 float으로 불러옴

    # conn = pymysql.connect(host='localhost', user='gmc', passwd='Gmc1234!', db='upbit', port=3306, charset='utf8')
    # cursor = conn.cursor()

    SQLALCHEMY_DATABASE_URL = 'mysql+pymysql://gmc:Gmc1234!@localhost:3306/upbit'

    engine = create_engine(SQLALCHEMY_DATABASE_URL)

    Session = sessionmaker(bind=engine)
    session = Session()

    connection = engine.connect()
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('highlow_point_tb', metadata, autoload=True, autoload_with=engine)
    #query = session.query(table).filter(table.coin_type == market, table.price < price, table.point_type == 'low').order_by(table.date)
    #query = session.query(Highlow).filter(Highlow.coin_type == market, Highlow.price < price, Highlow.point_type == 'low').order_by(Highlow.date)
    query = session.query(Highlow).filter(Highlow.coin_type == market).first()
    # query = sqlalchemy.select(table)
    #
    # result = connection.execute(query)
    # data = result.fetchone()
    print(query)


    # db에서 선택된 코인의 현재가보다 낮은 저점들을 날짜순으로 불러옴
    #query = f"SELECT * FROM highlow_point_tb where coin_type = '{market}' AND price < {price} AND point_type = 'low' ORDER BY date DESC"

    # cursor.execute(query)
    # data = cursor.fetchall()

    # 저점이 없을시 기본값 10%
    if not data:
       dict = {'low_price': 'no low_point', 'percent': 10, 'current_price' : price}
    else:
        for i in range(len(data)):
            if (((price - data[i][2]) / price) * 100) >= 4:
                # 저점에서 3% 차이나는 전저점 구하기
                query_low = f"SELECT * FROM highlow_point_tb where coin_type = '{market}' AND price < {data[i][2]} - ({data[i][2]} * 0.03) AND point_type = 'low' ORDER BY date DESC"

                cursor.execute(query_low)
                data_low = cursor.fetchall()
                conn.close()

                # 전 저점이 없을 시 저점 간격을 반환
                if not data_low:
                    percent = ((price - data[i][2]) / price) * 100
                    dict = {'date': str(data[i][1]), 'low_price': data[i][2], 'current_price': price,'percent': round(percent, 1)}

                else:
                    for j in range(len(data_low)):
                        percent = ((((price - data[i][2]) / price) * 100) + (((price - data_low[j][2]) / price) * 100)) / 2
                        dict = {'date': str(data[i][1]), 'low_price': data[i][2], 'current_price': price, 'percent': round(percent, 1)}
                        print("전저점 : ", data_low[j][2])
                        break
                break
            # 저점이 1개가 있는데 4%를 넘지 않을경우 빈 값으로 반환되기 때문에 기본값 10%으로 넣기
            elif len(data) == 1:
                dict = {'low_price': 'no low_point', 'percent': 10, 'current_price': price}
            else:
                if not dict:
                    dict = {'low_price': 'no low_point', 'percent': 10, 'current_price': price}
                else:
                    pass

    return dict


@app.get('/api/marketlist')
def market_list():
    print('요청 시작')
    df = pd.read_json(market_volume())
    kr_name = coindict()
    except_lst = find_market_percent()
    print('제외 코인 : ',except_lst)

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

    if not except_lst:
        df_total = df_total.head(30)
        print('예외 코인 없음')
    else:
        df_total = df_total[~except_market].head(30)  # ~를 포함하면 except_market 값을 제외함
        print('에외 코인 있음')

    # 각 코인별 저점 간격 계산
    df_coin = df_total['english'].tolist()
    df_per = percent(df_coin)
    df_per = pd.DataFrame(df_per)
    df_per = df_per.rename(columns={'market': 'english'})
    df_total = pd.merge(df_total, df_per, how='inner', on=None)
    print(df_total['english'])


    # 리스트에 [{english : 'KRW-MTL', korea : '메탈'},{```}] 형식으로 담기기
    df_dict = df_total[['english','korean','percent']]
    lst_dict = df_dict.to_dict('records')

    #print("코인리스트 반환 : ", lst_dict)

    return lst_dict

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)

