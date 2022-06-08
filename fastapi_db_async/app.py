from fastapi import FastAPI
import json
import datetime
import redis
import pymysql
import pandas as pd
import time
import uvicorn
from mail_sender import MailSender
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder

from upbit import market_volume, coinlist, coindict

# db 비동기 커넥션에 필요한 라이브러리
from models import Highlow
from asyncio import current_task
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_scoped_session
from sqlalchemy import select

app = FastAPI()

rd = redis.StrictRedis(host='localhost', port=6379, db=0)

@app.get('/')
def index():
    return 'online'


@app.get('/api/lowpercent')
async def calculate_low_percent(market):

    dict = {}

    # redis에서 coin의 현재가를 불러옴
    result = rd.get(market)
    price = float(result.decode()) # decode를 해주고 float으로 불러옴

    # db 동기 커넥션
    # conn = pymysql.connect(host='localhost', user='gmc', passwd='Gmc1234!', db='upbit', port=3306, charset='utf8')
    # cursor = conn.cursor()

    # db 비동기 커넥션
    SQLALCHEMY_DATABASE_URL = 'mysql+aiomysql://gmc:Gmc1234!@localhost:3306/upbit'

    engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=True)

    async_session = sessionmaker(bind=engine, class_=AsyncSession)
    session = async_scoped_session(session_factory=async_session, scopefunc=current_task)

    query = f"SELECT * FROM highlow_point_tb where coin_type = '{market}' AND price < {price} AND point_type = 'low' ORDER BY date DESC"

    result = await session.execute(text(query))
    data = result.all()

    # db에서 선택된 코인의 현재가보다 낮은 저점들을 날짜순으로 불러옴
    # query = f"SELECT * FROM highlow_point_tb where coin_type = '{market}' AND price < {price} AND point_type = 'low' ORDER BY date DESC"
    #
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

                result_low = await session.execute(text(query_low))
                data_low = result_low.all()
                await session.close()

                # cursor.execute(query_low)
                # data_low = cursor.fetchall()
                # conn.close()

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


# 고점 간격
@app.get('/api/highpercent')
async def calculate_high_percent(market):

    dict = {}

    # redis에서 coin의 현재가를 불러옴
    result = rd.get(market)
    price = float(result.decode())  # decode를 해주고 float으로 불러옴

    SQLALCHEMY_DATABASE_URL = 'mysql+aiomysql://gmc:Gmc1234!@localhost:3306/upbit'

    engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=True)

    async_session = sessionmaker(bind=engine, class_=AsyncSession)
    session = async_scoped_session(session_factory=async_session, scopefunc=current_task)

    query = f"SELECT * FROM highlow_point_tb where coin_type = '{market}' AND price > {price} AND point_type = 'high' ORDER BY date DESC"

    result = await session.execute(text(query))
    data = result.all()

    # db에서 선택된 코인의 현재가보다 낮은 저점들을 날짜순으로 불러옴
    # query = f"SELECT * FROM highlow_point_tb where coin_type = '{market}' AND price < {price} AND point_type = 'low' ORDER BY date DESC"


    # 저점이 없을시 기본값 10%
    if not data:
        dict = {'high_price': 'no high_point', 'percent': 10, 'current_price': price}
    else:
        for i in range(len(data)):
            #if (((price - data[i][2]) / price) * 100) >= 4:
            if ((data[i][2] - price) / price) * 100 >= 4:
                # 저점에서 3% 차이나는 전저점 구하기 (현행 방법)
                query_high = f"SELECT * FROM highlow_point_tb where coin_type = '{market}' AND price > {data[i][2]} + ({data[i][2]} * 0.03) AND point_type = 'high' ORDER BY date DESC"

                result_high = await session.execute(text(query_high))
                data_high = result_high.all()
                await session.close()

                # 전 저점이 없을 시 저점 간격을 반환
                if not data_high:
                    percent = ((data[i][2] - price) / price) * 100
                    dict = {'date': str(data[i][1]), 'high_price': data[i][2], 'current_price': price,
                            'percent': round(percent, 1)}

                else:
                    for j in range(len(data_high)):
                        percent = ((((data[i][2] - price) / price) * 100) + (((data_high[j][2] - price) / price) * 100)) / 2
                        dict = {'date': str(data[i][1]), 'high_price': data[i][2], 'current_price': price,
                                'percent': round(percent, 1)}
                        print("전고점 : ", data_high[j][2])
                        break
                break
            # 저점이 1개가 있는데 4%를 넘지 않을경우 빈 값으로 반환되기 때문에 기본값 10%으로 넣기
            elif len(data) == 1:
                dict = {'high_price': 'no high_point', 'percent': 10, 'current_price': price}
            else:
                if not dict:
                    dict = {'high_price': 'no high_point', 'percent': 10, 'current_price': price}
                else:
                    pass

    return dict

@app.get('/api/marketlist')
def market_list():

    lst = rd.get("marketlist").decode('utf8')
    a = eval(lst)
    return a


# fastapi 에서는 data 유효성 검사가 들어가야함.
class Mail(BaseModel):
    senderAddress: str
    title: str
    body: str
    recipients: list
    individual: bool
    advertising: bool


@app.post('/api/send_mail')
def process_mail(mail_info: Mail):
    try:
        # mail_info가 json 형태이기 때문에 json 인코더를 통해 데이터를 받아야 검증이됨.
        json_compatible_mail = jsonable_encoder(mail_info)
        res = MailSender().req_email_send(json_compatible_mail)
        result = res.getcode()
        return {"result": result}

    except Exception as e:
        print(e)
        return {"result": 500}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5000)
