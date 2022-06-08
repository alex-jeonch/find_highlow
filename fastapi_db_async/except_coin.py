from flask import Flask, request
import redis
import pymysql
import pandas as pd
import time
import asyncio
#import schedule
import aioschedule as schedule

from upbit import market_volume, coinlist, coindict
from asyncio import current_task

from sqlalchemy import text
from upbit import market_volume, coinlist, coindict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_scoped_session
from sqlalchemy import select


rd = redis.StrictRedis(host='localhost', port=6379, db=0)


def except_list():
    print('업비트 api 조회')
    df = pd.read_json(market_volume())
    kr_name = coindict()

    df_kr = pd.DataFrame(kr_name)
    df_kr = df_kr.rename(columns={'english': 'market'})

    df = pd.merge(df, df_kr, how='inner', on=None)
    # 시가에 비해 현재가가 50%이상 상승하지 않은 코인들
    df_upcoin = df.loc[
        (((df['trade_price'] - df['opening_price']) / df['opening_price']) * 100 < 50) &
        (df['market_warning'] == 'NONE')
        ]

    df_total = df_upcoin[['market','korean','trade_date','acc_trade_price_24h']]

    # 24시간 기준 누적 거래대금으로 상위 30개 코인 정렬
    df_total = df_total.sort_values(by=['acc_trade_price_24h'], ascending=False)
    df_total = df_total.head(30)

    c_lst = df_total['market'].tolist()

    return c_lst


async def find_market_percent():

    coin_lst = except_list()
    print('코인 리스트 계산 시작')

    lst = []
    for j in coin_lst:
        # redis에서 coin의 현재가를 불러옴
        #print('redis에서 코인 가격 조회')
        result = rd.get(j)

        # 코인이 없을 경우 (ex 신규상장코인) pass 한다.
        if result is None:
            pass

        else:
            price = float(result.decode()) # decode를 해주고 float으로 불러옴


            #print('DB접속')
            # conn = pymysql.connect(host='localhost', user='gmc', passwd='Gmc1234!', db='upbit', port=3306, charset='utf8')
            # cursor = conn.cursor()
            #
            # # db에서 선택된 코인의 현재가보다 낮은 저점들을 날짜순으로 불러옴
            # #print('DB에 조회하기')
            # query = f"SELECT * FROM highlow_point_tb where coin_type = '{j}' AND price < {price} AND point_type = 'low' ORDER BY date DESC"
            #
            #
            # cursor.execute(query)
            # data = cursor.fetchall()
            # conn.close()

            SQLALCHEMY_DATABASE_URL = 'mysql+aiomysql://gmc:Gmc1234!@localhost:3306/upbit'

            engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=True)

            async_session = sessionmaker(bind=engine, class_=AsyncSession)
            session = async_scoped_session(session_factory=async_session, scopefunc=current_task)

            query = f"SELECT * FROM highlow_point_tb where coin_type = '{j}' AND price < {price} AND point_type = 'low' ORDER BY date DESC"

            result = await session.execute(text(query))
            data = result.all()
            await session.close()

            df = pd.DataFrame(data, columns=['id', 'date', 'price', 'point_type', 'coin_type', 'etc'])
            df = df[['date', 'price', 'coin_type']]
            #print('코인별 계산하기')
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

                #print('계산완료')

    return lst


async def market_list_redis():
    print('요청 시작')
    df = pd.read_json(market_volume())
    kr_name = coindict()
    except_lst = await find_market_percent()
    print('제외 코인 : ',except_lst)

    df_kr = pd.DataFrame(kr_name)
    df_kr = df_kr.rename(columns={'english': 'market'})

    df = pd.merge(df, df_kr, how='inner', on=None)
    except_market = df['market'].isin(except_lst)


    # 시가에 비해 현재가가 50%이상 상승하지 않은 코인들
    df_upcoin = df.loc[
        (((df['trade_price'] - df['opening_price']) / df['opening_price']) * 100 < 50) &
        (df['market_warning'] == 'NONE')
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
    # df_coin = df_total['english'].tolist()
    # df_per = percent(df_coin)
    # df_per = pd.DataFrame(df_per)
    # df_per = df_per.rename(columns={'market': 'english'})
    # df_total = pd.merge(df_total, df_per, how='inner', on=None)
    # print(df_total['english'])


    # 리스트에 [{english : 'KRW-MTL', korea : '메탈'},{```}] 형식으로 담기기
    df_dict = df_total[['english','korean']]
    lst_dict = df_dict.to_dict('records')

    # redis에 key : marketlist values : 코인리스트로 문자열로 값 추가
    rd.set("marketlist",str(lst_dict))
    #js_lst = json.loads(lst)

    # redis 캐쉬 만료 시간 20분 설정
    # rd.expire("marketlist",12000)
    print('end')
    return lst_dict


# schedule.every().hour.at(":00").do(market_list_redis)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)

async def main():
    await market_list_redis()

schedule.every().hour.at(":00").do(main)
loop = asyncio.get_event_loop()


while True:
    loop.run_until_complete(schedule.run_pending())
    time.sleep(1)

