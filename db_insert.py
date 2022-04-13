import logging
import os.path
import sys

from data_download import data_download
from coinlist import coinlist
import pandas as pd
import pymysql
from highlow_point_find import low_point, high_point
from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()

engine = create_engine("mysql://{user}:{pw}@localhost/{db}".format(user='root', pw='0000', db='highlow'))
db_connection = engine.connect()

def create_table():

    conn = pymysql.connect(host='localhost', user='root', passwd='0000', db='highlow', port=3306, charset='utf8')
    cursor = conn.cursor()


    query = "CREATE TABLE highlow_point_tb ( idx varchar(30) NOT NULL PRIMARY KEY, date DATETIME NOT NULL, price DECIMAL NOT NULL, point_type VARCHAR(4) NOT NULL, coin_type VARCHAR(10) NOT NULL)"
    cursor.execute(query)
    conn.commit()

    conn.close()

    return 'success!'


def insert_data_raw(coin):

    df = pd.read_json('업비트btc1시간봉(2018~202204).json')
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']


    data_high = high_point(10)
    data_low = low_point(10)
    data_high['coin_type'] = coin
    data_low['coin_type'] = coin

    data_high.to_sql(name='highlow_point_tb', con=engine, if_exists='append',index=False)
    data_low.to_sql(name='highlow_point_tb', con=engine, if_exists='append',index=False)

    db_connection.close()


def insert_data_api(period):

    # dataframe을 db에 연결시켜주는 코드
    engine = create_engine("mysql://{user}:{pw}@localhost/{db}".format(user='root', pw='0000', db='highlow'))
    db_connection = engine.connect()

    conn = pymysql.connect(host='localhost', user='root', passwd='0000', db='highlow', port=3306, charset='utf8')

    # 코인리스트 추출하는 모듈 호출
    coin_lst = coinlist()

    # 코인리스트에 있는 코인들을 하나씩 불러와서 api로 다운로드
    for j in coin_lst:
        df = data_download(j)

        lst_low = []
        lst_high = []

        # 저점 계산하는 코드
        for i in range(len(df)):

            lst = df.iloc[i - period:i + (period + 1)]

            if (i + (period - 1)) == len(df):
                break

            elif df.iloc[i]['low'] == lst['low'].min():
                lst_low.append(df.iloc[i].values.tolist())

        # 고점 계산하는 코드
        for k in range(len(df)):

            lst = df.iloc[k - period:k + (period + 1)]

            if (k + (period - 1)) == len(df):
                break

            elif df.iloc[k]['high'] == lst['high'].max():
                lst_high.append(df.iloc[k].values.tolist())


        # 저점 전처리 코드
        df_low = pd.DataFrame(lst_low, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df_low['point_type'] = 'low'
        df_low['idx'] = df_low['date'] + 'low' + j.split('-')[1]
        df_low = df_low.rename(columns={'low': 'price'})
        df_low = df_low[['date', 'price', 'point_type']]

        df_low['coin_type'] = j.split('-')[1]

        # 고점 전처리 코드
        df_high = pd.DataFrame(lst_high, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df_high['point_type'] = 'high'
        df_low['idx'] = df_low['date'] + 'high' + j.split('-')[1]
        df_high = df_high.rename(columns={'high': 'price'})
        df_high = df_high[['date', 'price', 'point_type']]

        df_high['coin_type'] = j.split('-')[1]

        data_upsert(df_low, conn, "highlow_point_tb")
        data_upsert(df_high, conn, "highlow_point_tb")

        conn.commit()

        #conn.close()
        # filter_new_df(df_high, 'highlow_point_tb', engine, dup_cols=['date','price','coin_type'])
        # filter_new_df(df_low, 'highlow_point_tb', engine, dup_cols=['date','price','coin_type'])

        # 저점 및 고점 db에 삽입
        # df_low.to_sql(name='highlow_point_tb', con=engine, if_exists='append', index=False)
        # df_high.to_sql(name='highlow_point_tb', con=engine, if_exists='append', index=False)

        # db 닫기
        #db_connection.close()


    return 'success!'



def data_upsert(df, dbcon, tb_name):
    try:
        cols = [column for column in list(df.columns)]

        str_query = ""
        str_query += "INSERT IGNORE INTO %s (" % (tb_name)

        for col in cols:
            str_query += "`" + col + "`,"
        str_query = str_query[:-1]
        str_query += ")"

        rows_val = []
        for index in df.index:
            rowval = []
            for column in df.columns:
                rowval.append(df.loc[index, column])
            rows_val.append(rowval)

        str_query += " values "
        for rows in rows_val:
            str_query += "("
            for row in rows:
                if str(type(row)) == "<class 'str'>":
                    str_query += "'" + str(row) + "',"
                elif str(row) == "nan":
                    str_query += "NULL,"
                else:
                    str_query += str(row) + ","
            str_query = str_query[:-1]
            str_query += "),"
        str_query = str_query[:-1]

        cursor = dbcon.cursor()
        cnt = cursor.execute(str_query)

        cursor.close()

    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error("data_upsert -> exception! %s : %s %d" % (str(ex), fname, exc_tb.tb_lineno))



#create_table()
insert_data_api(10)
