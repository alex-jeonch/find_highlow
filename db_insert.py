import logging
import os.path
import sys

from data_download import data_download
from coinlist import coinlist
import pandas as pd
import numpy as np
import pymysql


def create_table():

    conn = pymysql.connect(host='localhost', user='root', passwd='0000', db='highlow', port=3306, charset='utf8')
    cursor = conn.cursor()


    query = "CREATE TABLE highlow_point_tb ( id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME NOT NULL, price float NOT NULL, point_type VARCHAR(4) NOT NULL, coin_type VARCHAR(10) NOT NULL, etc VARCHAR(30) UNIQUE NOT NULL)"
    cursor.execute(query)
    conn.commit()

    conn.close()

    return 'success!'


def insert_data_api(period):

    # 코인리스트 추출하는 모듈 호출
    coin_lst = coinlist()
    count = 1
    # 코인리스트에 있는 코인들을 하나씩 불러와서 api로 다운로드
    for j in coin_lst:
        print(count)
        print(j)
        count += 1
        print('==========================================')
        df = data_download(j)

        lst_low = []
        lst_high = []

        # 저점 계산하는 코드
        for i in range(len(df)):

            lst = df.iloc[i - period:i + (period + 1)]

            if (i + (period - 1)) == len(df):
                break

            elif df.iloc[i]['low'] == lst['low'].min():
                if (df.iloc[i-1]['low'] == df.iloc[i]['low']):
                    pass
                else:
                    lst_low.append(df.iloc[i].values.tolist())

        # 고점 계산하는 코드
        for k in range(len(df)):

            lst = df.iloc[k - period:k + (period + 1)]

            if (k + (period - 1)) == len(df):
                break

            elif df.iloc[k]['high'] == lst['high'].max():
                if (df.iloc[k-1]['high'] == df.iloc[k]['high']):
                    pass
                else:
                    lst_high.append(df.iloc[k].values.tolist())



        # 저점 전처리 코드
        df_low = pd.DataFrame(lst_low, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df_low['point_type'] = 'low'
        df_low['etc'] = df_low['date'] + 'low' + j.split('-')[1]
        df_low = df_low.rename(columns={'low': 'price'})
        df_low = df_low[['date', 'price', 'point_type','etc']]

        df_low['coin_type'] = j

        # 고점 전처리 코드
        df_high = pd.DataFrame(lst_high, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df_high['point_type'] = 'high'
        df_high['etc'] = df_high['date'] + 'high' + j.split('-')[1]
        df_high = df_high.rename(columns={'high': 'price'})
        df_high = df_high[['date', 'price', 'point_type','etc']]

        df_high['coin_type'] = j

        data_upsert(df_low, "highlow_point_tb")
        data_upsert(df_high, "highlow_point_tb")

    return 'success!'





def data_upsert(df, tb_name):
    try:
        conn = pymysql.connect(host='localhost', user='root', passwd='0000', db='highlow', port=3306, charset='utf8')
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

        cursor = conn.cursor()
        cnt = cursor.execute(str_query)
        conn.commit()
        conn.close()



    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error("data_upsert -> exception! %s : %s %d" % (str(ex), fname, exc_tb.tb_lineno))



#create_table()
insert_data_api(10)

