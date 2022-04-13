import logging
import sys

import pandas as pd
import pymysql
import highlow_point_find
from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()

engine = create_engine("mysql://{user}:{pw}@localhost/{db}".format(user='root', pw='0000', db='highlow'))
db_connection = engine.connect()

def create_table():

    #try:
    conn = pymysql.connect(host='localhost', user='root', passwd='0000', db='highlow', port=3306, charset='utf8')
    cursor = conn.cursor()
    # except:
    #     logging.error('')
    #     sys.exit(1)

    query = "CREATE TABLE highlow_point_tb ( date DATETIME NOT NULL, price DECIMAL NOT NULL, point_type VARCHAR(4) NOT NULL, coin_type VARCHAR(10) NOT NULL)"
    cursor.execute(query)
    conn.commit()

    conn.close()


def insert_data(coin):

    df = pd.read_json('업비트btc1시간봉(2018~202204).json')
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

    data_high = highlow_point_find.high_point(10)
    data_low = highlow_point_find.low_point(10)
    data_high['coin_type'] = coin
    data_low['coin_type'] = coin

    data_high.to_sql(name='highlow_point_tb', con=engine, if_exists='append',index=False)
    data_low.to_sql(name='highlow_point_tb', con=engine, if_exists='append',index=False)

    db_connection.close()



