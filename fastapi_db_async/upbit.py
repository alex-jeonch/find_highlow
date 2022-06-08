import json

import requests
import pandas as pd


def candle_refer(type, time , coin, count):

    url = "https://api.upbit.com/v1/candles/" + type + '/' + time + "?market=" + coin + "&count=" + count

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    return response.text


def coinlist():

    lst = market_refter()
    lst = json.loads(lst)

    coin_lst = []

    for i in range(len(lst)):
        if lst[i]['market'].split('-')[0] == 'KRW':
            if lst[i]['market'] == 'KRW-ADA':
                pass
            else:
                coin_lst.append(lst[i]['market'])
        else:
            pass

    return coin_lst


def coindict():
    print('조회 시작')
    lst = market_refter()
    lst = json.loads(lst)

    dict = {}
    lst_co = []

    for i in range(len(lst)):
        if lst[i]['market'].split('-')[0] == 'KRW':
            if lst[i]['market'] == 'KRW-ADA':
                pass
            else:
                dict = {'english':lst[i]['market'], 'korean':lst[i]['korean_name'], 'market_warning':lst[i]['market_warning']}
                lst_co.append(dict)
        else:
            pass

    print('조회 끝')

    return lst_co


def market_refter():

    url = "https://api.upbit.com/v1/market/all?isDetails=True"

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    return response.text


def market_volume():

    coin_lst = coinlist()
    lst = ','.join(coin_lst)

    url = "https://api.upbit.com/v1/ticker?markets=" + lst

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    return response.text


if __name__ == '__main__':
    print(coindict())