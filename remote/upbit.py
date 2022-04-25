import requests
import pandas as pd

def candle_refer(type, time , coin, count):

    url = "https://api.upbit.com/v1/candles/" + type + '/' + time + "?market=" + coin + "&count=" + count

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    return response.text


def coinlist():

    lst = market_refter()

    coin_lst = []

    for i in range(len(lst)):
        if lst[i]['market'].split('-')[0] == 'KRW':
            if lst[i]['market'] == 'KRW-BTC':
                pass
            else:
                coin_lst.append(lst[i]['market'])
        else:
            pass

    return coin_lst


def coindict():

    lst = market_refter()

    dict = {}
    lst_co = []

    for i in range(len(lst)):
        if lst[i]['market'].split('-')[0] == 'KRW':
            if lst[i]['market'] == 'KRW-BTC':
                pass
            else:
                dict = {'english' : lst[i]['market'], 'korean' : lst[i]['korean_name']}
                lst_co.append(dict)
        else:
            pass


    return lst_co



def market_refter():

    url = "https://api.upbit.com/v1/market/all?isDetails=false"

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    return response.json()


def market_volume():

    coin_lst = coinlist()
    lst = ','.join(coin_lst)

    url = "https://api.upbit.com/v1/ticker?markets=" + lst

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    return response.text



