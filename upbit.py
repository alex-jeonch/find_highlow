import requests

def candle_refer(type, time , coin, count):

    url = "https://api.upbit.com/v1/candles/" + type + '/' + time + "?market=" + coin + "&count=" + count

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    return response.text

#print(candle_refer('minutes','60','KRW-BTC','10'))

def market_refter():
