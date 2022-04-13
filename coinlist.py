import json
import upbit


def coinlist():

    lst = upbit.market_refter()

    coin_lst = []

    for i in range(len(lst)):
        if lst[i]['market'].split('-')[0] == 'KRW':
            coin_lst.append(lst[i]['market'])
        else:
            pass

    return coin_lst


