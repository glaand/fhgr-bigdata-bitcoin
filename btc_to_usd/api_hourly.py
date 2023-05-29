import pandas as pd
import requests
from datetime import datetime
import time

def get_hist_data(fsym='BTC', tsym='USD', timeframe='day', limit=2000, toTs='', exchange=''):
    url = 'https://min-api.cryptocompare.com/data/v2/histo'
    url += timeframe

    parameters = {'fsym': fsym,
                  'tsym': tsym,
                  'limit': limit}

    if toTs:
        parameters['toTs'] = toTs

    if exchange:
        print('exchange: ', exchange)
        parameters['e'] = exchange

    print('baseurl: ', url)
    print('timeframe: ', timeframe)
    print('parameters: ', parameters)

    response = requests.get(url, params=parameters)  # Get-JSON

    data = response.json()['Data']['Data']

    return data


def data_to_dataframe(data):
    df = pd.DataFrame.from_dict(data)

    # time is stored as an epoch, we need normal dates
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['year'] = df['time'].dt.year
    df['month'] = df['time'].dt.month
    df['day'] = df['time'].dt.day
    df['hour'] = df['time'].dt.hour
    df['avg'] = df[['high', 'low']].mean(axis=1)
    df.sort_values(by='time', inplace=True)
    df.set_index('time', inplace=True)
    df = df.drop(columns=["conversionType", "conversionSymbol"])
    print(df.tail())

    return df


if __name__ == "__main__":
    end_date = time.time()  # UNIX Timestamp in seconds
    _2000_datapoints = 2000 * 60 * 60 # 2000 hours in seconds
    start_date = datetime(2008, 1, 1, 12, 0, 0)
    start_date = time.mktime(start_date.timetuple())
    data = []
    while end_date > start_date:
        time.sleep(1)
        # multiple api calls to retrieve all data because api is limited to 2000 datapoints per call
        for i in get_hist_data(toTs=end_date, timeframe='hour'):
            data.append(i)
        end_date=end_date-_2000_datapoints
    print("num of datapoints:", len(data))
    df = data_to_dataframe(data)
    # df.drop(df[df['high'] == 0].index, inplace=True)  # remove emtpy rows
    print("min date in df:", df.index.min())
    print("max date in df:", df.index.max())
    print("columns:", df.columns)
    df.to_csv("prices_hourly.csv")
    print("csv created..")

    '''
    “Volume From” and “To” are the volumes of the respective currency pair.
    For example, for the BTC-USD pair “Volume From” is the number of Bitcoins traded for US dollars while “Volume To” 
    is the number of dollars traded (for the period) for Bitcoins.
    '''
