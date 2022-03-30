#CryptoDataDownload
# First import the libraries that we need to use
from turtle import end_fill
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import math
import os

CSV_FILE_SUFFIX = "_data.csv"

def get_time_intervals(req_limit, req_interval, start, end):
    start_date = datetime.strptime(start, "%m/%d/%Y")
    end_date = datetime.strptime(end, "%m/%d/%Y")
    intervals = req_limit * req_interval
    date_range = end_date - start_date
    num_iterations = math.ceil(date_range.total_seconds() / intervals)
    prev_date = start_date
    results = []
    for i in range (0, num_iterations):
        temp_date = prev_date + timedelta(0, intervals)
        if temp_date > end_date:
            temp_date = end_date
        results.append((prev_date, temp_date))
        prev_date = temp_date
    
    return results


def fetch_data(symbol, time, start, end, iteration):
    pair_split = symbol.split('/')  # symbol must be in format XXX/XXX ie. BTC/EUR
    symbol = pair_split[0] + '-' + pair_split[1]
    time_interval = {"1min": 60, "5mins": 300, "15mins": 900, "1hour": 3600, "1day": 86400}
    url = f'https://api.pro.coinbase.com/products/{symbol}/candles?granularity={time_interval[time]}&start={start}&end={end}'
    response = requests.get(url)
    if response.status_code == 200:  # check to make sure the response from server is good
        data = pd.DataFrame(json.loads(response.text), columns=['unix', 'low', 'high', 'open', 'close', 'volume'])
        data['date'] = pd.to_datetime(data['unix'], unit='s')  # convert to a readable date
        data['vol_fiat'] = data['volume'] * data['close']      # multiply the BTC volume by closing price to approximate fiat volume

# if we failed to get any data, print an error...otherwise write the file
        if data is None:
            print("Did not return any data from Coinbase for this symbol")
        else:
             data.to_csv(f'Coinbase_{pair_split[0] + pair_split[1]}_iter{iteration}_data.csv', index=False)
    else:
        print("Did not receieve OK response from Coinbase API")

def get_csv_filenames(files):
    return [filename for filename in files if filename.endswith(CSV_FILE_SUFFIX)]

def combine_csvs(path="."):
    """
    Function combines the multiple CSVs into one CSV called Coinbase_BTCUSD.csv
    """
    files = os.listdir(path)
    csv_files = get_csv_filenames(files)
    dfs = [pd.read_csv(csv_file) for csv_file in csv_files]
    df_concat = pd.concat(dfs)
    df_concat = df_concat.drop_duplicates()
    df_concat.to_csv("Coinbase_BTCUSD.csv", index=False)
    df_concat.to_parquet("Coinbase_BTCUSD.pq", index=False)
    for file in csv_files:
        if file.endswith(CSV_FILE_SUFFIX):
            os.remove(file)
    

    
if __name__ == "__main__":
# we set which pair we want to retrieve data for
    pair = "BTC/USD"
    time = "5mins"

    #hardcoded start 2/27/2022
    start = "2/26/2022"
    #hardcode end 3/7/2022
    end = "3/10/2022"
    time_intervals = get_time_intervals(req_limit=300, req_interval=300, start=start, end=end)
    for i, interval in enumerate(time_intervals):
        fetch_data(pair, time, interval[0], interval[1], i)
    path = "."
    combine_csvs(path)

                                   