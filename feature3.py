from datetime import datetime
from scipy.stats import linregress
import datetime
import numpy as np
import pandas as pd

def lobf(y):
        slope, intercept = linregress(np.arange(len(y)), y)[:2]
        #return(((slope * np.arange(len(y))) + intercept).mean())
        return slope

def run_stuff():

    df = pd.read_csv('data.csv')

    df['Block time in English'] = pd.to_datetime(df['Block time in English'])
    df = df.set_index('Block time in English')

    #df2 = df[df['Block Timestamp'].between(1646069400, 1646069400 + 4*60*60)]


    #df2['slope'] = df['Number of Transaction'].rolling(5).apply(lambda s: linregress(s.reset_index())[0])

    #df3 = df2.set_index('Block time in English')

    #f3['sum'] = df3.iloc[::-1]['Number of Transaction'].rolling('1h').sum()[::-1]


    #df['slope_num_trans'] = df.iloc[::-1]['Number of Transaction'].rolling('4h').apply(lambda s: linregress(df.index.map(datetime.date.toordinal), s)[0])[::-1]
    #df['slope_num_trans'] = df.iloc[::-1]['Number of Transaction'].rolling('4h').apply(lambda x: (x[-1]-x[0])/14400)[::-1]

    df['slope_num_trans'] = df.iloc[::-1]['Number of Transaction'].rolling('4h').apply(lobf)[::-1]

if __name__ == "__main__":
    run_stuff()