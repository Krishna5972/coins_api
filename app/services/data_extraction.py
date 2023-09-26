import pandas as pd
from datetime import datetime
from binance.enums import HistoricalKlinesType
from .config import *


def create_df(klines,cols,coin,timeframe='_'):
    df=pd.DataFrame(klines,columns=cols)
    df['OpenTime']=[datetime.fromtimestamp(x/1000) for x in df['OpenTime']]
    df['CloseTime']=[datetime.fromtimestamp(x/1000) for x in df['CloseTime']]
    for col in df.columns:
        if 'Time' not in col:
            df[col]=df[col].astype(float)
    # main_cols=['OpenTime',f'{coin}-USD_Open',f'{coin}-USD_High',f'{coin}-USD_Low',f'{coin}-USD_Close']
    # drop_cols=set(df.columns)-set(main_cols)
    # df.drop(drop_cols,axis=1,inplace=True)
    df['hour']=[x.hour for x in df['OpenTime']]
    df['minute']=[x.minute for x in df['OpenTime']]
    df['day']=[x.day for x in df['OpenTime']]
    df['month']=[x.month for x in df['OpenTime']]
    df['year']=[x.year for x in df['OpenTime']]
    
    df=df[['OpenTime','hour','minute','day','month','year','open','high','low','close','volume']]
    #print(f'Gathering {coin}_{timeframe}.csv')
    #print(f'Retrived {df.shape[0]} candlestick data')
    return df


def dataextract(coin,start_str,end_str,interval_,client):
    
    
    cols = ['OpenTime',
            f'open',
            f'high',
            f'low',
            f'close',
            f'volume',
            'CloseTime',
            f'{coin}-QuoteAssetVolume',
            f'{coin}-NumberOfTrades',
            f'{coin}-TBBAV',
            f'{coin}-TBQAV',
            f'{coin}-ignore']
    klines=client.get_historical_klines(symbol=f'{coin}USDT', interval=interval_, start_str=start_str,end_str=end_str,klines_type=HistoricalKlinesType.FUTURES)
    return create_df(klines,cols,coin,interval_)   