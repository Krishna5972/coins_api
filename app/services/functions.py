import hmac
import hashlib
import requests
import time
from binance.client import Client
import os

from .data_extraction import *
import app.config as config
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.text import MIMEText
from app.models import PivotSuperTrendConfiguration

client=Client(config.api_key,config.secret_key)

def get_max_leverage(coin: str, api_key: str, secret_key: str):
    base_url = "https://fapi.binance.com"

    # Get the server time
    response = requests.get(base_url + "/fapi/v1/time")
    response.raise_for_status()

    server_time = response.json()["serverTime"]

    # Prepare the request
    endpoint = "/fapi/v1/leverageBracket"
    params = {
        "timestamp": server_time
    }

    # Sort parameters by key
    sorted_params = sorted(params.items())

    # Create a query string
    query_string = "&".join([f"{d[0]}={d[1]}" for d in sorted_params])

    # Create the signature
    signature = hmac.new(secret_key.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    # Add the signature to the parameters
    params['signature'] = signature

    # Send the GET request
    headers = {
        'X-MBX-APIKEY': api_key
    }
    response = requests.get(base_url + endpoint, params=params, headers=headers)

    # Handle the response
    response.raise_for_status()
    data = response.json()

    usdt_leverage = 0
    busd_leverage = 0

    for dict_ in data:
        if dict_['symbol'] == f'{coin}BUSD':
            busd_leverage = dict_['brackets'][0]['initialLeverage']
        
    for dict_ in data:
        if dict_['symbol'] == f'{coin}USDT':
            usdt_leverage = dict_['brackets'][0]['initialLeverage']
    
    return usdt_leverage,busd_leverage



telegram_auth_token = '5515290544:AAG9T15VaY6BIxX2VYX8x2qr34aC-zVEYMo'
telegram_group_id = 'notifier2_scanner_bot_link'

import requests
import time
from requests.exceptions import RequestException

def get_dc_signal(entry, middle_dc, current_signal):
    if np.isnan(middle_dc):
        notifier('Looks like a new coin listing check')
        return 'long' if current_signal == "Buy" else 'short'
    
    return 'long' if entry > middle_dc else 'short'

def notifier(message, tries=5, base_sleep=1):
    telegram_api_url = f'https://api.telegram.org/bot{telegram_auth_token}/sendMessage'
    data = {
        'chat_id': f'@{telegram_group_id}',
        'text': message
    }
    
    for try_num in range(tries):
        try:
            tel_resp = requests.post(telegram_api_url, data=data)
            if tel_resp.status_code == 200:
                return
            else:
                print(f'Telegram API returned {tel_resp.status_code}. Try number: {try_num + 1}')
        except RequestException as e:
            print(f'Telegram notifier encountered an error: {e}. Try number: {try_num + 1}')

        # Exponential backoff
        time.sleep(base_sleep * try_num)
        
    print(f'Failed to send message after {tries} attempts.')


def create_futures_api_uri_v2(self, path: str) -> str:
        url = self.FUTURES_URL
        print('Updated to v2')
        if self.testnet:
            url = self.FUTURES_TESTNET_URL
        return url + '/' + 'v2' + '/' + path


def create_futures_api_uri_v1(self, path: str) -> str:
        print('using v1')
        url = self.FUTURES_URL
        if self.testnet:
            url = self.FUTURES_TESTNET_URL
        return url + '/' + 'v1' + '/' + path

def get_signal(super_df):
    signal = ['Buy' if super_df.iloc[-1]
                                    ['in_uptrend'] == True else 'Sell'][0]
    
    return signal

def get_entry(super_df):
    return super_df.iloc[-1]['close']

def get_stake(super_df,client,risk = 0.02):
    signal = get_signal(super_df)
    entry = get_entry(super_df)
    acc_balance = round(float(client.futures_account()['totalCrossWalletBalance']), 2)
    stake = (acc_balance*0.88)
    notifier(f'USDT : Allocated stake:{round(stake,2)}')
    if signal == 'Buy':
        sl = super_df.iloc[-1]['lowerband']
        sl_perc = (entry-sl)/entry
    else:
        sl = super_df.iloc[-1]['upperband']
        sl_perc = (sl-entry)/entry

    stake = (stake*risk)/sl_perc
    
    return stake

from datetime import datetime,timedelta
from time import sleep
from numba import njit
import numpy as np
import pandas as pd
import shutil
import os
import talib

def tr(data,coin):
    data['previous_close'] = data[f'close'].shift(1)
    data['high-low'] = abs(data[f'high'] - data[f'low'])
    data['high-pc'] = abs(data[f'high']- data['previous_close'])
    data['low-pc'] = abs(data[f'low'] - data['previous_close'])

    tr = data[['high-low', 'high-pc', 'low-pc']].max(axis=1)

    return tr

def candle_size(x,coin):
    return abs(((x[f'close']-x[f'open'])/x[f'open'])*100)


def atr(data, period,coin):
    data['tr'] = tr(data,coin)
    atr = data['tr'].rolling(period).mean()

    return atr

def supertrend_v2(coin, df, period, atr_multiplier):
    hl2 = (df[f'high'] + df[f'low']) / 2
    df['atr'] = atr(df, period, coin)
    df['upperband'] = hl2 + (atr_multiplier * df['atr'])
    df['lowerband'] = hl2 - (atr_multiplier * df['atr'])
    df['in_uptrend'] = True
    df['OpenTime'] = pd.to_datetime(df['OpenTime'])

    for current in range(1, len(df.index)):
        previous = current - 1
        if df[f'close'].iloc[current] > df['upperband'].iloc[previous]:
            df['in_uptrend'].iloc[current] = True
        elif df[f'close'].iloc[current] < df['lowerband'].iloc[previous]:
            df['in_uptrend'].iloc[current] = False
        else:
            df['in_uptrend'].iloc[current] = df['in_uptrend'].iloc[previous]

        if df['in_uptrend'].iloc[current] and df['lowerband'].iloc[current] < df['lowerband'].iloc[previous]:
            df['lowerband'].iloc[current] = df['lowerband'].iloc[previous]

        if not df['in_uptrend'].iloc[current] and df['upperband'].iloc[current] > df['upperband'].iloc[previous]:
            df['upperband'].iloc[current] = df['upperband'].iloc[previous]

    return df


import numpy as np
from numba import njit

@njit
def compute_supertrend(close, upperband, lowerband, in_uptrend):
    for current in range(1, len(close)):
        previous = current - 1

        if close[current] > upperband[previous]:
            in_uptrend[current] = True
        elif close[current] < lowerband[previous]:
            in_uptrend[current] = False
        else:
            in_uptrend[current] = in_uptrend[previous]

        if in_uptrend[current] and lowerband[current] < lowerband[previous]:
            lowerband[current] = lowerband[previous]

        if not in_uptrend[current] and upperband[current] > upperband[previous]:
            upperband[current] = upperband[previous]

    return in_uptrend, upperband, lowerband

def supertrend_njit(coin, df, period, atr_multiplier):
    hl2 = (df[f'high'] + df[f'low']) / 2
    df['atr'] = atr(df, period, coin)
    df['upperband'] = hl2 + (atr_multiplier * df['atr']).values
    df['lowerband'] = hl2 - (atr_multiplier * df['atr']).values
    in_uptrend = np.ones(len(df), dtype=bool)

    in_uptrend, df['upperband'], df['lowerband'] = compute_supertrend(df[f'close'].values, df['upperband'].values, df['lowerband'].values, in_uptrend)

    df['in_uptrend'] = in_uptrend
    df['OpenTime'] = pd.to_datetime(df['OpenTime'])

    return df



def supertrend(coin,df, period, atr_multiplier):
    hl2 = (df[f'high'] + df[f'low']) / 2
    df['atr'] = atr(df, period,coin)
    df['upperband'] = hl2 + (atr_multiplier * df['atr'])
    df['lowerband'] = hl2 - (atr_multiplier * df['atr'])
    df['in_uptrend'] = True
    
    df['OpenTime']=pd.to_datetime(df['OpenTime'])
   

    for current in range(1, len(df.index)):
        previous = current - 1

        if df[f'close'][current] > df['upperband'][previous]:
            df['in_uptrend'][current] = True
        elif df[f'close'][current] < df['lowerband'][previous]:
            df['in_uptrend'][current] = False
        else:
            df['in_uptrend'][current] = df['in_uptrend'][previous]

            if df['in_uptrend'][current] and df['lowerband'][current] < df['lowerband'][previous]:
                df['lowerband'][current] = df['lowerband'][previous]

            if not df['in_uptrend'][current] and df['upperband'][current] > df['upperband'][previous]:
                df['upperband'][current] = df['upperband'][previous]
        
    return df





@njit
def cal_numba(opens,highs,lows,closes,in_uptrends,profit_perc,sl_perc,upperbands,lowerbands):
    entries=np.zeros(len(opens))
    signals=np.zeros(len(opens))  #characters  1--> buy  2--->sell
    tps=np.zeros(len(opens))
    trades=np.zeros(len(opens))  #characters   1--->w  0---->L
    close_prices=np.zeros(len(opens))
    time_index=np.zeros(len(opens))
    candle_count=np.zeros(len(opens))
    local_max=np.zeros(len(opens))
    local_min=np.zeros(len(opens))
    upper=np.zeros(len(opens))
    lower=np.zeros(len(opens))
    
    local_max_bar=np.zeros(len(opens))
    local_min_bar=np.zeros(len(opens))
    
    indication = 0
    buy_search=0
    sell_search=1
    change_index=0
    i=-1
    while(i<len(opens)):
        i=i+1
        
        if (indication == 0) & (sell_search == 1) & (buy_search == 0) & (change_index == i):
            
            sell_search=0
            flag=0
            trade= 5
            while (indication == 0):
                
                entry = closes[i]
                tp = entry - (entry * profit_perc)
                sl = entry + (entry * sl_perc)
                
                upper[i]=upperbands[i]
                lower[i]=lowerbands[i]
                
                
                entries[i]=entry
                tps[i]=tp
                signals[i]=2
                local_max[i]=highs[i+1]
                local_min[i]=lows[i+1]
                for j in range(i+1,len(opens)):
                    candle_count[i]=candle_count[i]+1
                    if lows[j] < local_min[i]:
                        local_min[i]=lows[j]
                        local_min_bar[i]=candle_count[i]
                    if highs[j]>local_max[i]:
                        local_max[i]=highs[j]
                        local_max_bar[i]=candle_count[i]

                    if lows[j] < tp and flag==0:

                        trades[i] = 1
                        close_prices[i]=tp
                        time_index[i]=i
                        
                        indication=1
                        buy_search=1
                        flag=1
                        
                        
                    elif (highs[j] > sl and flag==0) or (in_uptrends[j] == 'True'):
                        if highs[j] > sl and flag==0:
                            trades[i] = 0
                            close_prices[i]=sl
                            time_index[i]=i

                            indication=1
                            buy_search=1
                            flag=1
                            
                        if in_uptrends[j] == 'True':
                            

                            if trades[i] ==1:
                                change_index=j
                            elif trades[i] == 0 and flag ==1:
                                change_index=j
                            else:
                                trades[i] = 0
                                close_prices[i]=closes[j]
                                time_index[i]=i
                                change_index=j
                            
                            indication=1
                            buy_search=1
                            break
                    else:
                        pass
                break
        elif (indication == 1 ) & (sell_search == 0) & (buy_search == 1) & (change_index==i):
            
            buy_search= 0
            flag=0

            while (indication == 1):


                entry = closes[i]
                tp = entry + (entry * profit_perc)
                sl = entry - (entry * sl_perc)
                
                upper[i]=upperbands[i]
                lower[i]=lowerbands[i]
                
                entries[i]=entry
                tps[i]=tp
                signals[i]=1
                local_max[i]=highs[i+1]  
                local_min[i]=lows[i+1]
                for j in range(i+1,len(opens)):
                    if lows[j] < local_min[i]:
                        local_min[i]=lows[j]
                        local_min_bar[i]=candle_count[i]
                    if highs[j]>local_max[i]:
                        local_max[i]=highs[j]
                        local_max_bar[i]=candle_count[i]
                        
                    candle_count[i]=candle_count[i]+1
                    if highs[j] > tp and flag==0 :
                        trades[i]  = 1
                        sell_search=1
                        close_prices[i]=tp
                        time_index[i]=i
                        

                        flag=1
                        indication=0
                    elif (lows[j] < sl and flag==0) or (in_uptrends[j] == 'False'):
                        if lows[j] < sl and flag==0:

                            trades[i]= 0
                            close_prices[i]=sl
                            time_index[i]=i
                            indication=0
                            sell_search=1
                            flag=1
                            
                        if in_uptrends[j] == 'False':
                            
                            if trades[i] ==1:
                                change_index=j
                            elif trades[i] == 0 and flag ==1:
                                change_index=j
                            else:
                                trades[i] = 0
                                close_prices[i]=closes[j]
                                time_index[i]=i
                                change_index=j
                            
                            indication=0
                            sell_search=1
                            break

                    
                        
                    else:
                        pass
                break
        else:
            continue
        
    return entries,signals,tps,trades,close_prices,time_index,candle_count,local_max,local_min,local_max_bar,local_min_bar,upper,lower
    
    
# def df_perc_cal(trade_df,profit):
#     for i in trade_df.index:
#         if trade_df['trade'].loc[i]=='W':
#             trade_df.at[i,'percentage']=profit
#         else:
#             close=trade_df['close_price'].loc[i]
#             entry=trade_df['entry'].loc[i]
#             trade_df.at[i,'percentage']=-abs((close-entry)/entry)
#     return trade_df


@njit
def df_perc_cal(entries,closes,signals,percentages):
    for i in range(0,len(entries)):
        if signals[i]=='Buy':
            percentages[i]=(closes[i]-entries[i])/entries[i]
        else:
            percentages[i]=-(closes[i]-entries[i])/entries[i]
    return percentages



@njit
def histroy(percentages):
    total_amount=1000
    amount=1000
    for i in range(0,len(percentages)) :
        PNL=amount*percentages[i]
        total_amount=total_amount+PNL
    return total_amount-1000


def signal_decoding(x):
    if x == 1:
        return 'Buy'
    else:
        return 'Sell'
    
def trade_decoding(x):
    if x > 0:
        return 'W'
    else:
        return 'L'
    

def calculate_min_max(x):
    if x['signal'] == 'Buy':
        return (((x['local_max'] - x['entry'])/x['entry']),
                (x['entry'] - x['local_min'])/x['entry'])
    else:
        return ((x['entry'] - x['local_min'])/x['entry'],
               ( x['local_max'] - x['entry'])/x['entry'])
                                    
def create_signal_df(super_df,df,coin,timeframe,atr1,period,profit,sl):
    opens=super_df[f'open'].to_numpy(dtype='float64')
    highs=super_df[f'high'].to_numpy(dtype='float64')
    lows=super_df[f'low'].to_numpy(dtype='float64')
    closes=super_df[f'close'].to_numpy(dtype='float64')
    in_uptrends=super_df['in_uptrend'].to_numpy(dtype='U5')
    upperbands=super_df['upperband'].to_numpy(dtype='float64')
    lowerbands=super_df['lowerband'].to_numpy(dtype='float64')
    entries,signals,tps,trades,close_prices,time_index,candle_count,local_max,local_min,local_max_bar,local_min_bar,upper,lower=cal_numba(opens,highs,lows,closes,in_uptrends,profit,sl,upperbands,lowerbands)
    trade_df=pd.DataFrame({'signal':signals,'entry':entries,'tp':tps,'trade':trades,'close_price':close_prices,'candle_count':candle_count,
                           'local_max':local_max,'local_min':local_min,'local_max_bar':local_max_bar,'local_min_bar':local_min_bar,'upperband':upper,'lowerband':lower})
    # before_drop=trade_df.shape[0]
    # print(f'Number of columns before drop : {before_drop}')
    total_rows = trade_df.shape[0]
    
    trade_df_index=trade_df[trade_df['entry']!=0]
    
    indexes=trade_df_index.index.to_list()
    
    for i in indexes:
        try:
            trade_df.at[i,'TradeOpenTime']=df[df.index==i+1]['OpenTime'][(i+1)]
        except KeyError:
            trade_df.at[i,'TradeOpenTime']=(df[df.index==i]['OpenTime'][(i)]) 
    for i in indexes:
        try:
            trade_df.at[i,'signalTime']=df[df.index==i]['OpenTime'][(i)]
        except KeyError:
            trade_df.at[i,'signalTime']=(df[df.index==i]['OpenTime'][(i)])
            
    trade_df['signal']=trade_df['signal'].apply(signal_decoding)
    total_rows = trade_df.shape[0]
    trade_df.dropna(inplace=True)
    total_rows = trade_df.shape[0]                    
    entries=trade_df['entry'].to_numpy(dtype='float64')
    closes=trade_df['close_price'].to_numpy(dtype='float64')
    # trades=trade_df['trade'].to_numpy(dtype='U1')
    signals=trade_df['signal'].to_numpy(dtype='U5')
    outputs=np.zeros(len(entries))
    percentages=df_perc_cal(entries,closes,signals,outputs)
    trade_df['percentage'] = percentages.tolist()
    trade_df['trade']=trade_df['percentage'].apply(trade_decoding)
    # after_drop=trade_df.shape[0]
    # print(f'Number of columns after drop : {after_drop}')
    total_rows = trade_df.shape[0]
    trade_df=trade_df.reset_index(drop=True)
    total_rows = trade_df.shape[0]
    trade_df['signalTime']=pd.to_datetime(trade_df['signalTime'])
    super_df['OpenTime']=pd.to_datetime(super_df['OpenTime'])
    total_rows = trade_df.shape[0]
    if 'upperband' in trade_df.columns:
        trade_df = trade_df.drop('upperband', axis=1)
    if 'lowerband' in trade_df.columns:
        trade_df = trade_df.drop('lowerband', axis=1)
    trade_df=pd.merge(trade_df, super_df, how='left', left_on=['signalTime'], right_on = ['OpenTime'])
    total_rows = trade_df.shape[0]
    trade_df_columns = trade_df.columns.to_list()
    trade_df=trade_df[['signal',
    'entry',
    'tp',
    'trade',
    'close_price',
    'TradeOpenTime',
    'percentage',
    'OpenTime',
    'candle_count',
    'local_max','local_min',
    'local_max_bar','local_min_bar',
    'upperband','lowerband']]
    
    total_rows = trade_df.shape[0]

    if total_rows == 0:
        return pd.DataFrame()
    trade_df['max_log_return'], trade_df['min_log_return'] = zip(*trade_df.apply(calculate_min_max, axis=1))
    trade_df['prev_max_log_return'] = trade_df['max_log_return'].shift(1)
    trade_df['prev_min_log_return'] = trade_df['min_log_return'].shift(1)
    trade_df['prev_local_max_bar'] = trade_df['local_max_bar'].shift(1)
    trade_df['prev_local_min_bar'] = trade_df['local_min_bar'].shift(1)
    trade_df['prev_percentage'] = trade_df['percentage'].shift(1)

    
    
    
    total_rows = trade_df.shape[0]


    trade_df=trade_df[2:]

    return trade_df


import pickle
import math

def DONCHIAN(hi, lo, n):
    hi = pd.Series(hi)
    lo = pd.Series(lo)
    uc = hi.rolling(n, min_periods=n).max()
    lc = lo.rolling(n, min_periods=n).min()
    mc = (uc + lc) / 2
    return lc, mc, uc

def save_to_pkl(data, path):
    with open(path, 'wb') as file:
        pickle.dump(data, file)


def pivot(osc, LBL, LBR, highlow):
    left = []
    right = []
    pivots = []
    for i in range(len(osc)):
        pivots.append(0.0)
        if i < LBL + 1:
            left.append(osc[i])
        if i > LBL:
            right.append(osc[i])
        if i > LBL + LBR:
            left.append(right[0])
            left.pop(0)
            right.pop(0)
            if checkhl(left, right, highlow):
                pivots[i - LBR] = osc[i - LBR]
    return pivots

def checkhl(data_back, data_forward, hl):
    if hl == 'high' or hl == 'High':
        ref = data_back[len(data_back)-1]
        for i in range(len(data_back)-1):
            if ref < data_back[i]:
                return 0
        for i in range(len(data_forward)):
            if ref <= data_forward[i]:
                return 0
        return 1
    if hl == 'low' or hl == 'Low':
        ref = data_back[len(data_back)-1]
        for i in range(len(data_back)-1):
            if ref > data_back[i]:
                return 0
        for i in range(len(data_forward)):
            if ref >= data_forward[i]:
                return 0
        return 1


def supertrend_pivot(coin, df, period, atr_multiplier, pivot_period):

    pivot_period = pivot_period
    trend_atr = atr_multiplier
    trend_period = period

    df['OpenTime'] = df['OpenTime'].apply(
        lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, int) else x)
    

    df['prev_close'] = df['close'].shift(1)
    df['prev_open'] = df['open'].shift(1)

    df['color'] = df.apply(lambda x: 1 if x['close'] >
                           x['open'] else -1, axis=1)

   
    
    df['pivot_high'] = pivot(df['high'], pivot_period, pivot_period, 'high')
    df['pivot_low'] = pivot(df['low'], pivot_period, pivot_period, 'low')
    df['atr'] = talib.ATR(df['high'], df['low'],
                          df['close'], timeperiod=trend_period)

    df['pivot_high'] = df['pivot_high'].shift(pivot_period)
    df['pivot_low'] = df['pivot_low'].shift(pivot_period)

    center = np.NaN
    lastpp = np.NaN
    centers = [np.NaN]
    for idx, row in df.iterrows():
        ph = row['pivot_high']
        pl = row['pivot_low']

        if ph:
            lastpp = ph
        elif pl:
            lastpp = pl
        else:
            lastpp = np.NaN

        if not math.isnan(lastpp):
            if math.isnan(centers[-1]):
                centers.append(lastpp)
            else:
                center = round(((centers[-1] * 2) + lastpp)/3, 9)
                centers.append(center)
        df.at[idx, 'center'] = center

    df.ffill(axis=0, inplace=True)
    df['up'] = df['center']-(trend_atr*df['atr'])
    df['down'] = df['center']+(trend_atr*df['atr'])

    Tup = [np.NaN]
    Tdown = [np.NaN]
    Trend = [0]
    df['prev_close'] = df['close'].shift(1)
    for idx, row in df.iterrows():
        if row['prev_close'] > Tup[-1]:
            Tup.append(max(row['up'], Tup[-1]))
        else:
            Tup.append(row['up'])

        if row['prev_close'] < Tdown[-1]:
            Tdown.append(min(row['down'], Tdown[-1]))
        else:
            Tdown.append(row['down'])

        if row['close'] > Tdown[-1]:
            df.at[idx, 'in_uptrend'] = True
            Trend.append(True)
        elif row['close'] < Tup[-1]:
            df.at[idx, 'in_uptrend'] = False
            Trend.append(False)
        else:
            if math.isnan(Trend[-1]):
                df.at[idx, 'in_uptrend'] = True
                Trend.append(True)
            else:
                df.at[idx, 'in_uptrend'] = Trend[-1]
                Trend.append(Trend[-1])

    Tup.pop(0)
    Tdown.pop(0)
    df['lowerband'] = Tup
    df['upperband'] = Tdown
    return df


def get_prev_pivot_supertrend_signal(pivot_super_df):
    trend= pivot_super_df.iloc[-2]['in_uptrend']
    if trend == True:
        return "Buy"
    else:
        return "Sell"
    
def get_signal(super_df):
    signal = ['Buy' if super_df.iloc[-1]
                                    ['in_uptrend'] == True else 'Sell'][0]
    
    return signal

def get_prev_signal(super_df):
    signal = ['Buy' if super_df.iloc[-2]
                                    ['in_uptrend'] == True else 'Sell'][0]
    
    return signal


def get_entry(super_df):
    return super_df.iloc[-1]['close']

def get_stake(super_df,client,risk = 0.02):
    signal = get_signal(super_df)
    entry = get_entry(super_df)
    client._create_futures_api_uri = create_futures_api_uri_v2.__get__(client, Client)
    acc_balance = round(float(client.futures_account()['totalCrossWalletBalance']), 2)
    client._create_futures_api_uri = create_futures_api_uri_v1.__get__(client, Client)
    stake = (acc_balance*0.88)
    notifier(f'USDT : Allocated stake:{round(stake,2)}')
    if signal == 'Buy':
        sl = super_df.iloc[-1]['lowerband']
        sl_perc = (entry-sl)/entry
    else:
        sl = super_df.iloc[-1]['upperband']
        sl_perc = (sl-entry)/entry

    stake = (stake*risk)/sl_perc
    
    return stake

def get_over_all_trend(coin):
    str_date = (datetime.now()- timedelta(days=20)).strftime('%b %d,%Y')
    end_str = (datetime.now() +  timedelta(days=3)).strftime('%b %d,%Y')

    df=dataextract(coin,str_date,end_str,'1d',client)
    df['Date'] = pd.to_datetime(df['OpenTime'])
    df.set_index('Date', inplace=True)

    df['ROC_6'] = df['close'].pct_change(periods=6) * 100
    df['ROC_EMA'] = df['ROC_6'].ewm(span=3, adjust=False).mean()

    
    latest_ROC_EMA = df['ROC_EMA'].iloc[-1]
    if latest_ROC_EMA > 0:
        trend = "Uptrend"
    elif latest_ROC_EMA < 0:
        trend = "Downtrend"
    else:
        trend = "Neutral"

    print(f"Latest Rate of Change EMA: {latest_ROC_EMA:.2f}%")
    print(f"Overall trend: {trend}")
    
    return trend

def get_middle_dc(client,coin):
    str_date = (datetime.now()- timedelta(days=450)).strftime('%b %d,%Y')
    end_str = (datetime.now() +  timedelta(days=3)).strftime('%b %d,%Y')

    df_day=dataextract(coin,str_date,end_str,'1d',client)
    lc, mc, uc = DONCHIAN(df_day['high'].shift(1), df_day['low'].shift(1), 20)
    df_day['Donchian_Lower'] = lc
    df_day['Donchian_Middle'] = mc
    df_day['Donchian_Upper'] = uc

    middle_dc = df_day['Donchian_Middle'].iloc[-1]

    return middle_dc

def get_latest_df(data,df):
    candle = data['k']
    candle_data = [candle['t'], candle['o'],
                candle['h'], candle['l'], candle['c'], candle['v']]
    temp_df = pd.DataFrame([candle_data], columns=[
                        'OpenTime', 'open', 'high', 'low', 'close', 'volume'])
    temp_df['OpenTime'] = temp_df['OpenTime'] / 1000  
    temp_df['OpenTime'] = temp_df['OpenTime'].apply(lambda x: datetime.fromtimestamp(x))
    if df['OpenTime'].iloc[-1] == temp_df['OpenTime'].iloc[-1]:
        df = df[:-1]
    df = pd.concat([df, temp_df])
    cols = ['open', 'high', 'low', 'close', 'volume']
    for col in cols:
        df[col] = df[col].astype(float)
    
    df = df.iloc[1:]
    df.reset_index(drop=True,inplace=True)
    return df

def get_ema(super_df,ema_condition = 'ema_81'):
    ema_period = int(ema_condition.split('_')[1])
    ema_series = talib.EMA(super_df['close'], ema_period)
    return ema_series.iloc[-1]


def close_any_open_positions(coin,client):
    client._create_futures_api_uri = create_futures_api_uri_v2.__get__(client, Client)
    positions = client.futures_position_information(symbol=f'{coin}USDT')
    client._create_futures_api_uri = create_futures_api_uri_v1.__get__(client, Client)
    closed = 0
  
    for position in positions:
        if float(position['positionAmt']) > 0:
            closed = 1
            client.futures_create_order(
                symbol=f"{position['symbol']}",
                side='SELL',
                type='MARKET',
                quantity=position['positionAmt'],
                dualSidePosition=True,
                positionSide='LONG'
            )
        elif float(position['positionAmt']) < 0:
            closed = 1
            client.futures_create_order(
                symbol=f"{position['symbol']}",
                side='BUY',
                type='MARKET',
                quantity=abs(float(position['positionAmt'])),
                dualSidePosition=True,
                positionSide='SHORT'
            )

    if closed == 0:
        notifier(f'No positions to close : {coin}')


def cancel_all_open_orders(coin,client):
    orders = client.futures_get_open_orders(symbol=f'{coin}USDT')
    for order in orders:
        client.futures_cancel_order(symbol=order['symbol'], orderId=order['orderId'])

def close_long_position(coin,client):
    #close long position
    client._create_futures_api_uri = create_futures_api_uri_v1.__get__(client, Client)
    client.futures_create_order(
                symbol=f'{coin}USDT', side='SELL', type='MARKET', quantity=100000, dualSidePosition=True, positionSide='LONG')
    notifier(f'USDT : Long Position Closed')

def get_timeframe():
    print("Please select a timeframe from the options below:")
    options = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '1d']
    for idx, option in enumerate(options, 1):
        print(f"{idx}) {option}")
    
    selection = input("Enter the number corresponding to your choice: ")

    # Validate user's selection
    while not selection.isdigit() or int(selection) not in range(1, len(options)+1):
        print("Invalid selection. Please select a valid timeframe.")
        selection = input("Enter the number corresponding to your choice: ")

    return options[int(selection) - 1]
def close_short_position(coin,client):
    #close short position
    client._create_futures_api_uri = create_futures_api_uri_v1.__get__(client, Client)
    client.futures_create_order(
                symbol=f'{coin}USDT', side='BUY', type='MARKET', quantity=100000, dualSidePosition=True, positionSide='SHORT')
    notifier(f'USDT : Short Position Closed') 


def get_pivot_supertrend_signal(pivot_super_df):
    trend= pivot_super_df.iloc[-1]['in_uptrend']
    if trend == True:
        return "Buy"
    else:
        return "Sell"
    

def get_lowerband(super_df):
    return super_df.iloc[-1]['lowerband']

def get_prev_lowerband(super_df):
    return super_df.iloc[-2]['lowerband']

def get_upperband(super_df):
    return super_df.iloc[-1]['upperband']

def get_prev_upperband(super_df):
    return super_df.iloc[-2]['upperband']

import json
import websocket
from threading import Timer

def fetch_volatile_coin(shared_coin,duration=30, sleep_time=600):
    stream = "wss://fstream.binance.com/ws/!ticker@arr"
    symbol_data = {}

    def get_volatile_dataframe():
        df = pd.DataFrame.from_dict(symbol_data, orient='index')
        df['PctRange'] = (df['h'] - df['l']) / abs(df['l']) * 100
        df['Volatility'] = df['PctRange'].rolling(window=1).mean()
        df_volatility = df.sort_values(by='Volatility')
        return df_volatility

    def on_message(ws, message):
        msg = json.loads(message)
        symbols = [x for x in msg if x['s'].endswith('USDT')]
        frame = pd.DataFrame(symbols)[['E', 's', 'o', 'h', 'l', 'c']]
        frame.E = pd.to_datetime(frame.E, unit='ms')
        frame[['o', 'h', 'l', 'c']] = frame[['o', 'h', 'l', 'c']].astype(float)
        for _, row in frame.iterrows():
            symbol = row['s']
            symbol_data[symbol] = row.to_dict()

    def on_error(ws, error):
        print(f"WebSocket Error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("WebSocket connection closed.")

    def stop_ws(ws):
        print(f"Stopping websocket after {duration} seconds.")
        ws.close()

    ws = websocket.WebSocketApp(stream, on_message=on_message, on_error=on_error, on_close=on_close)
    file_name = "volatile_coins.csv"
    previous_coin = get_last_coin_from_csv(file_name)
    while True:
        try:
            timer = Timer(duration, stop_ws, [ws])
            timer.start()

            print('checking for new volatile coin')
            ws.run_forever()

            df = get_volatile_dataframe()
            volatile_coin = df.iloc[-1]['s']

            print(volatile_coin)

            volatile_coin = volatile_coin.split('USDT')[0]

            shared_coin.value = volatile_coin

            if previous_coin != volatile_coin and previous_coin != None:
                start_time = pd.Timestamp.now()  # Current time for new coin
                save_to_csv(volatile_coin, start_time)  # Save the new coin with its start_time
                send_mail("volatile_coins.csv")
                previous_coin = volatile_coin

            notifier(f'Shared_coin updated to {shared_coin.value}')

            print(f'Sleeping for {sleep_time/60} minutes')
            time.sleep(sleep_time)

        except Exception as e:
            print(f"Error: {e}. Retrying in 10 seconds...")
            time.sleep(10)

def get_last_coin_from_csv(file_name):
    if os.path.exists(file_name):
        last_row = pd.read_csv(file_name, nrows=1, skipfooter=1, engine='python')
        if not last_row.empty:
            return last_row['coin'].iloc[0]
    return None


def send_mail(filename, subject='SARAVANA BHAVA'):
    from_ = 'gannamanenilakshmi1978@gmail.com'
    to = 'vamsikrishnagannamaneni@gmail.com'

    message = MIMEMultipart()
    message['From'] = from_
    message['To'] = to
    message['Subject'] = subject
    body_email = 'SARAVANA BHAVA !'

    message.attach(MIMEText(body_email, 'plain'))

    attachment = open(filename, 'rb')

    x = MIMEBase('application', 'octet-stream')
    x.set_payload((attachment).read())
    encoders.encode_base64(x)

    x.add_header('Content-Disposition', 'attachment; filename= %s' % filename)
    message.attach(x)

    s_e = smtplib.SMTP('smtp.gmail.com', 587)
    s_e.starttls()

    s_e.login(from_, 'upsprgwjgtxdbwki')
    text = message.as_string()
    s_e.sendmail(from_, to, text)
    print(f'Sent {filename}')

import pandas as pd
import os

def save_to_csv(coin, start_time, filename="volatile_coins.csv"):
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        df = pd.DataFrame(columns=["coin", "starttime"])

    new_data = {"coin": coin, "starttime": start_time}
    df = df.append(new_data, ignore_index=True)
    df.to_csv(filename, index=False)


def notifier_with_photo(file_path, caption, tries=25):
    telegram_api_url = f'https://api.telegram.org/bot{telegram_auth_token}/sendPhoto'
    files = {'photo': open(file_path, 'rb')}
    data = {'chat_id': f'@{telegram_group_id}', 'caption': caption}

    for try_num in range(tries):
        tel_resp = requests.post(telegram_api_url, files=files, data=data)

        if tel_resp.status_code == 200:
            return
        else:  
            print(f'Telegram notifier problem. Try number: {try_num + 1}')
            time.sleep(1)
    print(f'Failed to send photo after {tries} attempts.')


import logging
import pandas as pd
import time
from app.models import *
from binance.client import Client


logging.basicConfig(filename='trading_data_log.txt',  filemode='a',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_coins(data):
    coins = []
    daily_volatilites = []
    monthly_volatilites = []
    weekly_volatilites = []
    highs = []
    lows = []
    opens = []
    for i in data['data']:
        coin = i['d'][2].split('.')[0]
        daily_volatility = i['d'][4]
        monthly_volatility = i['d'][5]
        weekly_volatility = i['d'][6]
        high = i['d'][9]
        low = i['d'][10]
        open_ = i['d'][11]
        coins.append(coin)
        daily_volatilites.append(daily_volatility)
        monthly_volatilites.append(monthly_volatility)
        weekly_volatilites.append(weekly_volatility)
        highs.append(high)
        lows.append(low)
        opens.append(open_)
    df_vol = pd.DataFrame(zip(coins,daily_volatilites,monthly_volatilites,weekly_volatilites,highs,lows,opens),columns=['coin', 'volatility_d','volatility_m','volatility_w','high','low','open'])
    df_vol['time'] = datetime.now()
    df_vol['coin'] = df_vol['coin'].str.split('USDT').str[0]
    df_vol['max_perc'] = (df_vol['high'] - df_vol['open'])/df_vol['open']
    df_vol['low_perc'] = -((df_vol['open'] - df_vol['low'])/df_vol['open'])
    long_df = df_vol.sort_values(by='max_perc',ascending = False)
    short_df = df_vol.sort_values(by='low_perc',ascending = True)
    volatility_df = df_vol.sort_values(by='volatility_d',ascending = False)
    
    long_df = long_df[long_df['max_perc'] > 0.0321]
    short_df = short_df[short_df['low_perc'] < -0.0321]
    volatility_df = volatility_df[volatility_df['volatility_d']>6]
    
    possible_long = list(long_df['coin'])
    possible_short = list(short_df['coin'])
    possible_volatile = list(volatility_df['coin'])
    
    
    return possible_long,possible_short,possible_volatile


def get_scaner_data(sleep_time=3600):
    url = "https://scanner.tradingview.com/crypto/scan"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
        "Content-Type": "application/json",
        "Accept": "*/*",
    }
   
    payload = {
    "filter": [
        {"left": "exchange", "operation": "equal", "right": "BINANCE"},
        {"left": "active_symbol", "operation": "equal", "right": True},
        {"left": "currency", "operation": "in_range", "right": ["BUSD", "USDT"]}
    ],
    "options": {"lang": "en"},
    "filter2": {
        "operator": "and",
        "operands": [
            {
                "operation": {
                    "operator": "or",
                    "operands": [
                        {"expression": {"left": "typespecs", "operation": "has", "right": ["perpetual"]}}
                    ]
                }
            }
        ]
    },
    "markets": ["crypto"],
    "symbols": {
        "query": {"types": []},
        "tickers": []
    },
    "columns": [
        "base_currency_logoid", "currency_logoid", "name", "close", "Volatility.D", "Volatility.M", "Volatility.W",
        "change|60","change","high","low","open", "description", "type", "subtype", "update_mode", "exchange", "pricescale", "minmov",
        "fractional", "minmove2"
    ],
    "sort": {
        "sortBy": "Volatility.D",
        "sortOrder": "desc"
    },
    "price_conversion": {"to_symbol": False},
    "range": [0, 300]
}
    while True:
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code

            if response.status_code == 200:
                data = response.json()
                logging.info("Data fetched successfully")
                return data
                time.sleep(sleep_time)
            else:
                logging.error(f"Error {response.status_code}: {response.text}")
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")


def select_coin(data):
    coins = []
    daily_volatilites = []
    monthly_volatilites = []
    weekly_volatilites = []
    for i in data['data']:
        coin = i['d'][2].split('.')[0]
        daily_volatility = i['d'][4]
        monthly_volatility = i['d'][5]
        weekly_volatility = i['d'][6]
        coins.append(coin)
        daily_volatilites.append(daily_volatility)
        monthly_volatilites.append(monthly_volatility)
        weekly_volatilites.append(weekly_volatility)
    df_vol = pd.DataFrame(zip(coins,daily_volatilites,monthly_volatilites,weekly_volatilites),columns=['coin', 'volatility_d','volatility_m','volatility_w'])
    df_vol['time'] = datetime.now()
    df_vol['coin'] = df_vol['coin'].str.split('USDT').str[0]
    return df_vol.sort_values(by="volatility_w",ascending=False).iloc[0]['coin']

def dataextract_bybit(coin,start_str,end_str,interval_):
    
    str_date = datetime.strptime(start_str, '%b %d,%Y')
    end_str = datetime.strptime(end_str, '%b %d,%Y')
    
    timeframe = interval_
    
    timeframe_mapping = {
    '5m': 5,
    '15m': 15,
    '30m': 30,
    '45m': 45,
    '1h': 60,
    '2h': 120,
    '4h': 240,
    '1d': 'D'  
}

    timeframe = timeframe_mapping.get(timeframe, None)
        
    start_timestamp_millis = int(str_date.timestamp() * 1000)
    end_timestamp_millis = int(end_str.timestamp() * 1000)
    
    url = "https://api.bybit.com/derivatives/v3/public/kline"
    params = {
        "category": "linear",
        "symbol": f"{coin}USDT",
        "interval": str(timeframe),
        "start": str(start_timestamp_millis),
        "end": str(end_timestamp_millis),
        "limit" : 251
    }

    response = requests.get(url, params=params)

    # If you want the JSON response:
    data = response.json()
    klines = data['result']['list']
    
    df = pd.DataFrame(klines,columns = ['OpenTime','open','high','low','close','volume','turnover'])
    for col in ['open','high','low','close']:
        df[col] = df[col].astype(float)
    df['OpenTime']=[datetime.fromtimestamp(int(x)/1000) for x in df['OpenTime']]
    df['hour']=[x.hour for x in df['OpenTime']]
    df['minute']=[x.minute for x in df['OpenTime']]
    df['day']=[x.day for x in df['OpenTime']]
    df['month']=[x.month for x in df['OpenTime']]
    df['year']=[x.year for x in df['OpenTime']]
    df=df[['OpenTime','hour','minute','day','month','year','open','high','low','close','volume']]
    df = df.iloc[::-1].reset_index(drop=True)
    OpenTime = df.iloc[-1]['OpenTime']
    return df





