import requests
import logging
import json
import pandas as pd
from datetime import datetime,timedelta
from multiprocessing import Process, Manager
import os
from .. import cache
from ..config import *
from binance.client import Client
from .data_extraction import *
from ..models import PivotSuperTrendConfiguration
from .functions import *



from app.utils import *

@custom_memoize
def execute_and_get_results(timeframe):
    print(f'Running at {datetime.utcnow()}')
    possible_long, possible_short, possible_volatile = get_coins_list()
    half_size_long = len(possible_long) // 2
    half_size_short = len(possible_short) // 2
    half_size_volatile = len(possible_volatile) // 2

    long1, long2 = possible_long[:half_size_long], possible_long[half_size_long:]
    short1, short2 = possible_short[:half_size_short], possible_short[half_size_short:]
    volatile1, volatile2 = possible_volatile[:half_size_volatile], possible_volatile[half_size_volatile:]

    with Manager() as manager:
        final_list = manager.list()

        if timeframe == '5m':
            processes = [
            Process(target=check_volatile, args=(volatile1[:10], timeframe, final_list)),
        ]

        elif timeframe == '15m':
            processes = [
            Process(target=check_volatile, args=(volatile1, timeframe, final_list)),
            Process(target=check_volatile, args=(volatile2, timeframe, final_list))
        ]
        else:
            processes = [
            Process(target=check_longs, args=(long1, timeframe, final_list)),
            Process(target=check_longs, args=(long2, timeframe, final_list)),
            Process(target=check_shorts, args=(short1, timeframe, final_list)),
            Process(target=check_shorts, args=(short2, timeframe, final_list)),
            Process(target=check_volatile, args=(volatile1, timeframe, final_list)),
            Process(target=check_volatile, args=(volatile2, timeframe, final_list))
        ]

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        b = datetime.utcnow()

        final_list = list(final_list)

    

    strategies = ['volatile', 'long', 'short']

    result = {timeframe: {strategy: [] for strategy in strategies}}

    # Populating the structure
    for entry in final_list:
        timeframe = entry['timeframe']
        strategy = entry['strategy']

        keys = list(entry.keys())
        coin_info = {
            'coin': entry['coin'],
            'signal' : entry['signal'],
            'time': entry['time'],
            'rr' : entry['rr'],
            'inverse_trades_count' : entry['inverse_trades_count'],
            'long_term_prev_percentage' : entry['long_term_prev_percentage'],
            'short_term_prev_percentage': entry['short_term_prev_percentage'],
            'long_candle_count':entry['long_candle_count'],
            'prev_short_candle_count':entry['prev_short_candle_count'],
            'tp':entry['tp'],
            'sl':entry['sl']

        }


        result.setdefault(timeframe, {}).setdefault(strategy, []).append(coin_info)
                
    result['time'] = datetime.utcnow()
    return result



@custom_memoize_get_coins
def get_coins_list():
    print('calling for the first time')
    data = get_scaner_data(sleep_time=3600)
    possible_long, possible_short, possible_volatile = get_coins(data)

    possible_long = [item for item in possible_long if item not in possible_volatile]
    possible_short = [item for item in possible_short if item not in possible_volatile]

    return possible_long, possible_short, possible_volatile


@memoize_get_screener_data
def get_scaner_data(sleep_time=3600,first_volatile = 0):
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
            else:
                logging.error(f"Error {response.status_code}: {response.text}")
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")

def get_coins_funding():
    FUNDING_RATE_LIMIT = -0.0001
    url = 'https://www.binance.com/fapi/v1/premiumIndex'

    try:
        response = requests.get(url)
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Error: Unable to connect to the specified URL. {e}")
    df = pd.DataFrame(data)
    df['lastFundingRate']=df['lastFundingRate'].astype(float)
    df = df.sort_values(by='lastFundingRate')
    df= df[df['lastFundingRate']<=FUNDING_RATE_LIMIT]
    coin = None
    funding_rate = None
    if df.shape[0] > 0:
        coins = list(df['symbol'])
        coins = [x.split('USDT')[0] for x in coins]
        return [],[],coins
    return [],[],[]

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


def get_volatile_df(data):
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


    volatility_df = df_vol.sort_values(by='volatility_d',ascending = False)

    return volatility_df
    
    
    
 
   

@custom_memoize_first_volatile
def get_first_volatile_coin(perc):
    while True:
        current_hour = datetime.utcnow().hour
        if current_hour < 20:
            data = get_scaner_data(sleep_time=3600,first_volatile = 1)
            volatility_df = get_volatile_df(data)
            volatility_df = volatility_df[volatility_df['volatility_d']>perc]
            if volatility_df.shape[0] > 0:
                coin = list(volatility_df['coin'])[0]
                return coin
            time.sleep(1)

        

def check_longs(possible_long,timeframe,final_list):
    for coin in possible_long:
        try:
            value,data = is_long_tradable(coin, timeframe)
            if value:   
                final_list.append(data)

        except Exception as e:
            print(f'{coin} error', e)

def check_shorts(possible_short,timeframe,final_list):
    for coin in possible_short:
            try:
                value,data = is_short_tradable(coin, timeframe)
                if value:                    
                    final_list.append(data)
            except Exception as e:
                print(f'{coin} error',e)

def check_volatile(possible_volatile,timeframe,final_list):
    for coin in possible_volatile:
            try:
                value,data = is_volatile_tradable(coin, timeframe)
                if value:
                    final_list.append(data)
                    
            except Exception as e:
                print(f'{coin} error',e)

def is_long_tradable(coin,timeframe):

    data = {}
    
    timeframe_mapping = {
    '5m': (2, 30),
    '15m': (3, 15),
    '30m': (7, 9),
    '45m': (10, 6),  # Example values; adjust as needed
    '1h': (14, 4),   # Example values; adjust as needed
    '2h': (20, 2),   # Example values; adjust as needed
    '4h': (30, 1)    # Example values; adjust as needed
}

    look_back_days, candle_count_filter = timeframe_mapping.get(timeframe, (40, 1))
        
    client=Client(api_key,secret_key)
    str_date = (datetime.now()- timedelta(days=look_back_days)).strftime('%b %d,%Y')
    end_str = (datetime.now() +  timedelta(days=3)).strftime('%b %d,%Y')





    if timeframe in ['45m','2h','4h']:
        #df = dataextract_bybit(coin,str_date,end_str,timeframe)   
        df=dataextract(coin,str_date,end_str,timeframe,client)
    else:
        df=dataextract(coin,str_date,end_str,timeframe,client)

    #df.to_csv(f'data/{coin}/{coin}_{timeframe}.csv',mode='w+',index=False)

    df= df.iloc[:-1]

    df_copy = df.copy()

    pivot_st = PivotSuperTrendConfiguration()
    pivot_super_df = supertrend_pivot(coin, df_copy, pivot_st.period, pivot_st.atr_multiplier, pivot_st.pivot_period)
    pivot_signal = get_pivot_supertrend_signal(pivot_super_df)
    current_signal_short = pivot_signal
    prev_signal_short = get_prev_pivot_supertrend_signal(pivot_super_df)
    trade_df_short= create_signal_df(pivot_super_df,df,coin,timeframe,pivot_st.atr_multiplier,pivot_st.pivot_period,100,100)

    #long trend
    pivot_st = PivotSuperTrendConfiguration(period = 2, atr_multiplier = 2.6, pivot_period = 2)
    pivot_super_df = supertrend_pivot(coin, df_copy, pivot_st.period, pivot_st.atr_multiplier, pivot_st.pivot_period)
    pivot_signal = get_pivot_supertrend_signal(pivot_super_df)
    current_pivot_signal = pivot_signal
    prev_pivot_signal = get_prev_pivot_supertrend_signal(pivot_super_df)
    super_df = pivot_super_df
    signal_long = get_signal(super_df)
    current_signal_long = signal_long
    prev_signal_long = get_prev_pivot_supertrend_signal(pivot_super_df)
    trade_df_long= create_signal_df(pivot_super_df,df,coin,timeframe,pivot_st.atr_multiplier,pivot_st.pivot_period,100,100)

    candle_count = trade_df_long.iloc[-1]['candle_count']
    if candle_count < candle_count_filter:
        return False,data
    
    
    #check if tradabale
    if current_signal_long == 'Buy' and current_signal_short == 'Sell':
        if current_signal_short != prev_signal_short:
            long_trend_openTime = pd.to_datetime(trade_df_long.iloc[-1]['TradeOpenTime'])
            inverse_df_check = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime]
            inverse_trades = inverse_df_check[inverse_df_check['signal']=='Sell'].shape[0]
            
            ema_series = talib.EMA(super_df['close'], 100)
            ema = ema_series.iloc[-1]

            upperband = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime].iloc[-1]['upperband']
            lowerband = pivot_super_df.iloc[-1]['lowerband']
            entry = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime].iloc[-1]['entry']

            tp = (upperband - entry)
            sl = (entry - lowerband)
            ratio = tp/sl

            prev_percentage = trade_df_short.iloc[-1]['prev_percentage']
            prev_long_percentage = trade_df_long.iloc[-1]['prev_percentage']
            if trade_df_short.iloc[-1]['entry']< ema:
                data['ema'] = 'below'
            else:
                data['ema'] = 'above'
            
            data['coin'] = coin
            data['timeframe'] = timeframe 
            data['time'] = datetime.utcnow()
            data['signal'] = 'buy'
            data['strategy'] = 'long'
            data['rr'] = ratio
            data['inverse_trades_count'] = inverse_trades
            data['long_term_prev_percentage'] = prev_long_percentage
            data['short_term_prev_percentage'] = prev_percentage
            data['long_candle_count'] = candle_count
            data['prev_short_candle_count'] = trade_df_short.iloc[-2]['candle_count']
            data['tp'] = upperband
            data['sl'] = lowerband

            return 1,data
        else:
            return 0,data

    else:
        return 0,data

def is_volatile_tradable(coin,timeframe):
    
    data = {}
    timeframe_mapping = {
     '5m': (2, 15),
    '15m': (3, 15),
    '30m': (7, 9),
    '45m': (10, 6),  # Example values; adjust as needed
    '1h': (14, 4),   # Example values; adjust as needed
    '2h': (20, 2),   # Example values; adjust as needed
    '4h': (30, 1)    # Example values; adjust as needed
}

    look_back_days, candle_count_filter = timeframe_mapping.get(timeframe, (40, 1))
        
    client=Client(config.api_key,config.secret_key)
    str_date = (datetime.now()- timedelta(days=look_back_days)).strftime('%b %d,%Y')
    end_str = (datetime.now() +  timedelta(days=3)).strftime('%b %d,%Y')


    if timeframe in ['1h','2h','4h']:
        #df = dataextract_bybit(coin,str_date,end_str,timeframe)  
        df=dataextract(coin,str_date,end_str,timeframe,client) 
    else:
        df=dataextract(coin,str_date,end_str,timeframe,client)

    #df.to_csv(f'data/{coin}/{coin}_{timeframe}.csv',mode='w+',index=False)

    df= df.iloc[:-1]

    df_copy = df.copy()

    pivot_st = PivotSuperTrendConfiguration()
    pivot_super_df = supertrend_pivot(coin, df_copy, pivot_st.period, pivot_st.atr_multiplier, pivot_st.pivot_period)
    pivot_signal = get_pivot_supertrend_signal(pivot_super_df)
    current_signal_short = pivot_signal
    prev_signal_short = get_prev_pivot_supertrend_signal(pivot_super_df)
    trade_df_short= create_signal_df(pivot_super_df,df,coin,timeframe,pivot_st.atr_multiplier,pivot_st.pivot_period,100,100)

    #long trend
    pivot_st = PivotSuperTrendConfiguration(period = 2, atr_multiplier = 2.6, pivot_period = 2)
    pivot_super_df = supertrend_pivot(coin, df_copy, pivot_st.period, pivot_st.atr_multiplier, pivot_st.pivot_period)
    pivot_signal = get_pivot_supertrend_signal(pivot_super_df)
    current_pivot_signal = pivot_signal
    prev_pivot_signal = get_prev_pivot_supertrend_signal(pivot_super_df)
    super_df = pivot_super_df
    signal_long = get_signal(super_df)
    current_signal_long = signal_long
    prev_signal_long = get_prev_pivot_supertrend_signal(pivot_super_df)
    trade_df_long= create_signal_df(pivot_super_df,df,coin,timeframe,pivot_st.atr_multiplier,pivot_st.pivot_period,100,100)

    candle_count = trade_df_long.iloc[-1]['candle_count']
    if candle_count < candle_count_filter:
        return 0,data
    
    
    #check if tradabale
    if current_signal_long != current_signal_short:
        if current_signal_short != prev_signal_short:
            long_trend_openTime = trade_df_long.iloc[-1]['TradeOpenTime']
            inverse_df_check = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime]
            if current_signal_long == "Buy":
                inverse_trades = inverse_df_check[inverse_df_check['signal']=='Sell'].shape[0]
            else:
                inverse_trades = inverse_df_check[inverse_df_check['signal']=='Buy'].shape[0]

            entry = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime].iloc[-1]['entry']

            if current_signal_long == 'Buy':
                upperband = trade_df_short.iloc[-1]['upperband']
                lowerband = pivot_super_df.iloc[-1]['lowerband']
                tp = (upperband - entry)
                sl = (entry - lowerband)
                data['tp'] = upperband
                data['sl'] = lowerband  
            else:
                upperband = pivot_super_df.iloc[-1]['upperband']
                lowerband = trade_df_short.iloc[-1]['lowerband']
                tp = (entry - lowerband)
                sl = (upperband - entry)
                data['tp'] = lowerband
                data['sl'] = upperband 

            ema_series = talib.EMA(super_df['close'], 100)
            ema = ema_series.iloc[-1]

            ratio = tp/sl
           
            prev_percentage = trade_df_short.iloc[-1]['prev_percentage']
            prev_long_percentage = trade_df_long.iloc[-1]['prev_percentage']

            if current_signal_long == 'Buy':
                data['signal'] = 'buy'
            else:
                data['signal'] = 'sell'

            if trade_df_short.iloc[-1]['entry']< ema:
                data['ema'] ='below'
            else:
                data['ema'] ='above'

            data['coin'] = coin
            data['timeframe'] = timeframe 
            data['time'] = datetime.utcnow()
            data['strategy'] = 'volatile'
            data['rr'] = ratio
            data['inverse_trades_count'] = inverse_trades
            data['long_term_prev_percentage'] = prev_long_percentage
            data['short_term_prev_percentage'] = prev_percentage
            data['long_candle_count'] = candle_count
            data['prev_short_candle_count'] = trade_df_short.iloc[-2]['candle_count']
            


            return 1,data
        else:
            return 0,data

    else:
        return 0,data


def is_short_tradable(coin,timeframe):    

    data = {}
    timeframe_mapping = {
    '5m': (2, 30),
    '15m': (3, 15),
    '30m': (7, 9),
    '45m': (10, 6),  # Example values; adjust as needed
    '1h': (14, 4),   # Example values; adjust as needed
    '2h': (20, 2),   # Example values; adjust as needed
    '4h': (30, 1)    # Example values; adjust as needed
}

    look_back_days, candle_count_filter = timeframe_mapping.get(timeframe, (40, 1))
        
    client=Client(config.api_key,config.secret_key)
    str_date = (datetime.now()- timedelta(days=look_back_days)).strftime('%b %d,%Y')
    end_str = (datetime.now() +  timedelta(days=3)).strftime('%b %d,%Y')

    if timeframe in ['1h','2h','4h']:
        #df = dataextract_bybit(coin,str_date,end_str,timeframe)  
        df=dataextract(coin,str_date,end_str,timeframe,client) 
    else:
        df=dataextract(coin,str_date,end_str,timeframe,client)

    #df.to_csv(f'data/{coin}/{coin}_{timeframe}.csv',mode='w+',index=False)

    df= df.iloc[:-1]

    df_copy = df.copy()

    pivot_st = PivotSuperTrendConfiguration()
    pivot_super_df = supertrend_pivot(coin, df_copy, pivot_st.period, pivot_st.atr_multiplier, pivot_st.pivot_period)
    pivot_signal = get_pivot_supertrend_signal(pivot_super_df)
    current_signal_short = pivot_signal
    prev_signal_short = get_prev_pivot_supertrend_signal(pivot_super_df)
    trade_df_short= create_signal_df(pivot_super_df,df,coin,timeframe,pivot_st.atr_multiplier,pivot_st.pivot_period,100,100)

    #long trend
    pivot_st = PivotSuperTrendConfiguration(period = 2, atr_multiplier = 2.6, pivot_period = 2)
    pivot_super_df = supertrend_pivot(coin, df_copy, pivot_st.period, pivot_st.atr_multiplier, pivot_st.pivot_period)
    pivot_signal = get_pivot_supertrend_signal(pivot_super_df)
    current_pivot_signal = pivot_signal
    prev_pivot_signal = get_prev_pivot_supertrend_signal(pivot_super_df)
    super_df = pivot_super_df
    signal_long = get_signal(super_df)
    current_signal_long = signal_long
    prev_signal_long = get_prev_pivot_supertrend_signal(pivot_super_df)
    trade_df_long= create_signal_df(pivot_super_df,df,coin,timeframe,pivot_st.atr_multiplier,pivot_st.pivot_period,100,100)

    candle_count = trade_df_long.iloc[-1]['candle_count']
    if candle_count < candle_count_filter:
        return False,data
    
    

    if current_signal_long == 'Sell' and current_signal_short == 'Buy':
        if current_signal_short != prev_signal_short:
            long_trend_openTime = trade_df_long.iloc[-1]['TradeOpenTime']
            inverse_df_check = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime]
            inverse_trades = inverse_df_check[inverse_df_check['signal']=='Buy'].shape[0]

            lowerband = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime].iloc[-1]['lowerband']
            upperband = pivot_super_df.iloc[-1]['upperband']

            entry = trade_df_short[trade_df_short['TradeOpenTime'] > long_trend_openTime].iloc[-1]['entry']
            prev_percentage = trade_df_short[trade_df_short['TradeOpenTime'] < long_trend_openTime].iloc[-1]['prev_percentage']

            tp = (entry - lowerband)
            sl = (upperband - entry)
            ratio = tp/sl

            ema_series = talib.EMA(super_df['close'], 100)
            ema = ema_series.iloc[-1]

            if trade_df_short.iloc[-1]['entry']< ema:
                data['ema'] = 'below'
            else:
                data['ema'] = 'above'

            
            prev_percentage = trade_df_short.iloc[-1]['prev_percentage']
            prev_long_percentage = trade_df_long.iloc[-1]['prev_percentage']
            
                
            data['coin'] = coin
            data['timeframe'] = timeframe 
            data['time'] = datetime.utcnow()
            data['signal'] = 'sell'
            data['strategy'] = 'short'
            data['rr'] = ratio
            data['inverse_trades_count'] = inverse_trades
            data['long_term_prev_percentage'] = prev_long_percentage
            data['short_term_prev_percentage'] = prev_percentage
            data['long_candle_count'] = candle_count
            data['prev_short_candle_count'] = trade_df_short.iloc[-2]['candle_count']
            data['tp'] = lowerband
            data['sl'] = upperband

            return 1,data
        else:
            return 0,data

    else:
        return 0,data