import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.trading import *



def check_longs(possible_long,timeframe):
    for coin in possible_long:
        try:
            value,data = is_long_tradable(coin, timeframe)
            if value:   
                print(data)

        except Exception as e:
            print(f'{coin} error', e)

possible_long =['ARK']
timeframe = '1h'

print('hi')

check_longs(possible_long,timeframe)


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
            data['time'] = datetime.utcnow()
            data['sigal'] = 'buy'
            data['strategy'] = 'long'
            data['rr'] = ratio
            data['inverse_trades_count'] = inverse_trades
            data['long_term_prev_percentage'] = prev_long_percentage
            data['short_term_prev_percentage'] = prev_percentage
            data['long_candle_count'] = candle_count
            data['prev_short_candle_count'] = trade_df_short.iloc[-1]['candle_count']
            data['tp'] = tp
            data['sl'] = sl

            return 0,data
        else:
            return 0,data

    else:
        return 0,data
