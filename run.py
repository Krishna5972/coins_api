from app import create_app,cache
import logging
import os
from app.services.functions import notifier_with_photo,notifier
from app.services.trading import execute_and_get_results,get_first_volatile_coin
import threading
from datetime import datetime,timedelta
logging.disable(logging.CRITICAL + 1)
import time
import warnings
warnings.filterwarnings("ignore")



import asyncio


async def async_execute_and_get_results(timeframe):
    execute_and_get_results(timeframe)
    await asyncio.sleep(0.1)


async def asyn_get_first_volatile_coin(perc):
    get_first_volatile_coin(perc)
    await asyncio.sleep(0.1)

async def background_task():
    print(f'Background task running')
    cache_key_15m = f"execute_and_get_results-15m"
    cache_key_30m = f"execute_and_get_results-30m"
    cache_key_1h = f"execute_and_get_results-1h"
    cache_key_5m = f"execute_and_get_results-5m"

    get_coins_list_cache_key = 'get_coins_list'
    memoize_get_screener_data_cache_key = 'memoize_get_screener_data'

    current_time = datetime.utcnow()
    current_minute = current_time.minute
    current_hour = current_time.hour
    current_second  = current_time.second

    while True:      
        current_time = datetime.utcnow()
        current_minute = current_time.minute
        current_hour = current_time.hour
        current_second  = current_time.second

        sleep_time = 60

        

        if current_minute == 0 :
            cache.delete(get_coins_list_cache_key)
            cache.delete(cache_key_5m)
            cache.delete(cache_key_15m)
            cache.delete(cache_key_30m)
            cache.delete(cache_key_1h)
            notifier('All cache cleared')
            await asyncio.gather(
                async_execute_and_get_results('5m'),
                async_execute_and_get_results('15m'),
                async_execute_and_get_results('30m'),
                async_execute_and_get_results('1h')
            )
            notifier('Calculation is Done for all timeframes')
            time.sleep(sleep_time)


        elif current_minute % 30 == 0:
            cache.delete(get_coins_list_cache_key)
            cache.delete(cache_key_5m)
            cache.delete(cache_key_15m)
            cache.delete(cache_key_30m)
            notifier('15m and 30m cache cleared')
            await asyncio.gather(
                async_execute_and_get_results('5m'),
                async_execute_and_get_results('15m'),
                async_execute_and_get_results('30m'),
            )
            notifier('Calculation is Done for 15m and 30m')
            time.sleep(sleep_time)
        elif current_minute % 15 == 0:
            cache.delete(get_coins_list_cache_key)
            cache.delete(cache_key_15m)
            cache.delete(cache_key_5m)
            await asyncio.gather(
                async_execute_and_get_results('5m'),
                async_execute_and_get_results('15m'),
            )
            notifier('Calculation is Done for 15m')
            time.sleep(sleep_time)

        elif current_minute % 5 == 0:
            cache.delete(get_coins_list_cache_key)
            cache.delete(cache_key_5m)
            execute_and_get_results('5m')
            time.sleep(sleep_time)


        if current_hour == 0 and current_minute == 0 and current_second < 30:
            cache.delete(memoize_get_screener_data_cache_key)
            await asyncio.gather(
                asyn_get_first_volatile_coin(1),
                asyn_get_first_volatile_coin(1.5)
            )

        

        time.sleep(1)
        
    
def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_until_complete(background_task())


app = create_app()


if __name__ == '__main__':
    notifier_with_photo(os.path.join('app', 'data', 'saravanabhava.jpeg'), "SARAVANA BHAVA")

    background_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(background_loop,), daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=5000,debug=True,use_reloader=False)
