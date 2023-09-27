from app import create_app,cache
import logging
import os
from app.services.functions import notifier_with_photo,notifier
from app.services.trading import execute_and_get_results
import threading
from datetime import datetime,timedelta
logging.disable(logging.CRITICAL + 1)
import time


import asyncio

async def async_execute_and_get_results(timeframe):
    execute_and_get_results(timeframe)
    await asyncio.sleep(0.1)

async def background_task():
    cache_key_15m = f"execute_and_get_results-15m"
    cache_key_30m = f"execute_and_get_results-30m"
    cache_key_1h = f"execute_and_get_results-1h"
    get_coins_list_cache_key = 'get_coins_list'

    while True:      
        current_time = datetime.utcnow()
        current_minute = current_time.minute

        sleep_time = 60

        if current_minute == 0 :
            cache.delete(get_coins_list_cache_key)
            cache.delete(cache_key_15m)
            cache.delete(cache_key_30m)
            cache.delete(cache_key_1h)
            notifier('All cache cleared')
            await asyncio.gather(
                async_execute_and_get_results('15m'),
                async_execute_and_get_results('30m'),
                async_execute_and_get_results('1h')
            )
            time.sleep(sleep_time)

            
        elif current_minute % 30 == 0:
            cache.delete(get_coins_list_cache_key)
            cache.delete(cache_key_15m)
            cache.delete(cache_key_30m)
            notifier('15m and 30m cache cleared')
            await asyncio.gather(
                async_execute_and_get_results('15m'),
                async_execute_and_get_results('30m'),
            )
            time.sleep(sleep_time)
        elif current_minute % 15 == 0:
            cache.delete(get_coins_list_cache_key)
            cache.delete(cache_key_15m)
            execute_and_get_results('15m')
            notifier('15m cache cleared')
            time.sleep(sleep_time)

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
    app.run(debug=True,use_reloader=False)
