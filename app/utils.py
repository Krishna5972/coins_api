from functools import wraps
from . import cache
from datetime import datetime

def custom_memoize(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Choose the cache timeout based on the first argument

        current_minute = datetime.utcnow().minute

        if args[0] == '15m':
            timeout = 14.9 * 60  # 15 minutes
            if current_minute % 15 == 0:
                cache.delete(f"{f.__name__}-15m")
        elif args[0] == '30m':
            timeout = 29.9 * 60  # 30 minutes
            if current_minute % 30 == 0:
                cache.delete(f"{f.__name__}-30m")
        elif args[0] == '1h':
            timeout = 59 * 60  # 1 hour
            if current_minute == 0:
                cache.delete(f"{f.__name__}-1h")
        elif args[0] == '5m':
            timeout = 4.9 * 60  # 5m
            if current_minute == 0:
                cache.delete(f"{f.__name__}-5m")
        else:
            timeout = None  # Default

        # Generate a unique cache key based on function name and arguments
        cache_key = f"{f.__name__}-{args[0]}"


        # Check if result is in cache
        result = cache.get(cache_key)
        if result is not None:
            return result

        # If not in cache, compute and then cache the result
        result = f(*args, **kwargs)
        cache.set(cache_key, result, timeout=timeout)
        return result

    return decorated_function


def custom_memoize_get_coins(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Choose the cache timeout based on the first argument
        timeout = 840

        cache_key = f"{f.__name__}"

        result = cache.get(cache_key)
        if result is not None:
            return result

        # If not in cache, compute and then cache the result
        result = f(*args, **kwargs)
        cache.set(cache_key, result, timeout=timeout)
        return result

    return decorated_function


def custom_memoize_first_volatile(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):

        current_time = datetime.utcnow()
       
        current_minute = current_time.minute
        current_hour = current_time.hour
        current_second  = current_time.second


        cache_key = f"{f.__name__}-{args[0]}"  #custom_memoize_first_volatile-1

        if current_hour == 0 and current_minute == 0 and current_second < 30:
            cache.delete(f"{f.__name__}-{args[0]}")

        result = cache.get(cache_key)
        if result is not None:
            return result

        # If not in cache, compute and then cache the result
        result = f(*args, **kwargs)
        cache.set(cache_key, result)
        return result
    return decorated_function



def memoize_get_screener_data(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Choose the cache timeout based on the first argument
        timeout = 180

        cache_key = f"{f.__name__}"  #memoize_get_screener_data

        result = cache.get(cache_key)
        if result is not None:
            return result

        # If not in cache, compute and then cache the result
        result = f(*args, **kwargs)
        cache.set(cache_key, result, timeout=timeout)
        return result

    return decorated_function