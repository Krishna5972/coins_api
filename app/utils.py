from functools import wraps
from . import cache

def custom_memoize(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Choose the cache timeout based on the first argument
        if args[0] == '15m':
            timeout = 14 * 60  # 15 minutes
        elif args[0] == '30m':
            timeout = 29 * 60  # 30 minutes
        elif args[0] == '1h':
            timeout = 59 * 60  # 1 hour
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
