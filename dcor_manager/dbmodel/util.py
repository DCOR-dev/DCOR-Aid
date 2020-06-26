import functools
import time


def ttl_cache(seconds=5, maxsize=32, typed=False):
    """A time-to-live cache based on lru_cache"""
    def wrapper_cache(func):
        func = functools.lru_cache(maxsize=maxsize, typed=typed)(func)
        func.tinit = time.perf_counter()
        func.delta = 0

        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            delta = (time.perf_counter() - func.tinit) // seconds
            if delta != func.delta:
                func.cache_clear()
                func.delta = delta
            return func(*args, **kwargs)
        return wrapped_func

    return wrapper_cache
