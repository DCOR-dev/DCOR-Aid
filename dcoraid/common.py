import functools
import hashlib
import pathlib
import time
from functools import lru_cache

import requests

ConnectionTimeoutErrors = (ConnectionError,
                           requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout)


@lru_cache(maxsize=2000)
def sha256sum(path):
    """Compute the SHA256 sum of a file on disk"""
    block_size = 2**20
    path = pathlib.Path(path)
    file_hash = hashlib.sha256()
    with path.open("rb") as fd:
        while True:
            data = fd.read(block_size)
            if not data:
                break
            file_hash.update(data)
    return file_hash.hexdigest()


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
