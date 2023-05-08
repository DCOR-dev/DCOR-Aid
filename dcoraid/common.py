import functools
import hashlib
import pathlib
import weakref

import requests

ConnectionTimeoutErrors = (ConnectionError,
                           requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout)


@functools.lru_cache(maxsize=2000)
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


def weak_lru_cache(maxsize=128, typed=False):
    """LRU Cache decorator that keeps a weak reference to "self""

    https://stackoverflow.com/questions/33672412/
    python-functools-lru-cache-with-instance-methods-release-object
    """
    def wrapper(func):
        @functools.lru_cache(maxsize, typed)
        def _func(_self, *args, **kwargs):
            return func(_self(), *args, **kwargs)

        @functools.wraps(func)
        def inner(self, *args, **kwargs):
            return _func(weakref.ref(self), *args, **kwargs)

        return inner
    return wrapper
