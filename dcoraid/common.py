import hashlib
import pathlib
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
