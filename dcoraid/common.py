import functools
import hashlib
import pathlib
import weakref

import requests

ConnectionTimeoutErrors = (ConnectionError,
                           requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout)


@functools.lru_cache(maxsize=2000)
def etagsum(path):
    """Compute the ETag for a file

    The ETag of a resource on DCOR is defined by the way data
    are uploaded to S3. Upload to S3 is done in chunks, the
    number of which are defined by the size of the file.

    The ETag can be computed from the MD5 sum of the MD5 sums [sic]
    of the individual upload parts, followed by a dash "-" and the
    number of upload parts.

    The code for generating the upload URLs can be found at
    :func:`dcor_shared.s3.create_presigned_upload_urls`.
    """
    gib = 1024**3
    mib = 1024**2
    path = pathlib.Path(path)
    file_size = path.stat().st_size

    if file_size % gib == 0:
        num_parts = file_size // gib
    else:
        num_parts = file_size // gib + 1

    # Compute the MD5 sums of the individual upload parts.
    md5_sums = []
    with path.open("rb") as fd:
        for ii in range(num_parts):
            cur_md5 = hashlib.md5()
            for jj in range(1024):  # 1GB chunk = 1024 * 1MB chunk
                data = fd.read(mib)
                if not data:
                    break
                cur_md5.update(data)
            md5_sums.append(cur_md5.hexdigest())

    if len(md5_sums) == 1:
        etag = md5_sums[0]
    else:
        # Combine the MD5 sums into the ETag
        hasher = hashlib.md5()
        for etag_part in md5_sums:
            etag_binary = int(etag_part, 16).to_bytes(length=16,
                                                      byteorder="big")
            hasher.update(etag_binary)
        etag = f"{hasher.hexdigest()}-{len(md5_sums)}"

    return etag


@functools.lru_cache(maxsize=2000)
def sha256sum(path):
    """Compute the SHA256 hash of a file"""
    mib = 1024 ** 2
    file_hash = hashlib.sha256()
    with open(path, "rb") as fd:
        while data := fd.read(mib):
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
