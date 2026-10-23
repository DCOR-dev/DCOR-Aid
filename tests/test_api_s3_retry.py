"""Offline tests for retrying S3 uploads"""
import hashlib
import io
from unittest import mock

import pytest
import requests

from dcoraid.api import s3_api
from dcoraid.api.errors import S3UploadError

DATA = b"Hello S3!" * 1000
MD5 = hashlib.md5(DATA).hexdigest()


def make_response(etag):
    resp = mock.MagicMock()
    resp.headers = {"ETag": f'"{etag}"'}
    resp.content = b""
    return resp


def put_part(**kwargs):
    return s3_api.requests_put_data_and_get_etag(
        file_object=io.BytesIO(DATA),
        put_url="https://objectstore.example.com/bucket/object",
        part_number=0,
        part_size=len(DATA),
        file_size=len(DATA),
        callback=None,
        **kwargs)


def test_wait_before_retry():
    with mock.patch.object(s3_api.time, "sleep") as sleep:
        for attempt in range(4):
            s3_api.wait_before_retry(attempt, attempts=4, retry_wait=5)
    # no waiting after the last attempt
    assert [c.args[0] for c in sleep.call_args_list] == [5, 10, 20]


def test_put_retry_after_refused_connection():
    """Connection problems (e.g. refused connections) are retried later"""
    refused = requests.exceptions.ConnectionError("connection refused")
    with mock.patch.object(s3_api.requests, "put",
                           side_effect=[refused, refused,
                                        make_response(MD5)]) as put, \
            mock.patch.object(s3_api.time, "sleep") as sleep:
        etag = put_part(retries=3)
    assert etag == MD5
    assert put.call_count == 3
    assert [c.args[0] for c in sleep.call_args_list] == [5, 10]


def test_put_retry_after_etag_mismatch():
    with mock.patch.object(s3_api.requests, "put",
                           side_effect=[make_response("0" * 32),
                                        make_response(MD5)]) as put, \
            mock.patch.object(s3_api.time, "sleep") as sleep:
        etag = put_part(retries=3, retry_wait=2)
    assert etag == MD5
    assert put.call_count == 2
    assert [c.args[0] for c in sleep.call_args_list] == [2]


def test_put_retries_exhausted():
    refused = requests.exceptions.ConnectionError("connection refused")
    with mock.patch.object(s3_api.requests, "put",
                           side_effect=refused) as put, \
            mock.patch.object(s3_api.time, "sleep") as sleep, \
            pytest.raises(S3UploadError, match="connection refused"):
        put_part(retries=3)
    assert put.call_count == 3
    # no waiting after the last attempt
    assert [c.args[0] for c in sleep.call_args_list] == [5, 10]


def test_put_no_wait_on_success():
    with mock.patch.object(s3_api.requests, "put",
                           return_value=make_response(MD5)), \
            mock.patch.object(s3_api.time, "sleep") as sleep:
        assert put_part() == MD5
    sleep.assert_not_called()
