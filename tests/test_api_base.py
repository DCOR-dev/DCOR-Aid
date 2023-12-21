import time

from . import common


def test_api_requests_cache_no_parameters():
    """Test the requests_cache for an API call *without* parameters"""
    api = common.get_api()
    t0 = time.perf_counter()
    api.get("status_show")
    t1 = time.perf_counter()
    for ii in range(50):
        api.get("status_show")
    t2 = time.perf_counter()
    assert (t1 - t0) > (t2 - t1)


def test_api_requests_cache_with_parameters():
    """Test the requests_cache for an API call *with* parameters"""
    api = common.get_api()
    t0 = time.perf_counter()
    api.get("organization_list_for_user", permission="create_dataset")
    t1 = time.perf_counter()
    for ii in range(50):
        api.get("organization_list_for_user", permission="create_dataset")
    t2 = time.perf_counter()
    assert (t1 - t0) > (t2 - t1)
