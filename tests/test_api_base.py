import time

import pytest

from dcoraid.api import ckan_api

from . import common


def test_api_no_maintenance():
    """Normally, the DCOR instance should not be under maintenance"""
    api = common.get_api()
    assert not api.is_under_maintenance()


def test_api_requests_cache_no_parameters():
    """Test the requests_cache for an API call *without* parameters"""
    api = common.get_api()
    t0 = time.perf_counter()
    api.get("status_show")
    t1 = time.perf_counter()
    for ii in range(20):
        api.get("status_show")
    t2 = time.perf_counter()
    assert (t1 - t0) > (t2 - t1)


def test_api_requests_cache_with_parameters():
    """Test the requests_cache for an API call *with* parameters"""
    api = common.get_api()
    t0 = time.perf_counter()
    api.get("organization_list_for_user", permission="create_dataset")
    t1 = time.perf_counter()
    for ii in range(20):
        api.get("organization_list_for_user", permission="create_dataset")
    t2 = time.perf_counter()
    assert (t1 - t0) > (t2 - t1)


@pytest.mark.parametrize("server,api_server", [
    ("http://localhost:5000", "http://localhost:5000"),
    ("https://localhost:5000", "https://localhost:5000"),
    ("localhost:5000", "https://localhost:5000"),
])
def test_api_server_name(server, api_server):
    api = ckan_api.CKANAPI(server=server,
                           check_ckan_version=False)
    assert api.server == api_server
    assert api.api_url.startswith(api_server)
