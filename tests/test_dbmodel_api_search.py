"""Offline tests for the pagination in `search_dataset_via_api`"""
from unittest import mock

import pytest

from dcoraid.dbmodel import db_api


def make_interrogator(num_total, delete_after=None):
    """Return an APIInterrogator with a mocked CKAN `package_search`

    Parameters
    ----------
    num_total: int
        Number of datasets matching the search query
    delete_after: int
        Simulate datasets being deleted in-between requests by
        returning no results beyond this index
    """
    requests = []

    def get(api_call, rows, start, **kwargs):
        assert api_call == "package_search"
        # CKAN does not return more than 1000 rows per request
        assert 0 < rows <= 1000
        requests.append((start, rows))
        stop = min(start + rows, num_total)
        if delete_after is not None:
            stop = min(stop, delete_after)
        return {"count": num_total,
                "results": [{"id": f"{ii}"} for ii in range(start, stop)]}

    api = mock.MagicMock()
    api.user_id = None
    api.copy.return_value = api
    api.get.side_effect = get
    return db_api.APIInterrogator(api), requests


@pytest.mark.parametrize("limit,num_total,start,num_expected", [
    # limits below the CKAN maximum of 1000 rows
    [100, 3000, 0, 100],
    [500, 3000, 0, 500],
    [1000, 3000, 0, 1000],
    # limits above the CKAN maximum (used to return too many results)
    [1500, 3000, 0, 1500],
    [2500, 5000, 0, 2500],
    [2000, 2000, 0, 2000],
    # fewer datasets available than requested
    [1500, 1200, 0, 1200],
    [100, 50, 0, 50],
    [100, 0, 0, 0],
    # get all datasets
    [0, 2500, 0, 2500],
    [0, 1000, 0, 1000],
    [0, 0, 0, 0],
    # with an offset
    [1500, 3000, 1000, 1500],
    [1500, 3000, 2000, 1000],
    [100, 3000, 2950, 50],
    [0, 3000, 500, 2500],
    [100, 3000, 3000, 0],
    [100, 3000, 4000, 0],
])
def test_search_limit(limit, num_total, start, num_expected):
    ai, requests = make_interrogator(num_total)
    data = ai.search_dataset_via_api(limit=limit,
                                     start=start,
                                     ret_db_extract=False)
    assert len(data) == num_expected
    # results are contiguous, in order, and start at `start`
    assert [d["id"] for d in data] == \
        [f"{ii}" for ii in range(start, start + num_expected)]
    # no superfluous requests (at most one when there are no results)
    assert len(requests) == max(1, -(-num_expected // 1000))


def test_search_datasets_deleted_in_between():
    """Do not loop forever when the server returns fewer datasets"""
    ai, requests = make_interrogator(num_total=2500, delete_after=1200)
    data = ai.search_dataset_via_api(limit=0, ret_db_extract=False)
    assert len(data) == 1200
    assert len(requests) == 3


def test_search_negative_limit():
    ai, _ = make_interrogator(num_total=10)
    with pytest.raises(ValueError, match="must be 0 or >0"):
        ai.search_dataset_via_api(limit=-1)
