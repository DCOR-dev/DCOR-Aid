import pytest

from dcoraid.dbmodel.meta_cache_datasets import SQLiteBackedROListOfDicts


def make_list(ds_ids):
    """Return a SQLiteBackedROListOfDicts backed by plain dictionaries"""
    sqlite_dbs = {"org": {ds_id: {"id": ds_id} for ds_id in ds_ids}}
    map_id_org = {ds_id: "org" for ds_id in ds_ids}
    return SQLiteBackedROListOfDicts(sqlite_dbs=sqlite_dbs,
                                     map_index_id=list(ds_ids),
                                     map_id_org=map_id_org)


def test_eq_equal():
    assert make_list(["a", "b"]) == [{"id": "a"}, {"id": "b"}]
    assert make_list([]) == []


def test_eq_different_item():
    assert make_list(["a", "b"]) != [{"id": "a"}, {"id": "c"}]


def test_eq_other_shorter():
    assert make_list(["a", "b"]) != [{"id": "a"}]
    assert make_list(["a", "b"]) != []


def test_eq_other_longer():
    assert make_list(["a"]) != [{"id": "a"}, {"id": "b"}]
    assert make_list([]) != [{"id": "a"}]


def test_eq_not_a_list():
    with pytest.raises(NotImplementedError):
        make_list(["a"]) == ({"id": "a"},)  # noqa: B015
