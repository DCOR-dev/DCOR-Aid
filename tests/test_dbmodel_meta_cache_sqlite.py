import pytest
from dcoraid.dbmodel import meta_cache_sqlite

from .common import make_dataset_dict_full_fake


def test_create_write_read(tmp_path):
    db_path = tmp_path / "database.db"

    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict = make_dataset_dict_full_fake()
        # Setting always involves the search glob.
        db[ds_dict["id"]] = (ds_dict, "Kassierer")
        # Fetching only returns the dict.
        ds_dict2 = db[ds_dict["id"]]
        assert ds_dict == ds_dict2

    # Open again
    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict3 = db[ds_dict["id"]]
        assert ds_dict == ds_dict3


def test_create_multiple_and_iter(tmp_path):
    db_path = tmp_path / "database.db"

    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict = make_dataset_dict_full_fake()
        db[ds_dict["id"]] = (ds_dict, "Kassierer")

        for ii in range(10):
            dsi = make_dataset_dict_full_fake()
            db[dsi["id"]] = (dsi, "Kassierer")

        assert db[ds_dict["id"]] == ds_dict

        # iterate over all datasets
        ids = []
        for item in db:
            ids.append(item["id"])
        assert len(list(set(ids))) == 11


def test_pop(tmp_path):
    db_path = tmp_path / "database.db"

    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict = make_dataset_dict_full_fake()
        db[ds_dict["id"]] = (ds_dict, "Kassierer")
        ds_dict2 = db[ds_dict["id"]]
        assert ds_dict == ds_dict2
        db.pop(ds_dict["id"])
        with pytest.raises(KeyError):
            db[ds_dict["id"]]
