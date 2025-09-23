import uuid

from dcoraid.dbmodel import meta_cache

from .common import make_dataset_dict_full_fake

import pytest


def test_cache_append_to_one_circle(tmp_path):
    """Append 100 datasets to a circle and make sure they persist"""
    org_id = str(uuid.uuid4())
    with meta_cache.MetaCache(tmp_path) as mc:
        for _ in range(100):
            ds_dict = make_dataset_dict_full_fake(org_id=org_id)
            mc.upsert_dataset(ds_dict)

        # The registry should contain 100 datasets
        assert len(mc._registry_org) == 1
        assert len(mc._registry_org[org_id]) == 100

        # The search store should contain 100 entries
        assert len(mc._srt_blobs) == 100

        assert len(mc.datasets) == 100

    # There should only be one database file
    assert len(list(tmp_path.glob("*.db"))) == 1

    # Try to recover all items from the database
    with meta_cache.MetaCache(tmp_path) as mc:
        # The registry should contain 100 datasets
        assert len(mc._registry_org) == 1
        assert len(mc._registry_org[org_id]) == 100

        # The search store should contain 100 entries
        assert len(mc._srt_blobs) == 100

        # All datasets must be unique
        found_dicts = {}
        for ds_dict in mc._databases[org_id]:
            ds_id = ds_dict["id"]
            assert ds_id in mc._registry_org[org_id]
            assert ds_id not in found_dicts
            found_dicts[ds_id] = ds_dict
            assert mc[ds_id] == ds_dict

            # Test dataset indices
            idx = mc._dataset_index_dict[ds_id]
            assert mc.datasets[idx] == ds_dict
            assert mc._dataset_ids[idx] == ds_dict["id"]

        assert len(mc.datasets) == 100


def test_cache_creates_databases(tmp_path):
    org_ids = []
    with meta_cache.MetaCache(tmp_path) as mc:
        for _ in range(5):
            ds_dict = make_dataset_dict_full_fake()
            org_ids.append(ds_dict['owner_org'])
            mc.upsert_dataset(ds_dict)

    # Check whether the databases exist
    for org_id in org_ids:
        assert (tmp_path / f"circle_{org_id}.db").is_file()


def test_cache_search(tmp_path):
    """Simple dataset search"""
    with meta_cache.MetaCache(tmp_path) as mc:
        for _ in range(5):
            mc.upsert_dataset(make_dataset_dict_full_fake())
        # create a dataset for which we are going to search
        ds_dict = make_dataset_dict_full_fake(title="fertig-nein-hann-solo")
        mc.upsert_dataset(ds_dict)

        # Perform search
        ds_list = mc.search("fertig-nein-hann-solo")
        assert len(ds_list) == 1
        assert ds_list[0] == ds_dict

    # Same thing with stored databases
    with meta_cache.MetaCache(tmp_path) as mc:
        ds_list = mc.search("fertig-nein-hann-solo")
        assert len(ds_list) == 1
        assert ds_list[0] == ds_dict


def test_cache_search_blob_size_increases(tmp_path):
    with meta_cache.MetaCache(tmp_path) as mc:
        mc.upsert_dataset(make_dataset_dict_full_fake(title="1" * 300))
        start_size = int(mc._srt_blobs.dtype["blob"].str[2:])

        # insert a dataset
        mc.upsert_dataset(make_dataset_dict_full_fake(title="1" * 400))
        insert_size = int(mc._srt_blobs.dtype["blob"].str[2:])
        assert start_size + 100 == insert_size

        # update a dataset
        ds_dict = make_dataset_dict_full_fake()
        mc.upsert_dataset(ds_dict)
        ds_dict["title"] = "1" * 500
        mc.upsert_dataset(ds_dict)
        update_size = int(mc._srt_blobs.dtype["blob"].str[2:])
        assert start_size + 200 == update_size


def test_cache_search_multiple_results(tmp_path):
    """Simple dataset search"""
    with meta_cache.MetaCache(tmp_path) as mc:
        for _ in range(5):
            mc.upsert_dataset(make_dataset_dict_full_fake())
        # create a dataset for which we are going to search
        mc.upsert_dataset(make_dataset_dict_full_fake(title="bububuMA"))
        mc.upsert_dataset(make_dataset_dict_full_fake(title="bububuPA"))

        # Perform search
        ds_list = mc.search("bububu")
        assert len(ds_list) == 2

    # Same thing with stored databases
    with meta_cache.MetaCache(tmp_path) as mc:
        ds_list = mc.search("bububu")
        assert len(ds_list) == 2


def test_cache_search_multiple_results_limit(tmp_path):
    """Simple dataset search"""
    with meta_cache.MetaCache(tmp_path) as mc:
        for _ in range(5):
            mc.upsert_dataset(make_dataset_dict_full_fake())
        # create a dataset for which we are going to search
        for _ in range(5):
            mc.upsert_dataset(make_dataset_dict_full_fake(title="bububuMA"))

        # Perform search
        ds_list = mc.search("bububu", limit=3)
        assert len(ds_list) == 3

        ds_list = mc.search("bababa", limit=3)
        assert len(ds_list) == 0

    # Same thing with stored databases
    with meta_cache.MetaCache(tmp_path) as mc:
        ds_list = mc.search("bububu", limit=3)
        assert len(ds_list) == 3

        ds_list = mc.search("bababa", limit=3)
        assert len(ds_list) == 0


def test_cache_search_updated_dataset_case(tmp_path):
    with meta_cache.MetaCache(tmp_path) as mc:
        for _ in range(5):
            mc.upsert_dataset(make_dataset_dict_full_fake())
        # create a dataset for which we are going to search
        ds_dict = make_dataset_dict_full_fake(title="fertig-nein-hann-solo")
        mc.upsert_dataset(ds_dict)

        # update the dataset
        ds_dict["title"] = "Filst-fu-fit-fir-fuscheln"
        mc.upsert_dataset(ds_dict)

        assert ds_dict in mc.datasets

        ds_list = mc.search("fertig-nein-hann-solo")
        assert len(ds_list) == 0

        # case-insensitive search
        ds_list = mc.search("FILST-fu-fit-fir-fuscheln")
        assert len(ds_list) == 1
        assert ds_list[0] == ds_dict

    # Same thing with stored databases
    with meta_cache.MetaCache(tmp_path) as mc:
        assert ds_dict in mc.datasets

        ds_list = mc.search("fertig-nein-hann-solo")
        assert len(ds_list) == 0

        # case-insensitive search
        ds_list = mc.search("FILST-fu-fit-fir-fuscheln")
        assert len(ds_list) == 1
        assert ds_list[0] == ds_dict


@pytest.mark.parametrize("input,output", [
    [["peter", "hans"], ["peter", "hans"]],
    [{"peter": "hans"}, ["hans"]],
    [[2, {"rin": "dop"}, [["sinn"]]], ["2", "dop", "sinn"]],
    [[2, {"other-key": "dop"}, [["sinn"]]], ["2", "sinn"]],
    ])
def test_values_only(input, output):
    assert meta_cache._values_only(input, only_keys=["peter", "rin"]) == output


def test_create_blob_for_search():
    ds_dict = make_dataset_dict_full_fake()
    blob = meta_cache._create_blob_for_search(ds_dict)

    assert isinstance(blob, str)
    assert ds_dict["id"] in blob
    assert ds_dict["owner_org"] in blob
    assert not ds_dict["authors"] in blob
    assert ds_dict["authors"].lower() in blob
