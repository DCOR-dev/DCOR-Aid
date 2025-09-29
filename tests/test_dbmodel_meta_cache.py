import time
import uuid

from dcoraid.dbmodel import meta_cache

from .common import make_dataset_dict_full_fake

import pytest


def test_cache_append_to_one_org(tmp_path):
    """Append 100 datasets to a org and make sure they persist"""
    org_id = str(uuid.uuid4())
    with meta_cache.MetaCache(tmp_path) as mc:
        for _ in range(100):
            ds_dict = make_dataset_dict_full_fake(org_id=org_id)
            mc.upsert_dataset(ds_dict)

        # The registry should contain 100 datasets
        assert len(mc._registry_org) == 1
        assert len(mc._registry_org[org_id]) == 100

        assert len(mc.datasets) == 100

    # There should only be one database file
    assert len(list(tmp_path.glob("*.db"))) == 1

    # Try to recover all items from the database
    with meta_cache.MetaCache(tmp_path) as mc:
        # The registry should contain 100 datasets
        assert len(mc._registry_org) == 1
        assert len(mc._registry_org[org_id]) == 100

        # All datasets must be unique
        found_dicts = {}
        for ds_dict in mc._databases[org_id]:
            ds_id = ds_dict["id"]
            assert ds_id in mc._registry_org[org_id]
            assert ds_id not in found_dicts
            found_dicts[ds_id] = ds_dict
            assert mc[ds_id] == ds_dict

            # Test dataset indices
            idx = mc._map_id_index[ds_id]
            assert mc.datasets[idx] == ds_dict
            assert mc._map_index_id[idx] == ds_dict["id"]

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
        assert (tmp_path / f"org_{org_id}.db").is_file()


def test_cache_dataset_index_dict(tmp_path):
    t0 = time.time()
    with meta_cache.MetaCache(tmp_path) as mc:
        ds1 = make_dataset_dict_full_fake(time_created=t0)
        ds2 = make_dataset_dict_full_fake(time_created=t0 - 50)
        ds3 = make_dataset_dict_full_fake(time_created=t0 + 50)
        mc.upsert_dataset(ds1)
        mc.upsert_dataset(ds2)
        mc.upsert_dataset(ds3)

        assert mc._map_id_index[ds1["id"]] == 1
        assert mc._map_id_index[ds2["id"]] == 0
        assert mc._map_id_index[ds3["id"]] == 2


def test_cache_dataset_index_dict_2(tmp_path):
    t0 = time.time()

    ds_list = []
    for ii in range(200):
        ds_list.append(make_dataset_dict_full_fake(time_created=t0 + ii))

    # sort dataset list according to ID
    ds_list_edit = sorted(ds_list, key=lambda x: x["id"])
    assert ds_list_edit != ds_list

    with meta_cache.MetaCache(tmp_path) as mc:
        for ds_dict in ds_list_edit:
            mc.upsert_dataset(ds_dict)

        for ii in range(200):
            assert mc._map_id_index[ds_list[ii]["id"]] == ii

        for ds1, ds2 in zip(mc.datasets, ds_list):
            assert ds1 == ds2


def test_cache_dataset_index_dict_2_multi(tmp_path):
    t0 = time.time()
    org_id = str(uuid.uuid4())

    ds_list = []
    for ii in range(200):
        ds_list.append(make_dataset_dict_full_fake(time_created=t0 - ii,
                                                   org_id=org_id))

    # sort dataset list according to ID
    ds_list_edit = sorted(ds_list, key=lambda x: x["id"])
    assert ds_list_edit != ds_list

    with meta_cache.MetaCache(tmp_path) as mc:
        mc._upsert_many_insert(org_id, ds_list_edit)

        for ii in range(200):
            assert mc._map_id_index[ds_list[ii]["id"]] == ii

        for ds1, ds2 in zip(mc.datasets, ds_list):
            assert ds1 == ds2


def test_cache_datasets_user_owned(tmp_path):
    user_id = str(uuid.uuid4())
    ds_user = []
    ds_other = []
    with meta_cache.MetaCache(tmp_path, user_id=user_id) as mc:
        # single datasets
        for ii in range(10):
            ds_dict = make_dataset_dict_full_fake(user_id=user_id)
            ds_user.append(ds_dict)
            mc.upsert_dataset(ds_dict)
        for ii in range(10):
            ds_dict = make_dataset_dict_full_fake()
            ds_other.append(ds_dict)
            mc.upsert_dataset(ds_dict)

        # Make sure that the user dictionaries are stored correctly
        for ds in ds_user:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert mc.datasets_user_owned[idx]

        for ds in ds_other:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert not mc.datasets_user_owned[idx]

    with meta_cache.MetaCache(tmp_path, user_id=user_id) as mc:
        # Make sure that the user dictionaries are recovered correctly
        for ds in ds_user:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert mc.datasets_user_owned[idx]

        for ds in ds_other:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert not mc.datasets_user_owned[idx]


def test_cache_datasets_user_owned_full(tmp_path):
    user_id = str(uuid.uuid4())
    ds_user = []
    ds_other = []
    with meta_cache.MetaCache(tmp_path, user_id=user_id) as mc:
        # single datasets
        for ii in range(10):
            ds_dict = make_dataset_dict_full_fake(user_id=user_id)
            ds_user.append(ds_dict)
            mc.upsert_dataset(ds_dict)
        for ii in range(10):
            ds_dict = make_dataset_dict_full_fake()
            ds_other.append(ds_dict)
            mc.upsert_dataset(ds_dict)

        # multiple datasets
        org_id = str(uuid.uuid4())
        ds_dicts_a = [
            make_dataset_dict_full_fake(org_id=org_id, user_id=user_id)
            for _ in range(10)
            ]
        ds_user += ds_dicts_a
        mc._upsert_many_insert(org_id, ds_dicts_a)
        ds_dicts_b = [
            make_dataset_dict_full_fake(org_id=org_id)
            for _ in range(10)
            ]
        mc._upsert_many_insert(org_id, ds_dicts_b)
        ds_other += ds_dicts_b

        # Make sure that the user dictionaries are stored correctly
        for ds in ds_user:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert mc.datasets_user_owned[idx]

        for ds in ds_other:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert not mc.datasets_user_owned[idx]

    with meta_cache.MetaCache(tmp_path, user_id=user_id) as mc:
        # Make sure that the user dictionaries are recovered correctly
        for ds in ds_user:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert mc.datasets_user_owned[idx]

        for ds in ds_other:
            idx = mc._map_id_index[ds["id"]]
            assert mc.datasets[idx] == ds
            assert not mc.datasets_user_owned[idx]


def test_cache_upsert_many(tmp_path):
    """Test the `upsert_many` wrapper"""
    ds_list = []

    org_id = str(uuid.uuid4())

    # create datasets in ascending order (order must be reversed in MetaCache)
    for ii in range(15):
        ds_list.append(make_dataset_dict_full_fake(
            org_id=org_id,
            time_created=time.time() - 100 + ii,
            ))

    with meta_cache.MetaCache(tmp_path) as mc:
        mc.upsert_many(ds_list, org_id=org_id)

        # make sure datasets are in descending order
        assert mc.datasets == ds_list[::-1]

    with meta_cache.MetaCache(tmp_path) as mc:
        assert mc.datasets == ds_list[::-1]

        ds_list[0]["title"] = "Peter Pan"
        ds_list[1]["title"] = "Peter Pan 3"
        ds_list[-1]["title"] = "Peter Pan 2"

        mc.upsert_many(ds_list, org_id=org_id)

        assert mc.datasets == ds_list[::-1]
        # descending order again
        assert mc.datasets[-1]["title"] == "Peter Pan"
        assert mc.datasets[0]["title"] == "Peter Pan 2"
        assert mc.datasets[-2]["title"] == "Peter Pan 3"


def test_cache_upsert_many_search(tmp_path):
    """Test the `upsert_many` wrapper, searching data afterwards"""
    ds_list = []

    org_id = str(uuid.uuid4())

    # create datasets in ascending order (order must be reversed in MetaCache)
    for ii in range(15):
        ds_list.append(make_dataset_dict_full_fake(
            org_id=org_id,
            time_created=time.time() - 100 + ii,
            ))

    ds_list.append(make_dataset_dict_full_fake(title="Mordor",
                                               org_id=org_id,
                                               time_created=time.time() - 98,
                                               ))

    org_id_2 = str(uuid.uuid4())

    ds_list.append(make_dataset_dict_full_fake(title="Frodo",
                                               org_id=org_id_2,
                                               time_created=time.time() - 98,
                                               ))

    for ii in range(18):
        ds_list.append(make_dataset_dict_full_fake(
            org_id=org_id_2,
            time_created=time.time() - 100 + ii,
            ))

    with meta_cache.MetaCache(tmp_path) as mc:
        mc.upsert_many(ds_list)
        # search for them in freshly-modified cache
        assert mc.search(query="Frodo")[0]["title"] == "Frodo"
        assert mc.search(query="Mordor")[0]["title"] == "Mordor"

    with meta_cache.MetaCache(tmp_path) as mc:
        # search for them in loaded cache
        assert mc.search(query="Frodo")[0]["title"] == "Frodo"
        assert mc.search(query="Mordor")[0]["title"] == "Mordor"


@pytest.mark.parametrize("previous_datasets", [0, 10, 100])
def test_cache_upsert_many_insert(tmp_path, previous_datasets):
    # generate datasets
    all_ds_dicts = []
    org_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    for org_id in org_ids:
        ds_dicts = []
        for ii in range(15):
            ds_dicts.append(make_dataset_dict_full_fake(org_id=org_id))
        all_ds_dicts.append(ds_dicts)

    # populate cache
    with meta_cache.MetaCache(tmp_path) as mc:
        for ii in range(previous_datasets):
            # Before inserting many datasets for the two organizations,
            # add `previous_datasets` other datasets that each will have
            # its own unique org.
            mc.upsert_dataset(make_dataset_dict_full_fake())

        for (org_id, ds_dicts) in zip(org_ids, all_ds_dicts):
            mc._upsert_many_insert(org_id, ds_dicts)

    # Check whether the databases exist
    for org_id in org_ids:
        assert (tmp_path / f"org_{org_id}.db").is_file()

    # Check a few other things
    with meta_cache.MetaCache(tmp_path) as mc:
        # The registry should contain 100 datasets
        assert len(mc._registry_org) == 2 + previous_datasets

        assert len(mc.datasets) == 30 + previous_datasets

        # All datasets must be unique
        found_dicts = {}
        for org_id in org_ids:
            assert len(mc._registry_org[org_id]) == 15
            for ds_dict in mc._databases[org_id]:
                ds_id = ds_dict["id"]
                assert ds_id in mc._registry_org[org_id]
                assert ds_id not in found_dicts
                found_dicts[ds_id] = ds_dict
                assert mc[ds_id] == ds_dict

                # Test dataset indices
                idx = mc._map_id_index[ds_id]
                assert mc.datasets[idx] == ds_dict
                assert mc._map_index_id[idx] == ds_dict["id"]

    # check whether every single dataset exists
    with meta_cache.MetaCache(tmp_path) as mc:
        for ds_dicts in all_ds_dicts:
            for ds_dict in ds_dicts:
                assert mc[ds_dict["id"]] == ds_dict


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
    assert list(meta_cache._values_only(input,
                                        only_keys=["peter", "rin"])) == output


def test_create_blob_for_search():
    ds_dict = make_dataset_dict_full_fake()
    blob = meta_cache._create_blob_for_search(ds_dict)

    assert isinstance(blob, str)
    assert ds_dict["id"] in blob
    assert ds_dict["owner_org"] in blob
    assert not ds_dict["authors"] in blob
    assert ds_dict["authors"].lower() in blob
