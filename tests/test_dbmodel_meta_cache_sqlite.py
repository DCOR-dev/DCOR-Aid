import uuid

import pytest
from dcoraid.dbmodel import meta_cache_sqlite


def make_example_ds_dict():
    org_id = f"{uuid.uuid4()}"
    ds_id = f"{uuid.uuid4()}"
    user_id = f"{uuid.uuid4()}"
    return {"authors": "John Doe",
            "creator_user_id": f"{user_id}",
            "doi": "",
            "id": f"{ds_id}",
            "isopen": True,
            "license_id": "CC-BY-SA-4.0",
            "license_title": "Creative Commons Attribution Share-Alike 4.0",
            "license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
            "metadata_created": "2025-09-11T08:09:52.947634",
            "metadata_modified": "2025-09-11T08:10:13.882541",
            "name": "an-example-dataset",
            "notes": "A description",
            "num_resources": 1, "num_tags": 2,
            "organization": {
                "id": f"{org_id}",
                "name": "dcoraid-circle", "title": "",
                "type": "organization",
                "description": "", "image_url": "",
                "created": "2020-09-23T09:50:44.826360",
                "is_organization": True,
                "approval_status": "approved",
                "state": "active"},
            "owner_org": f"{org_id}",
            "private": True, "references": "",
            "state": "active", "title": "Dataset Title",
            "type": "dataset", "resources": [
                {"cache_last_updated": None, "cache_url": None,
                 "created": "2025-09-18T08:10:06.286171",
                 "dc:experiment:date": "2018-12-11",
                 "dc:experiment:event count": 47, "dc:experiment:run index": 1,
                 "url_type": "s3_upload"}],
            "tags": [{"display_name": "GFP",
                      "id": "5b5e7553-49a0-49b4-b342-28b8384800c0",
                      "name": "GFP",
                      "state": "active",
                      "vocabulary_id": None},
                     {"display_name": "HL60",
                      "id": "f4e18050-d2dc-4fd2-93d4-015666f0e066",
                      "name": "HL60",
                      "state": "active",
                      "vocabulary_id": None}],
            "groups": [],
            "relationships_as_subject": [],
            "relationships_as_object": []}


def test_create_write_read(tmp_path):
    db_path = tmp_path / "database.db"

    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict = make_example_ds_dict()
        db[ds_dict["id"]] = ds_dict
        ds_dict2 = db[ds_dict["id"]]
        assert ds_dict == ds_dict2

    # Open again
    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict3 = db[ds_dict["id"]]
        assert ds_dict == ds_dict3



def test_create_multiple_and_iter(tmp_path):
    db_path = tmp_path / "database.db"

    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict = make_example_ds_dict()
        db[ds_dict["id"]] = ds_dict

        for ii in range(10):
            dsi = make_example_ds_dict()
            db[dsi["id"]] = dsi

        assert db[ds_dict["id"]] == ds_dict

        # iterate over all datasets
        ids = []
        for item in db:
            ids.append(item["id"])
        assert len(list(set(ids))) == 11


def test_pop(tmp_path):
    db_path = tmp_path / "database.db"

    with meta_cache_sqlite.SQLiteKeyJSONDatabase(db_path) as db:
        ds_dict = make_example_ds_dict()
        db[ds_dict["id"]] = ds_dict
        ds_dict2 = db[ds_dict["id"]]
        assert ds_dict == ds_dict2
        db.pop(ds_dict["id"])
        with pytest.raises(KeyError):
            db[ds_dict["id"]]
