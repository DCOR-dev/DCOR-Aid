import pathlib
import random

import pytest

from dcoraid.api import CKANAPI, errors, dataset_create
from dcoraid.dbmodel import db_api

from . import common

dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_get_circles():
    api = common.get_api()
    db = db_api.APIInterrogator(api=api)
    circles = db.get_circles()
    assert common.CIRCLE in circles
    # requires that the "dcoraid" user is in the figshare-import circle
    assert "figshare-import" in circles


def test_get_collections():
    api = common.get_api()
    db = db_api.APIInterrogator(api=api)
    collections = db.get_collections()
    assert common.COLLECTION in collections
    # requires that the "dcoraid" user is in the figshare-collection collection
    assert "figshare-collection" in collections


def test_get_users_anonymous():
    api = CKANAPI(server="dcor-dev.mpl.mpg.de")
    db = db_api.APIInterrogator(api=api)
    with pytest.raises(errors.APIAuthorizationError, match="Access denied"):
        db.get_users()


def test_public_api_interrogator():
    """This test uses the figshare datasets on SERVER"""
    api = common.get_api()
    db = db_api.APIInterrogator(api=api)
    assert common.CIRCLE in db.get_circles()
    assert common.COLLECTION in db.get_collections()
    assert common.USER in db.get_users()


def test_user_data():
    """Test the user information"""
    api = common.get_api()
    db = db_api.APIInterrogator(api=api)
    data = db.user_data
    assert data["fullname"] == common.USER_NAME, "fullname not correct"


def test_search_dataset_basic():
    api = common.get_api()
    ranstr = ''.join(random.choice("0123456789") for _i in range(10))
    # Create a test dataset
    dataset_create({"title": "{} {}".format(common.TITLE, ranstr),
                    "owner_org": common.CIRCLE,
                    "authors": common.USER_NAME,
                    "license_id": "CC0-1.0",
                    "groups": [{"name": common.COLLECTION}],
                    },
                   api=api,
                   resources=[dpath],
                   activate=True)
    db = db_api.APIInterrogator(api=api)
    # Positive test
    data = db.search_dataset(query="dcoraid",
                             circles=[common.CIRCLE],
                             collections=[common.COLLECTION],
                             )
    assert len(data) >= 1
    for dd in data:
        if dd["name"].endswith(ranstr):
            break
    else:
        assert False, "{} not found!".format(common.DATASET)
    # Negative test
    data = db.search_dataset(query="cliauwenlc_should_never_exist",
                             circles=[common.CIRCLE],
                             collections=[common.COLLECTION],
                             )
    assert len(data) == 0, "search result for non-existent dataset?"


def test_search_dataset_limit():
    api = common.get_api()
    ranstr = ''.join(random.choice("0123456789") for _i in range(10))
    dataset_ids = []
    # Create three test datasets
    for _ in range(3):
        ds_dict = dataset_create(
            {"title": "{} {}".format(common.TITLE, ranstr),
             "owner_org": common.CIRCLE,
             "authors": common.USER_NAME,
             "license_id": "CC0-1.0",
             "groups": [{"name": common.COLLECTION}],
             },
            api=api,
            resources=[dpath],
            activate=True)
        dataset_ids.append(ds_dict["id"])
    db = db_api.APIInterrogator(api=api)
    data_limited = db.search_dataset(query=ranstr,
                                     circles=[common.CIRCLE],
                                     collections=[common.COLLECTION],
                                     circle_collection_union=True,
                                     limit=2
                                     )
    assert len(data_limited) == 2
    data_unlimited = db.search_dataset(query=ranstr,
                                       circles=[common.CIRCLE],
                                       collections=[common.COLLECTION],
                                       circle_collection_union=True,
                                       limit=0
                                       )
    assert len(data_unlimited) == 3


def test_search_dataset_limit_negative_error():
    api = common.get_api()
    ranstr = ''.join(random.choice("0123456789") for _i in range(10))
    # Create three test datasets
    dataset_create(
        {"title": "{} {}".format(common.TITLE, ranstr),
         "owner_org": common.CIRCLE,
         "authors": common.USER_NAME,
         "license_id": "CC0-1.0",
         "groups": [{"name": common.COLLECTION}],
         },
        api=api,
        resources=[dpath],
        activate=True)
    db = db_api.APIInterrogator(api=api)
    with pytest.raises(ValueError, match="must be 0 or >0"):
        db.search_dataset(query=ranstr,
                          circles=[common.CIRCLE],
                          collections=[common.COLLECTION],
                          circle_collection_union=True,
                          limit=-1
                          )


def test_search_dataset_only_one_filter_query():
    api = common.get_api()
    db = db_api.APIInterrogator(api=api)
    ds = db.search_dataset(filter_queries=[f"-creator_user_id:{api.user_id}"])
    for di in ds:
        if di["name"] == "figshare-7771184-v2":
            break
    else:
        assert False, "Search did not return figshare-7771184-v2!"


def test_get_datasets_user_shared_figshare():
    """The figshare circle must have the user "dcoraid" as a member
    """
    api = common.get_api()
    db = db_api.APIInterrogator(api=api)
    datasets = db.get_datasets_user_shared()
    for dd in datasets:
        if dd["id"] == "89bf2177-ffeb-9893-83cc-b619fc2f6663":
            break
    else:
        assert False, "Search did not return figshare-7771184-v2!"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "_call_"):
            loc[key]()
