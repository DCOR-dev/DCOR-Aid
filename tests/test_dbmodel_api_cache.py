import pathlib

import pytest

from dcoraid.api import CKANAPI, errors
from dcoraid.dbmodel import CachedAPIInterrogator

from . import common

dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"

HAS_FIGSHARE_ACCESS = common.get_test_defaults()["user"] == "dcoraid"


@pytest.mark.skipif(not HAS_FIGSHARE_ACCESS,
                    reason="No access to figshare-import circle")
def test_get_circles(tmp_path):
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    circles_dicts = db.get_circles()
    circles = [c["name"] for c in circles_dicts]
    defaults = common.get_test_defaults()
    assert defaults["circle"] in circles
    # requires that the "dcoraid" user is in the figshare-import circle
    assert "figshare-import" in circles


@pytest.mark.skipif(not HAS_FIGSHARE_ACCESS,
                    reason="No access to figshare-import circle")
def test_get_collections(tmp_path):
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    collections = [c["name"] for c in db.get_collections()]
    defaults = common.get_test_defaults()
    assert defaults["collection"] in collections
    # requires that the "dcoraid" user is in figshare-collection collection
    assert "figshare-collection" in collections


def test_get_datasets_user_owned(tmp_path):
    ds_dict = common.make_dataset_for_download()
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    db.update()
    ds_list = db.get_datasets_user_owned()
    assert ds_dict in ds_list


def test_get_users_anonymous(tmp_path):
    api = CKANAPI(server=common.SERVER)  # anonymous access
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    with pytest.raises(errors.APIAuthorizationError, match="Access denied"):
        db.get_users()


def test_public_api_interrogator(tmp_path):
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    circles = [c["name"] for c in db.get_circles()]
    defaults = common.get_test_defaults()
    collections = [c["name"] for c in db.get_collections()]
    assert defaults["circle"] in circles
    assert defaults["collection"] in collections
    assert defaults["user"] in db.get_users()


def test_user_data(tmp_path):
    """Test the user information"""
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    data = db.user_data
    defaults = common.get_test_defaults()
    assert data["fullname"] == defaults["user_name"], "fullname not correct"


@pytest.mark.skipif(not HAS_FIGSHARE_ACCESS,
                    reason="No access to figshare-import circle")
def test_get_datasets_user_shared_figshare(tmp_path):
    # The figshare circle must have the testing user as a member
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    datasets = db.get_datasets_user_shared()
    for dd in datasets:
        if dd["id"] == "89bf2177-ffeb-9893-83cc-b619fc2f6663":
            break
    else:
        assert False, "Search did not return figshare-7771184-v2!"
