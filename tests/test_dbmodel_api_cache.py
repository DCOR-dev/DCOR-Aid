import pathlib

import pytest

from dcoraid.api import CKANAPI, errors
from dcoraid.dbmodel import CachedAPIInterrogator

from . import common

dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_get_circles(tmp_path):
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    circles_dicts = db.get_circles()
    circles = [c["name"] for c in circles_dicts]
    defaults = common.get_test_defaults()
    assert defaults["circle"] in circles
    # circle of another user in which the test user is a member
    shared = common.get_shared_defaults()
    assert shared["circle"] in circles
    assert shared["circle"] in [c["name"] for c in db.get_circles_user()]


def test_get_collections(tmp_path):
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    collections = [c["name"] for c in db.get_collections()]
    defaults = common.get_test_defaults()
    assert defaults["collection"] in collections
    # collection of another user in which the test user is a member
    shared = common.get_shared_defaults()
    assert shared["collection"] in collections
    collections_user = [c["name"] for c in db.get_collections_user()]
    assert shared["collection"] in collections_user


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
    # There may be more users than `limit`, so search for the test user.
    assert defaults["user"] in db.get_users(query=defaults["user"])


def test_user_data(tmp_path):
    """Test the user information"""
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    data = db.user_data
    defaults = common.get_test_defaults()
    assert data["fullname"] == defaults["user_name"], "fullname not correct"


def test_get_datasets_user_shared(tmp_path):
    # dataset of another user in a circle the test user is a member of
    shared = common.get_shared_defaults()
    api = common.get_api()
    db = CachedAPIInterrogator(cache_location=tmp_path, api=api)
    datasets = db.get_datasets_user_shared()
    assert shared["dataset_id"] in [dd["id"] for dd in datasets]
