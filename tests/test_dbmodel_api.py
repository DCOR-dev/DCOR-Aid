import pathlib
import random

from dcoraid.upload import create_dataset
from dcoraid.dbmodel import model_api

import common

dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_dcor_dev_public_api_interrogator():
    """This test uses the figshare datasets on SERVER"""
    api = common.get_api()
    db = model_api.APIInterrogator(api=api)
    assert common.CIRCLE in db.get_circles()
    assert common.COLLECTION in db.get_collections()
    assert common.USER in db.get_users()


def test_dcor_dev_user_data():
    """Test the user information"""
    api = common.get_api()
    db = model_api.APIInterrogator(api=api)
    data = db.get_user_data()
    assert data["fullname"] == common.USER_NAME, "fullname not correct"


def test_dcor_dev_search():
    api = common.get_api()
    ranstr = ''.join(random.choice("0123456789") for _i in range(10))
    # Create a test dataset
    create_dataset({"title": "{} {}".format(common.TITLE, ranstr),
                    "owner_org": common.CIRCLE,
                    "authors": common.USER_NAME,
                    "license_id": "CC0-1.0",
                    "groups": [{"name": common.COLLECTION}],
                    },
                   api=api,
                   resources=[dpath],
                   activate=True)
    db = model_api.APIInterrogator(api=api)
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


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
