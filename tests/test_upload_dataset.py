import pathlib
import time

from dcoraid.upload import dataset
from dcoraid.api import APIConflictError

import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def make_dataset_dict(hint=""):
    hint += " "
    dataset_dict = {
        "title": "A test dataset {}{}".format(hint, time.time()),
        "private": True,
        "license_id": "CC0-1.0",
        "owner_org": common.CIRCLE,
        "authors": common.USER_NAME,
    }
    return dataset_dict


def test_dataset_create_same_resource():
    """There should be an error when a resource is added twice"""
    api = common.get_api()
    # create some metadata
    dataset_dict = make_dataset_dict(hint="create-with-same-resource")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api)
    dataset.add_resource(dataset_id=data["id"],
                         path=dpath,
                         api=api
                         )
    try:
        dataset.add_resource(dataset_id=data["id"],
                             path=dpath,
                             api=api
                             )
    except APIConflictError:
        pass
    else:
        assert False, "Should not be able to upload same resource twice"


def test_dataset_creation():
    """Just test whether we can create (and remove) a draft dataset"""
    api = common.get_api()
    # create some metadata
    dataset_dict = make_dataset_dict(hint="basic_test")
    # post dataset creation request
    data = dataset.create_dataset(dataset_dict=dataset_dict,
                                  api=api,
                                  )
    # simple test
    assert "authors" in data
    assert data["authors"] == common.USER_NAME
    assert data["state"] == "draft"
    # remove draft dataset
    dataset.remove_draft(dataset_id=data["id"],
                         api=api,
                         )
    # make sure it is gone
    try:
        api.get("package_show", id=data["id"])
    except BaseException:
        pass
    else:
        assert False


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
