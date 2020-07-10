import os
import pathlib
import time

from dcor_manager.api import CKANAPI

# These don't have to be hard-coded like this.
SERVER = "dcor-dev.mpl.mpg.de"
CIRCLE = "dcor-manager-test-circle"
COLLECTION = "dcor-manager-test"
USER = "dcor-manager-test"
USER_NAME = "Dcor Manager"
SEARCH_QUERY = "data"
DATASET = "test-dataset-1"


def get_api_key():
    key = os.environ.get("DCOR_API_KEY")
    if not key:
        # local file
        kp = pathlib.Path(__file__).parent / "api_key"
        if not kp.exists():
            raise ValueError("No DCOR_API_KEY variable or api_key file!")
        key = kp.read_text().strip()
    return key


def make_dataset_dict(hint=""):
    hint += " "
    dataset_dict = {
        "title": "A test dataset {}{}".format(hint, time.time()),
        "private": True,
        "license_id": "CC0-1.0",
        "owner_org": CIRCLE,
        "state": "draft",
        "authors": USER_NAME,
    }
    return dataset_dict


def test_dataset_creation():
    """Just test whether we can create (and remove) a draft dataset"""
    api_key = get_api_key()
    # create some metadata
    dataset_dict = make_dataset_dict(hint="basic_test")
    # initiate API
    api = CKANAPI(server=SERVER, api_key=api_key)
    # post dataset creation request
    data = api.post("package_create", dataset_dict)
    # simple test
    assert "authors" in data
    assert data["authors"] == USER_NAME
    assert data["state"] == "draft"
    # remove draft dataset
    api.post("package_delete", data)
    # make sure it is gone
    req = api.get("package_show", id=data["id"])
    assert req["state"] == "deleted"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
