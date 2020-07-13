import os
import pathlib
import time

from dcor_manager.upload import create_dataset, remove_draft
from dcor_manager.api import CKANAPI

# These don't have to be hard-coded like this.
SERVER = "dcor-dev.mpl.mpg.de"
CIRCLE = "dcor-manager-test-circle"
COLLECTION = "dcor-manager-test"
USER = "dcor-manager-test"
USER_NAME = "Dcor Managerin"
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
        "authors": USER_NAME,
    }
    return dataset_dict


def test_dataset_creation():
    """Just test whether we can create (and remove) a draft dataset"""
    # create some metadata
    dataset_dict = make_dataset_dict(hint="basic_test")
    # post dataset creation request
    data = create_dataset(dataset_dict=dataset_dict,
                          server=SERVER,
                          api_key=get_api_key())
    # simple test
    assert "authors" in data
    assert data["authors"] == USER_NAME
    assert data["state"] == "draft"
    # remove draft dataset
    remove_draft(dataset_id=data["id"],
                 server=SERVER,
                 api_key=get_api_key(),
                 )
    # make sure it is gone
    api = CKANAPI(server=SERVER, api_key=get_api_key())
    req = api.get("package_show", id=data["id"])
    assert req["state"] == "deleted"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
