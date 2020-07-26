import os
import pathlib

from dcoraid.upload import create_dataset
from dcoraid.dbmodel import model_api


# These don't have to be hard-coded like this.
SERVER = "dcor-dev.mpl.mpg.de"
CIRCLE = "dcor-manager-test-circle"
COLLECTION = "dcor-manager-test"
USER = "dcor-manager-test"
USER_NAME = "Dcor Managerin"
SEARCH_QUERY = "dataset"
DATASET = "test-dataset-for-search"
TITLE = "Test Dataset for search"

dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def get_api_key():
    key = os.environ.get("DCOR_API_KEY")
    if not key:
        # local file
        kp = pathlib.Path(__file__).parent / "api_key"
        if not kp.exists():
            raise ValueError("No DCOR_API_KEY variable or api_key file!")
        key = kp.read_text().strip()
    return key


def test_dcor_dev_public_api_interrogator():
    """This test uses the figshare datasets on SERVER"""
    db = model_api.APIInterrogator(SERVER)
    assert CIRCLE in db.get_circles()
    assert COLLECTION in db.get_collections()
    assert USER in db.get_users()


def test_dcor_dev_user_data():
    """Test the user information"""
    api_key = get_api_key()
    db = model_api.APIInterrogator(SERVER, api_key=api_key)
    data = db.get_user_data()
    assert data["fullname"] == USER_NAME, "fullname not 'Dcor Managerin'"


def test_dcor_dev_search():
    api_key = get_api_key()
    # Create a test dataset
    create_dataset({"title": TITLE,
                    "owner_org": CIRCLE,
                    "authors": USER_NAME,
                    "license_id": "CC0-1.0",
                    "groups": [{"name": COLLECTION}],
                    },
                   server=SERVER,
                   api_key=api_key,
                   resources=[dpath],
                   activate=True)
    db = model_api.APIInterrogator(SERVER, api_key=api_key)
    # Positive test
    data = db.search_dataset(query=SEARCH_QUERY,
                             circles=[CIRCLE],
                             collections=[COLLECTION],
                             )
    assert len(data) >= 1
    for dd in data:
        if dd["name"] == DATASET:
            break
    else:
        assert False, "{} not found!".format(DATASET)
    # Negative test
    data = db.search_dataset(query="cliauwenlc_should_never_exist",
                             circles=[CIRCLE],
                             collections=[COLLECTION],
                             )
    assert len(data) == 0, "search result for non-existent dataset?"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
