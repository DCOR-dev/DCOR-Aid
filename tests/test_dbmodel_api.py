import os
import pathlib

from dcor_manager.dbmodel import model_api


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
    """This test uses the figshare datasets on dcor-dev.mpl.mpg.de"""
    db = model_api.APIInterrogator("dcor-dev.mpl.mpg.de")
    assert "figshare-import" in db.get_circles()
    assert 'dcor-manager-test' in db.get_collections()
    assert 'dcor-manager-test' in db.get_users()


def test_dcor_dev_user_data():
    """Test the user information"""
    api_key = get_api_key()
    db = model_api.APIInterrogator("dcor-dev.mpl.mpg.de", api_key=api_key)
    data = db.get_user_data()
    assert data["fullname"] == "Dcor Manager", "fullname not 'Dcor Manager'"


def test_dcor_dev_search():
    api_key = get_api_key()
    db = model_api.APIInterrogator("dcor-dev.mpl.mpg.de", api_key=api_key)
    # Positive test
    data = db.search_dataset(query="data",
                             circles=["dcor-manager-test-circle"],
                             collections=["dcor-manager-test"],
                             )
    assert len(data) >= 1
    for dd in data:
        if dd["name"] == "test-dataset-1":
            break
    else:
        assert False, "test-dataset-1 not found!"
    # Negative test
    data = db.search_dataset(query="cliauwenlc_should_never_exist",
                             circles=["dcor-manager-test-circle"],
                             collections=["dcor-manager-test"],
                             )
    assert len(data) == 0, "search result for non-existent dataset?"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
