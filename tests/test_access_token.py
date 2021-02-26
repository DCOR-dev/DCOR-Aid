import pathlib

from dcoraid.gui.wizard import access_token

datapath = pathlib.Path(__file__).parent / "data"


def test_get_hostname():
    """This test uses the figshare datasets on SERVER"""
    hostname = access_token.get_hostname(datapath / "cptamerica.dcor-access",
                                         "42")
    assert hostname == "dcor.example.com"


def test_get_api_key():
    api_key = access_token.get_api_key(datapath / "cptamerica.dcor-access",
                                       "42")
    assert api_key == "7c0c7203-4e25-4b14-a118-553c496a7a52"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
