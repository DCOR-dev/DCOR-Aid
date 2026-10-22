from unittest import mock

import pytest

from dcoraid.api import errors, users

from . import common


def test_create_dcor_dev_user_offline():
    with mock.patch.object(users, "CKANAPI") as ckanapi:
        ckanapi.return_value.post.return_value = {"token": "my-token"}
        key = users.create_dcor_dev_user(server="dcor-dev.example.com")
    assert key == "my-token"
    ckanapi.assert_called_once_with(server="dcor-dev.example.com",
                                    ssl_verify=True)
    api_call, kwargs = ckanapi.return_value.post.call_args
    assert api_call == ("user_create",)
    data = kwargs["data"]
    assert data["name"].startswith("dcoraid-")
    assert data["email"] == f"{data['name']}@dcor-dev.mpl.mpg.de"
    assert data["fullname"] == f"Player {data['name'][8:]}"
    assert len(data["password"]) >= 8
    assert data["with_apitoken"]


def test_create_dcor_dev_user_fullname_offline():
    with mock.patch.object(users, "CKANAPI") as ckanapi:
        ckanapi.return_value.post.return_value = {"token": "my-token"}
        users.create_dcor_dev_user(fullname="DCOR-Aid Tester")
    _, kwargs = ckanapi.return_value.post.call_args
    assert kwargs["data"]["fullname"] == "DCOR-Aid Tester"


@pytest.mark.parametrize("exception", [errors.APIAuthorizationError,
                                       errors.APINotFoundError,
                                       errors.NoAPIKeyError])
def test_is_valid_api_key_invalid_offline(exception):
    with mock.patch.object(users, "CKANAPI") as ckanapi:
        ckanapi.return_value.get_user_dict.side_effect = exception("no")
        assert not users.is_valid_api_key("dcor-dev.example.com", "abc")


def test_is_valid_api_key_empty_offline():
    with mock.patch.object(users, "CKANAPI") as ckanapi:
        assert not users.is_valid_api_key("dcor-dev.example.com", "")
        assert not users.is_valid_api_key("dcor-dev.example.com", None)
    ckanapi.assert_not_called()


def test_is_valid_api_key():
    key = common.get_api_key()
    assert users.is_valid_api_key(common.SERVER, key)
    assert not users.is_valid_api_key(common.SERVER, key[:-4] + "AAAA")
