"""Offline tests for listing circles and collections in `APIInterrogator`"""
from unittest import mock

import pytest

from dcoraid.api.errors import APIBadRequest
from dcoraid.dbmodel import db_api


def make_interrogator(num_circles=60, num_collections=40, user_id="user-1",
                      member_of=(), max_items=25, group_list_action=False):
    """Return an APIInterrogator with a mocked CKAN API

    Parameters
    ----------
    num_circles, num_collections: int
        Number of circles and collections on the server
    user_id: str or None
        ID of the user (None for anonymous access)
    member_of: list of str
        Collection names in which the user is a member
    max_items: int
        Maximum number of items CKAN returns with `all_fields=True`
    group_list_action: bool
        Whether the server supports the `dcor_group_list_for_user` action
        (ckanext-dcor_schemas >= 1.2.0)
    """
    circles = [{"id": f"oid-{ii:03d}", "name": f"circle-{ii:03d}"}
               for ii in range(num_circles)]
    collections = [{"id": f"gid-{ii:03d}", "name": f"collection-{ii:03d}"}
                   for ii in range(num_collections)]
    calls = []

    def get(api_call, **kwargs):
        calls.append((api_call, kwargs))
        if api_call in ["organization_list", "group_list"]:
            items = circles if api_call == "organization_list" else collections
            assert kwargs["all_fields"]
            assert kwargs["sort"] == "name asc"
            offset = kwargs.get("offset", 0)
            limit = min(kwargs.get("limit", max_items), max_items)
            return items[offset:offset + limit]
        elif api_call == "member_list":
            col = next(c for c in collections if c["id"] == kwargs["id"])
            if col["name"] in member_of:
                return [[user_id, "user", "member"]]
            else:
                return [["other-user", "user", "admin"]]
        elif api_call == "organization_list_for_user":
            return circles[:2]
        elif api_call == "dcor_group_list_for_user":
            if group_list_action:
                return [c for c in collections if c["name"] in member_of]
            else:
                raise APIBadRequest("Bad request - Action name not known: "
                                    "dcor_group_list_for_user")
        raise ValueError(f"Unexpected API call {api_call}")

    api = mock.MagicMock()
    api.user_id = user_id
    api.get_user_dict.return_value = {"id": user_id,
                                      "name": "test-user",
                                      "number_created_packages": 0}
    api.copy.return_value = api
    api.get.side_effect = get
    return db_api.APIInterrogator(api), calls


@pytest.mark.parametrize("num_circles", [0, 1, 24, 25, 26, 50, 60])
def test_get_circles_all(num_circles):
    """CKAN returns at most 25 items, so we must page through results"""
    ai, _ = make_interrogator(num_circles=num_circles)
    circles = ai.get_circles()
    assert [c["name"] for c in circles] == \
        [f"circle-{ii:03d}" for ii in range(num_circles)]


@pytest.mark.parametrize("max_items", [10, 25])
def test_get_circles_server_max_items(max_items):
    """Paging works even if the server limit is lower than 25"""
    ai, _ = make_interrogator(num_circles=60, max_items=max_items)
    assert len(ai.get_circles()) == 60


def test_get_collections_all():
    ai, _ = make_interrogator(num_collections=40)
    collections = ai.get_collections()
    assert [c["name"] for c in collections] == \
        [f"collection-{ii:03d}" for ii in range(40)]


def test_get_list_all_fields_no_duplicates():
    """An item created in-between requests shifts the pages"""
    ai, _ = make_interrogator()
    pages = [[{"id": "a", "name": "a"}, {"id": "b", "name": "b"}],
             [{"id": "b", "name": "b"}, {"id": "c", "name": "c"}],
             []]
    ai.api.get.side_effect = lambda api_call, **kwargs: pages.pop(0)
    items = ai._get_list_all_fields("organization_list")
    assert [it["id"] for it in items] == ["a", "b", "c"]


def test_get_circles_user():
    ai, calls = make_interrogator()
    assert [c["name"] for c in ai.get_circles_user()] == \
        ["circle-000", "circle-001"]
    assert calls == [("organization_list_for_user", {"permission": "read"})]


def test_get_collections_user():
    """Collections in which the user has any role (also "member")"""
    ai, calls = make_interrogator(num_collections=40,
                                  member_of=["collection-003",
                                             "collection-030"],
                                  group_list_action=True)
    assert [c["name"] for c in ai.get_collections_user()] == \
        ["collection-003", "collection-030"]
    # the server-side action is used (one request)
    assert [c[0] for c in calls] == ["dcor_group_list_for_user"]


def test_get_collections_user_fallback():
    """Older servers do not support `dcor_group_list_for_user`"""
    ai, calls = make_interrogator(num_collections=40,
                                  member_of=["collection-003",
                                             "collection-030"],
                                  group_list_action=False)
    assert [c["name"] for c in ai.get_collections_user()] == \
        ["collection-003", "collection-030"]
    # fall back to checking the members of all collections
    api_calls = [c[0] for c in calls]
    assert api_calls[0] == "dcor_group_list_for_user"
    assert api_calls.count("member_list") == 40


def test_get_collections_user_other_bad_request():
    """Only fall back if the action is not known to the server"""
    ai, _ = make_interrogator()
    ai.api.get.side_effect = APIBadRequest("Bad request - something else")
    with pytest.raises(APIBadRequest, match="something else"):
        ai.get_collections_user()


def test_get_lists_user_anonymous():
    ai, calls = make_interrogator(user_id=None)
    assert ai.get_circles_user() == []
    assert ai.get_collections_user() == []
    assert calls == []


@pytest.mark.parametrize("kwargs,expected", [
    [{}, {"q": "", "limit": 20}],
    [{"query": "peter"}, {"q": "peter", "limit": 20}],
    [{"query": "peter", "limit": 5}, {"q": "peter", "limit": 5}],
])
def test_get_users_query(kwargs, expected):
    ai, _ = make_interrogator()
    users = [{"id": "1", "name": "peter", "fullname": "Peter Pan"},
             {"id": "2", "name": "peterson", "fullname": ""}]
    ai.api.get.side_effect = None
    ai.api.get.return_value = users
    assert ai.get_users(**kwargs) == ["peter", "peterson"]
    ai.api.get.assert_called_with("user_autocomplete", **expected)
    assert ai.get_users(ret_fullnames=True, **kwargs) == (
        ["peter", "peterson"], ["Peter Pan", "peterson"])
