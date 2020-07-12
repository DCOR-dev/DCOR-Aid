import copy

from ..api import CKANAPI


def create_dataset(dataset_dict, server, api_key):
    """Creates a draft dataset"""
    api = CKANAPI(server=server, api_key=api_key)
    dataset_dict = copy.deepcopy(dataset_dict)
    dataset_dict["state"] = "draft"
    data = api.post("package_create", dataset_dict)
    return data


def remove_draft(dataset_id, server, api_key):
    """Remove a draft dataset"""
    api = CKANAPI(server=server, api_key=api_key)
    api.post("package_delete", {"id": dataset_id})


def remove_all_drafts(server, api_key):
    """Remove all draft datasets"""
    api = CKANAPI(server=server, api_key=api_key)
    user_dict = api.get_user_dict()
    data = api.get(
        "package_search",
        q="*:*",
        include_drafts=True,
        include_private=True,
        fq="creator_user_id:{} AND state:draft".format(user_dict["id"]),
        rows=1000)
    for dd in data["results"]:
        assert dd["state"] == "draft"
        remove_draft(dd["id"], server=server, api_key=api_key)
    return data["results"]


def activate_dataset(dataset_id, server, api_key):
    # TODO: use package_revise instead
    api = CKANAPI(server=server, api_key=api_key)
    data = api.get("package_show", id=dataset_id)
    data["state"] = "active"
    res = api.post("package_update", data)
    assert res["state"] == "active"
