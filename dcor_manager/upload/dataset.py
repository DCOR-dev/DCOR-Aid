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
    api = CKANAPI(server=server, api_key=api_key)
    api.post("package_delete", {"id": dataset_id})
