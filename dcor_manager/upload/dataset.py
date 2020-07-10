from ..api import CKANAPI


def create_dataset(dataset_dict, server, api_key):
    api = CKANAPI(server=server, api_key=api_key)
    data = api.call("package_create", **dataset_dict)
    import IPython
    IPython.embed()
    return data


def remove_draft(dataset_id):
    pass
