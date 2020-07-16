import copy
import pathlib

from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from ..api import CKANAPI


def activate_dataset(dataset_id, server, api_key):
    """Change the state of a dataset to "active"

    In the DCOR workflow, datasets are created as drafts and
    resources are added to the drafts. After that, the
    datasets are activated (and no resources can be added
    anymore).

    Parameters
    ----------
    dataset_id: str
        CKAN ID of the dataset
    server: str
        server domain name
    api_key: str
        API key of the CKAN/DCOR user
    """
    # TODO: use package_revise instead
    api = CKANAPI(server=server, api_key=api_key)
    data = api.get("package_show", id=dataset_id)
    data["state"] = "active"
    res = api.post("package_update", data)
    assert res["state"] == "active"


def create_dataset(dataset_dict, server, api_key, resources=[],
                   activate=False):
    """Create a draft dataset

    Parameters
    ----------
    dataset_dict: dict
        CKAN dataset dictionary
    server: str
        server domain name
    api_key: str
        API key of the CKAN/DCOR user
    resources: list of str or pathlib.Path
        Paths to dataset resources
    activate: bool
        If True, then the dataset state is changed to "active"
        after uploading of the resources is complete. For DCOR,
        this implies that no other resources can be added to the
        dataset. 
    """
    api = CKANAPI(server=server, api_key=api_key)
    dataset_dict = copy.deepcopy(dataset_dict)
    dataset_dict["state"] = "draft"
    data = api.post("package_create", dataset_dict)
    if resources:
        for res in resources:
            add_resource(dataset_id=data["id"],
                         path=res,
                         server=server,
                         api_key=api_key)
    if activate:
        activate_dataset(dataset_id=data["id"], server=server, api_key=api_key)
    return data


def add_resource(dataset_id, path, server, api_key, monitor_callback=None):
    """Add a resource to a dataset

    Parameters
    ----------
    dataset_id: str
        CKAN ID of the dataset to which the resource is added
    path: str or pathlib.Path
        Path to the resource
    server: str
        server domain name
    api_key: str
        API key of the CKAN/DCOR user
    monitor_callback: None or callable
        This callable is used to monitor the upload progress. It must
        accept one argument: a
        `requests_toolbelt.MultipartEncoderMonitor` object.

    See Also
    --------
    dcor_manager.upload.joblist.UploadJob
        An implementation of an upload job that monitors progress.
    """
    path = pathlib.Path(path)
    api = CKANAPI(server=server, api_key=api_key)
    e = MultipartEncoder(
        fields={'package_id': dataset_id,
                'name': path.name,
                'upload': (path.name, path.open('rb'))}
    )
    m = MultipartEncoderMonitor(e, monitor_callback)
    api.post("resource_create",
             data=m,
             dump_json=False,
             headers={"Content-Type": m.content_type})


def remove_draft(dataset_id, server, api_key):
    """Remove a draft dataset

    Use this for deleting datasets that you created but which were
    not yet activated. The dataset is deleted and purged.

    Parameters
    ----------
    dataset_id: str
        CKAN ID of the dataset to which the resource is added
    server: str
        server domain name
    api_key: str
        API key of the CKAN/DCOR user
    """
    api = CKANAPI(server=server, api_key=api_key)
    api.post("package_delete", {"id": dataset_id})
    api.post("dataset_purge", {"id": dataset_id})


def remove_all_drafts(server, api_key):
    """Remove all draft datasets

    Find and delete all draft datasets for a user. The user
    ID is inferred from the API key.

    Parameters
    ----------
    server: str
        server domain name
    api_key: str
        API key of the CKAN/DCOR user
    """
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
