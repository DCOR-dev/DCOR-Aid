import copy
import pathlib

from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor


def activate_dataset(dataset_id, api):
    """Change the state of a dataset to "active"

    In the DCOR workflow, datasets are created as drafts and
    resources are added to the drafts. After that, the
    datasets are activated (and no resources can be added
    anymore).

    Parameters
    ----------
    dataset_id: str
        CKAN ID of the dataset
    api: dcoraid.api.CKANAPI
        API instance with server, api_key (and optional certificate)
    """
    revise_dict = {
        "match": {"id": dataset_id},
        "update": {"state": "active"}}
    api.post("package_revise", revise_dict)


def add_resource(dataset_id, path, api, resource_name=None,
                 resource_dict=None, exist_ok=False, monitor_callback=None):
    """Add a resource to a dataset

    Parameters
    ----------
    dataset_id: str
        CKAN ID of the dataset to which the resource is added
    path: str or pathlib.Path
        Path to the resource
    api: dcoraid.api.CKANAPI
        API instance with server, api_key (and optional certificate)
    resource_name: str
        Alternate resource name, if not set `path.name` is used
    resource_dict: dict
        Dictionary of resource meta data (used for supplementary
        resource schemas "sp:section:key")
    exist_ok: bool
        If the uploaded resource already exists, do not re-upload
        it, only update the resource_dict.
    monitor_callback: None or callable
        This callable is used to monitor the upload progress. It must
        accept one argument: a
        `requests_toolbelt.MultipartEncoderMonitor` object.

    See Also
    --------
    dcoraid.upload.queue.UploadJob
        An implementation of an upload job that monitors progress.
    """
    if resource_dict is None:
        resource_dict = {}
    path = pathlib.Path(path)
    if resource_name is None:
        resource_name = path.name

    pkg_dict = api.get("package_show", id=dataset_id)
    res_names = [r["name"] for r in pkg_dict["resources"]]
    if resource_name in res_names and exist_ok:
        # Resource already uploaded and user does not wish to re-upload
        data = pkg_dict["resources"][res_names.index(resource_name)]
    else:
        # Attempt upload
        e = MultipartEncoder(fields={
            'package_id': dataset_id,
            'name': resource_name,
            'upload': (resource_name, path.open('rb'))})
        m = MultipartEncoderMonitor(e, monitor_callback)
        # perform upload
        data = api.post("resource_create",
                        data=m,
                        dump_json=False,
                        headers={"Content-Type": m.content_type})
    if resource_dict:
        # add resource_dict
        revise_dict = {
            "match": {"id": dataset_id},
            "update__resources__{}".format(data["id"]): resource_dict}
        api.post("package_revise", revise_dict)


def create_dataset(dataset_dict, api, resources=None,
                   create_circle=False, activate=False):
    """Create a draft dataset

    Parameters
    ----------
    dataset_dict: dict
        CKAN dataset dictionary
    api: dcoraid.api.CKANAPI
        API instance with server, api_key (and optional certificate)
    resources: list of str or list of pathlib.Path
        Paths to dataset resources
    create_circle: bool
        Create the circle if it does not already exist
    activate: bool
        If True, then the dataset state is changed to "active"
        after uploading of the resources is complete. For DCOR,
        this implies that no other resources can be added to the
        dataset.
    """
    if resources is None:
        resources = []
    if create_circle:
        circles = [c["name"] for c in api.get("organization_list_for_user")]
        if dataset_dict["owner_org"] not in circles:
            # Create the circle before creating the dataset
            api.post("organization_create",
                     data={"name": dataset_dict["owner_org"]})
    dataset_dict = copy.deepcopy(dataset_dict)
    dataset_dict["state"] = "draft"
    data = api.post("package_create", dataset_dict)
    if resources:
        for res in resources:
            add_resource(dataset_id=data["id"],
                         path=res,
                         api=api)
    if activate:
        activate_dataset(dataset_id=data["id"], api=api)
        data["state"] = "active"
    return data


def remove_draft(dataset_id, api):
    """Remove a draft dataset

    Use this for deleting datasets that you created but which were
    not yet activated. The dataset is deleted and purged.

    Parameters
    ----------
    dataset_id: str
        CKAN ID of the dataset to which the resource is added
    api: dcoraid.api.CKANAPI
        API instance with server, api_key (and optional certificate)
    """
    api.post("package_delete", {"id": dataset_id})
    api.post("dataset_purge", {"id": dataset_id})


def remove_all_drafts(api):
    """Remove all draft datasets

    Find and delete all draft datasets for a user. The user
    ID is inferred from the API key.

    Parameters
    ----------
    api: dcoraid.api.CKANAPI
        API instance with server, api_key (and optional certificate)
    """
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
        remove_draft(dd["id"], api=api)
    return data["results"]


def resource_exists(dataset_id, resource_name, api, resource_dict=None):
    """Check whether a resource exists in a dataset

    Parameters
    ----------
    dataset_id: str
        UUID of the dataset
    resource_name: str
        name of the resource
    api: dcoraid.api.CKANAPI
        API instance
    resource_dict: dict
        resource dictionary to check against (optional); If this is given,
        then this method returns False if there are discrepancies in the
        resource schema supplements (even if the resource exists).
    """
    if resource_dict is None:
        resource_dict = {}
    pkg_dict = api.get("package_show", id=dataset_id)
    for resource in pkg_dict["resources"]:
        if resource["name"] == resource_name:
            # check that the resource dict matches
            for key in resource_dict:
                if key not in resource or resource[key] != resource_dict[key]:
                    # Either the resource schema supplement is missing
                    # or it is wrong.
                    return False
            return True
    else:
        return False


def resource_sha256_sums(dataset_id, api):
    """Return a dictionary of resources with the SHA256 sums as values"""
    pkg_dict = api.get("package_show", id=dataset_id)
    sha256dict = {}
    for resource in pkg_dict["resources"]:
        sha256dict[resource["name"]] = resource.get("sha256", None)
    return sha256dict
