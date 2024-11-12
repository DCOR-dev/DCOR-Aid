"""Convenience wrappers for commonly used API calls"""
import copy
import functools
import logging
import pathlib
import time
from typing import Callable, Dict

import requests
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from ..common import ConnectionTimeoutErrors

from .errors import (
    APIBadRequest, APIConflictError, APINotFoundError, NoS3UploadAvailableError
)
from .ckan_api import CKANAPI
from .s3_api import upload_s3_presigned


def dataset_activate(dataset_id: str, api: CKANAPI):
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
    try:
        # First check whether the dataset is already active. Use a long
        # timeout for this critical step.
        ds_dict = api.get("package_show", id=dataset_id, timeout=500)
    except ConnectionTimeoutErrors:
        raise
    else:
        if not ds_dict["state"] == "active":
            revise_dict = {
                "match": {"id": dataset_id},
                "update": {"state": "active"}}
            api.post("package_revise",
                     revise_dict,
                     # Dataset activation may take long when there
                     # are a lot of resources.
                     timeout=500)


def dataset_create(dataset_dict, api, resources=None,
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
    # Make sure we can access the desired circle.
    usr_circles = [c["name"] for c in api.get("organization_list_for_user",
                                              permission="create_dataset")]
    tgt_circle = dataset_dict.get("owner_org")
    if tgt_circle is None:
        raise APIConflictError(
            "Datasets must always be uploaded to a Circle. Please specify "
            "a circle via the 'owner_org' key in the CKAN dataset dictionary!")
    elif tgt_circle not in usr_circles:
        if create_circle:
            # Create the circle before creating the dataset
            api.post("organization_create",
                     data={"name": tgt_circle})
        else:
            raise APINotFoundError(
                f"The circle '{tgt_circle}' does not exist or the user "
                f"{api.user_name} is not allowed to upload datasets to it.")
    # Create the dataset.
    dataset_dict = copy.deepcopy(dataset_dict)
    dataset_dict["state"] = "draft"
    data = api.post("package_create", dataset_dict)
    if resources:
        # Upload resources
        for res in resources:
            resource_add(dataset_id=data["id"],
                         path=res,
                         api=api)
    if activate:
        # Finalize.
        dataset_activate(dataset_id=data["id"], api=api)
        data["state"] = "active"
    return data


def dataset_draft_remove(dataset_id, api):
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


def dataset_draft_remove_all(api, ignore_dataset_ids=None):
    """Remove all draft datasets

    Find and delete all draft datasets for a user. The user
    ID is inferred from the API key.

    Parameters
    ----------
    api: dcoraid.api.CKANAPI
        API instance with server, api_key (and optional certificate)
    ignore_dataset_ids: list or dcoraid.upload.queue.PersistentUploadJobList
        List of IDs that should not be deleted
    """
    if ignore_dataset_ids is None:
        ignore_dataset_ids = []
    user_dict = api.get_user_dict()
    data = api.get(
        "package_search",
        q="*:*",
        include_drafts=True,
        include_private=True,
        fq=f"creator_user_id:{user_dict['id']} AND state:draft",
        rows=1000)
    deleted = []
    ignored = []
    for dd in data["results"]:
        assert dd["state"] == "draft"
        if dd["id"] not in ignore_dataset_ids:
            dataset_draft_remove(dd["id"], api=api)
            deleted.append(dd)
        else:
            ignored.append(dd)
    if len(data["results"]) == 1000:
        # get more
        del2, ign2 = dataset_draft_remove_all(api)
        deleted += del2
        ignored += ign2
    return deleted, ignored


@functools.lru_cache(maxsize=100)
def get_organization_id_for_dataset(
        api: CKANAPI,
        dataset_id: str):
    """Return the organization ID for the given dataset

    This method is cached, because `package_show*` is not cached
    by the underlying API. Since we are only interested in the
    organization ID (which cannot change), this is fine.
    """
    ds_dict = api.get("package_show", id=dataset_id)
    return ds_dict["organization"]["id"]


def resource_add(dataset_id, path, api, resource_name=None,
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
        Dictionary of resource metadata (used for supplementary
        resource schemas "sp:section:key")
    exist_ok: bool
        If the uploaded resource already exists, do not re-upload
        it, only update the resource_dict.
    monitor_callback: None or callable
        This callable is used to monitor the upload progress. It must
        accept one argument: a
        `requests_toolbelt.MultipartEncoderMonitor` object.

    Returns
    -------
    srv_time: float
        Total time the server (nginx+uwsgi) needed to process the upload
    etag: str
        ETag of the resource on S3; returns None if the upload was not
        performed via S3

    See Also
    --------
    dcoraid.upload.queue.UploadJob
        An implementation of an upload job that monitors progress.
    """
    logger = logging.getLogger(__name__ + ".resource_add")
    etag = None
    if resource_dict is None:
        resource_dict = {}
    path = pathlib.Path(path)
    if resource_name is None:
        resource_name = path.name
    srv_time = 0  # total time waited for the server to process the upload
    # The ETag is computed locally only while uploading to S3, so in any other
    # case (e.g. the resource already exists), no ETag is computed. We
    # normally use the ETag to verify the upload, for which in this case
    # we have to use the SHA256 hash of the local file.
    if not exist_ok or not resource_exists(dataset_id=dataset_id,
                                           resource_name=resource_name,
                                           api=api):

        # Perform the upload
        try:
            # Uploading directly to S3 is the preferred option.
            etag = resource_add_upload_direct_s3(
                api=api,
                resource_path=path,
                dataset_id=dataset_id,
                resource_name=resource_name,
                monitor_callback=monitor_callback,
                logger=logger,
                timeout=27.9
            )
        except NoS3UploadAvailableError as e:
            # This is the fall-back option that causes a performance hit
            # on the DCOR server and will be unsupported in the future.
            logger.warning(str(e))
            resource_add_upload_legacy_indirect_ckan(
                api=api,
                resource_path=path,
                dataset_id=dataset_id,
                resource_name=resource_name,
                monitor_callback=monitor_callback,
                logger=logger,
                timeout=27.9
            )

    # If we are here, then the upload was successful
    logger.info(f"Finished upload of {dataset_id}/{resource_name}")

    if resource_dict:
        pkg_dict = api.get("package_show", id=dataset_id)
        res_list = pkg_dict.get("resources", [])
        res_names = [r["name"] for r in res_list]
        res_dict = res_list[res_names.index(resource_name)]
        # add resource_dict
        revise_dict = {
            "match__id": dataset_id,
            f"update__resources__{res_dict['id']}": resource_dict}
        api.post("package_revise", revise_dict)

    return srv_time, etag


def resource_add_upload_direct_s3(
        api: CKANAPI,
        dataset_id: str,
        resource_path: str | pathlib.Path,
        resource_name: str,
        monitor_callback: Callable = None,
        logger: logging.Logger = None,
        timeout: float = 27.9) -> str:
    """Direct resource upload to S3

    This is an efficient method for uploading resources. The files are
    uploaded directly to S3, without the detour via the CKAN API.

    Parameters
    ----------
    api: CKANAPI
        instance of :class:`CKANAPI`
    dataset_id: str
        dataset ID to which the resource belongs (used to identify the
        S3 bucket to which the resource is uploaded)
    resource_path: str or pathlib.Path
        path to the file that will be uploaded
    resource_name: str
        name of the resource (as displayed by DCOR)
    monitor_callback: callable
        callback method for tracking the progress
    logger: logging.Logger
        logger instance
    timeout: float
        timeout for the requests.post command for the upload

    Returns
    -------
    etag: str
        ETag computed for the resource during upload (can be compared
        to the ETag that DCOR stores in the "etag" resource property to
        verify an upload)
    """
    upload_id = f"{dataset_id}/{resource_name}"
    if logger is not None:
        logger.info(f"Commencing S3 upload of {upload_id}")

    resource_path = pathlib.Path(resource_path)
    # retrieve the organization ID and file size
    org_id = get_organization_id_for_dataset(api=api, dataset_id=dataset_id)
    file_size = resource_path.stat().st_size

    # retrieve the upload URL and the data fields
    try:
        upload_info = api.get(
            "resource_upload_s3_urls",
            organization_id=org_id,
            file_size=file_size,
            )
    except APIBadRequest:
        raise NoS3UploadAvailableError(f"Server {api.server} does not yet "
                                       f"support direct upload to S3")

    logger.info(
        f"We have {len(upload_info['upload_urls'])} upload parts for "
        f"a resource of size {file_size/1024**2:.2f} MiB of {upload_id}")

    etag = upload_s3_presigned(
        path=resource_path,
        upload_urls=upload_info["upload_urls"],
        complete_url=upload_info["complete_url"],
        callback=monitor_callback,
        timeout=timeout,
    )

    # The upload succeeded, now add the resource to the CKAN database.
    revise_dict = {
        "match": {"id": dataset_id},
        # Add a new resource(__extend on flattened key)
        "update__resources__extend": [{"id": upload_info["resource_id"],
                                       "name": resource_name,
                                       "s3_available": True,
                                       }
                                      ]
        }
    api.post("package_revise", revise_dict)
    return etag


def resource_add_upload_legacy_indirect_ckan(
        api: CKANAPI,
        resource_path: str | pathlib.Path,
        dataset_id: str,
        resource_name: str,
        monitor_callback: Callable = None,
        logger: logging.Logger = None,
        timeout: float = 27.9):
    """Legacy upload procedure for resources

    This method uses the legacy path (nginx -> uwsgi -> CKAN API) to
    upload resources to the block storage of the DCOR instance. It is
    termed legacy, because it is very inefficient to process resources
    like this. A better approach is to upload resources directly to S3.
    DCOR supports uploading resources directly to S3, which is facilitated
    via the `resource_upload_s3_url` API action providing a presigned
    upload link. (see :func:`resource_add_upload_direct_s3`)
    """
    upload_id = f"{dataset_id}/{resource_name}"
    if logger is not None:
        logger.info(f"Commencing legacy upload of {upload_id}")

    with pathlib.Path(resource_path).open("rb") as fd:
        # use package_revise to upload the resource
        # https://github.com/DCOR-dev/DCOR-Aid/issues/28
        e = MultipartEncoder(fields={
            'match__id': dataset_id,
            'update__resources__extend': f'[{{"name":"{resource_name}"}}]',
            'update__resources__-1__upload': (resource_name, fd)})
        m = MultipartEncoderMonitor(e, monitor_callback)
        # Increase the read size to speed-up upload (the default chunk
        # size for uploads in urllib is 8k which results in a lot of
        # Python code being involved in uploading a 20GB file; Setting
        # the chunk size to 4MB should increase the upload speed):
        # https://github.com/requests/toolbelt/issues/75
        # #issuecomment-237189952
        m._read = m.read
        m.read = lambda size: m._read(4 * 1024 * 1024)

        try:
            api.post("package_revise",
                     data=m,
                     dump_json=False,
                     headers={"Content-Type": m.content_type},
                     timeout=timeout)
        except requests.exceptions.Timeout:
            # This means that the server does not respond. This is ok,
            # because we can just check whether the resource was
            # processed correctly.
            if logger is not None:
                logger.info(f"Timeout for upload {upload_id}")
            start_wait_srv = time.monotonic()
            wait_time_minutes = 60
            for ii in range(wait_time_minutes):
                if resource_exists(dataset_id=dataset_id,
                                   resource_name=resource_name,
                                   api=api):
                    srv_time = timeout + time.monotonic() - start_wait_srv
                    if logger is not None:
                        logger.info(
                            f"Waited {srv_time / 60:.1f} min for {upload_id}")
                    break
                else:
                    if logger is not None:
                        logger.info(f"Waiting {ii + 1} min for {upload_id}")
                    time.sleep(60)
            else:
                raise ValueError(f"Timeout or {upload_id} not processed after "
                                 + f"{wait_time_minutes} minutes!")


def resource_exists(dataset_id: str,
                    resource_name: str,
                    api: CKANAPI,
                    resource_dict: Dict = None):
    """Check whether a resource exists in a dataset on DCOR

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
    for resource in pkg_dict.get("resources", []):
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
