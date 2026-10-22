import datetime
import functools
import json
import os
import pathlib
import shutil
import tempfile
import uuid
import warnings

import time

from dcoraid.api import (
    CKANAPI, dataset_create, APIConflictError, create_dcor_dev_user,
    is_valid_api_key
)
from dcoraid.upload import UploadQueue


# You can run e.g. tests on a local DCOR instance with
# export DCORAID_TEST_SERVER="http://192.168.122.143"
SERVER = os.environ.get("DCORAID_TEST_SERVER", "dcor-dev.mpl.mpg.de")

dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"
tpath = pathlib.Path(__file__).parent / "data" / "text_file.txt"


def get_api():
    api = CKANAPI(server=SERVER, api_key=get_api_key(), ssl_verify=True)
    return api


@functools.lru_cache
def get_api_key():
    """Return the API key of the test user

    The API key is taken from the `DCOR_API_KEY` environment variable.
    If that is not set, the key stored in the file `tests/api_key` is
    used. If that file does not exist or the key is invalid (e.g. the
    user was purged from the server), a new user is created on the test
    server (only possible on DCOR-dev-like servers) and its API key is
    written to `tests/api_key`.
    """
    key = os.environ.get("DCOR_API_KEY")
    if not key:
        key = get_or_create_api_key_file(
            pathlib.Path(__file__).parent / "api_key")
    return key


@functools.lru_cache
def get_api_key_other_user():
    """Return the API key of a second test user

    This user shares a circle, a collection, and a dataset with the
    test user (see :func:`get_shared_defaults`). The API key is stored
    in the file `tests/api_key_2` (created on demand like for
    :func:`get_api_key`).
    """
    return get_or_create_api_key_file(
        pathlib.Path(__file__).parent / "api_key_2")


def get_or_create_api_key_file(path):
    """Return a valid API key from `path` or create a new user"""
    if path.exists():
        key = path.read_text().strip()
        if is_valid_api_key(server=SERVER, api_key=key):
            return key
    # This only works on DCOR-dev-like servers that allow creating
    # users via the API.
    key = create_dcor_dev_user(server=SERVER, fullname="DCOR-Aid Tester")
    path.write_text(key)
    return key


@functools.lru_cache
def get_test_defaults():
    api = get_api()
    user_dict = api.get("user_show")
    name = user_dict["name"]
    return {
        "circle": f"{name}-circle",
        "collection": f"{name}-collection",
        "dataset": f"{name}-dataset",
        "user": name,
        "user_name": f"DCOR-Aid Tester {name.capitalize()}",
        "user_dict": user_dict,
        "title": "DCOR-Aid test dataset",
        "server": SERVER,
    }


@functools.lru_cache
def get_shared_defaults():
    """Return circle, collection, and dataset shared with the test user

    A second test user (see :func:`get_api_key_other_user`) creates a
    circle and a collection, adds the test user as a member to both, and
    creates a dataset in that circle and collection. From the point of
    view of the test user, these are not their own, but shared with them.
    """
    test_user = get_test_defaults()["user"]
    api = CKANAPI(server=SERVER, api_key=get_api_key_other_user(),
                  ssl_verify=True)
    name = api.user_name
    circle = f"{name}-shared-circle"
    collection = f"{name}-shared-collection"
    try:
        api.post("organization_create", {"name": circle})
    except APIConflictError:
        pass
    try:
        api.post("group_create", {"name": collection})
    except APIConflictError:
        pass
    api.post("organization_member_create",
             {"id": circle, "username": test_user, "role": "member"})
    api.post("group_member_create",
             {"id": collection, "username": test_user, "role": "member"})
    dataset_dict = make_dataset_dict(hint="shared", circle=circle)
    dataset_dict["groups"] = [{"name": collection}]
    ds_dict = dataset_create(dataset_dict=dataset_dict,
                             api=api,
                             resources=[dpath],
                             activate=True)
    return {
        "circle": circle,
        "collection": collection,
        "dataset_id": ds_dict["id"],
        "user": name,
    }


def make_dataset_dict(hint="", circle=None):
    defaults = get_test_defaults()
    space = " " if hint else ""
    dataset_dict = {
        "title": f"A test dataset {hint}{space}{time.time()}",
        "private": True,
        "license_id": "CC0-1.0",
        "owner_org": circle or defaults["circle"],
        "authors": defaults["user_name"],
    }
    return dataset_dict


def make_dataset_dict_full_fake(org_id=None, ds_id=None, user_id=None,
                                time_created=None, time_modified=None,
                                title=None):
    org_id = org_id or f"{uuid.uuid4()}"
    ds_id = ds_id or f"{uuid.uuid4()}"
    user_id = user_id or f"{uuid.uuid4()}"
    time_created = time_created or time.time() - 50
    time_modified = time_modified or time_created + 50
    created = datetime.datetime.fromtimestamp(time_created).isoformat()
    modified = datetime.datetime.fromtimestamp(time_modified).isoformat()
    title = title or "Standard title"

    return {"authors": "John Doe",
            "creator_user_id": f"{user_id}",
            "doi": "",
            "id": f"{ds_id}",
            "isopen": True,
            "license_id": "CC-BY-SA-4.0",
            "license_title": "Creative Commons Attribution Share-Alike 4.0",
            "license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
            "metadata_created": created,
            "metadata_modified": modified,
            "name": "an-example-dataset",
            "notes": "A description",
            "num_resources": 1, "num_tags": 2,
            "organization": {
                "id": f"{org_id}",
                "name": "dcoraid-circle", "title": "",
                "type": "organization",
                "description": "", "image_url": "",
                "created": "2020-09-23T09:50:44.826360",
                "is_organization": True,
                "approval_status": "approved",
                "state": "active"},
            "owner_org": f"{org_id}",
            "private": True, "references": "",
            "state": "active",
            "title": title,
            "type": "dataset", "resources": [
                {"cache_last_updated": None, "cache_url": None,
                 "created": created,
                 "modified": modified,
                 "dc:experiment:date": "2018-12-11",
                 "dc:experiment:event count": 47, "dc:experiment:run index": 1,
                 "url_type": "s3_upload"}],
            "tags": [{"display_name": "GFP",
                      "id": "5b5e7553-49a0-49b4-b342-28b8384800c0",
                      "name": "GFP",
                      "state": "active",
                      "vocabulary_id": None},
                     {"display_name": "HL60",
                      "id": "f4e18050-d2dc-4fd2-93d4-015666f0e066",
                      "name": "HL60",
                      "state": "active",
                      "vocabulary_id": None}],
            "groups": [],
            "relationships_as_subject": [],
            "relationships_as_object": []}


@functools.lru_cache
def make_dataset_for_download(seed=0, add_text_file=False, circle=None):
    """Set `seed` to get a new dataset (in case you need fresh resource ids)"""
    defaults = get_test_defaults()

    api = get_api()
    # create some metadata
    dataset_dict = make_dataset_dict(hint="test-download-dataset",
                                     circle=circle or defaults["circle"],
                                     )
    # post dataset creation request
    data = dataset_create(dataset_dict=dataset_dict, api=api)
    joblist = UploadQueue(api=api)
    paths = [dpath]
    if add_text_file:
        paths.append(tpath)
    joblist.new_job(dataset_id=data["id"],
                    paths=paths)
    wait_for_job(joblist, data["id"],
                 wait_for_resource_metadata=True)
    return api.get("package_show", id=data["id"])


def make_upload_task(task_id=True,  # tester may pass `None` to disable
                     dataset_id=None,
                     dataset_dict=True,
                     resource_paths=None,
                     resource_names=None,
                     resource_supplements=None,
                     collections=None,
                     ):
    """Return path to example task file"""
    if resource_paths is None:
        resource_paths = [dpath]
    if resource_names is None:
        resource_names = ["gorgonzola.rtdc"]
    if task_id is True:
        task_id = str(uuid.uuid4())
    if dataset_dict and not isinstance(dataset_dict, dict):
        dataset_dict = make_dataset_dict(hint="task_test")
    if dataset_dict and dataset_id is None:
        dataset_id = dataset_dict.get("id")
    if collections is None:
        collections = []
    td = pathlib.Path(tempfile.mkdtemp(prefix="task_"))
    # copy resources there
    new_resource_paths = []
    for pp in resource_paths:
        newpp = td / pathlib.Path(pp).name
        if pathlib.Path(pp).exists():
            # only move the file if the test uses an actual file
            shutil.copy2(pp, newpp)
            new_resource_paths.append(newpp)
        else:
            new_resource_paths.append(pp)
    uj_state = {
        "dataset_id": dataset_id,
        "task_id": task_id,
        "resource_paths": [str(pp) for pp in new_resource_paths],
        "resource_names": resource_names,
        "resource_supplements": resource_supplements,
        "collections": collections,
    }
    data = {"upload_job": uj_state}
    if dataset_dict:
        data["dataset_dict"] = dataset_dict
    taskp = td / "test.dcoraid-task"
    with taskp.open("w") as fd:
        json.dump(data, fd,
                  ensure_ascii=False,
                  indent=2,
                  sort_keys=True)
    return str(taskp)


def wait_for_job(upload_queue, dataset_id, wait_time=60,
                 wait_for_resource_metadata=False,
                 set_job_done=True):
    uq = upload_queue
    uj = uq.get_job(dataset_id)
    # wait for the upload to finish
    for _ in range(wait_time*10):
        if uj.state == "done":
            complete = True
            if wait_for_resource_metadata:
                # make sure these keys are set in the resource dict
                ds_dict = uq.api.get("package_show", id=dataset_id)
                for key in ["sha256", "mimetype", "size"]:
                    for res in ds_dict["resources"]:
                        if key not in res:
                            complete = False
                            break
                    if not complete:
                        break
            if complete:
                if set_job_done and uq.jobs_eternal:
                    # TODO:
                    # We do this manually here. Actually, a better solution
                    # would be to implement a signal-slot type of workflow
                    # where the job tells the queue when it is done.
                    uq.jobs_eternal.set_job_done(dataset_id)
                break
        time.sleep(.1)
    else:
        assert False, f"Job '{uj}' not done in {wait_time}s, state {uj.state}!"


def wait_for_job_no_queue(upload_job, wait_time=120):
    uj = upload_job
    # wait for the upload to finish
    for _ in range(wait_time*10):
        with warnings.catch_warnings():
            # Ignore warnings about current state of upload job
            warnings.simplefilter("ignore", category=UserWarning)
            uj.task_verify_resources()
        if uj.state == "done":
            break
        time.sleep(.1)
    else:
        assert False, f"Job '{uj}' not done in {wait_time}s, state {uj.state}!"
