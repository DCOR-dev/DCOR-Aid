import functools
import json
import os
import pathlib
import shutil
import tempfile
import uuid
import warnings

import time

from dcoraid.api import CKANAPI, dataset_create
from dcoraid.upload import UploadQueue


# You can run e.g. tests on a local DCOR instance with
# export DCORAID_TEST_SERVER="http://192.168.122.143"
SERVER = os.environ.get("DCORAID_TEST_SERVER", "dcor-dev.mpl.mpg.de")

dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def get_api():
    api = CKANAPI(server=SERVER, api_key=get_api_key(), ssl_verify=True)
    return api


def get_api_key():
    key = os.environ.get("DCOR_API_KEY")
    if not key:
        # local file
        kp = pathlib.Path(__file__).parent / "api_key"
        if not kp.exists():
            raise ValueError("No DCOR_API_KEY variable or api_key file!")
        key = kp.read_text().strip()
    return key


@functools.lru_cache()
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


def make_dataset_dict(hint=""):
    defaults = get_test_defaults()
    space = " " if hint else ""
    dataset_dict = {
        "title": f"A test dataset {hint}{space}{time.time()}",
        "private": True,
        "license_id": "CC0-1.0",
        "owner_org": defaults["circle"],
        "authors": defaults["user_name"],
    }
    return dataset_dict


@functools.lru_cache()
def make_dataset_for_download(seed=0, wait_for_resource_metadata=False):
    """Set `seed` to get a new dataset (in case you need fresh resource ids)"""
    api = get_api()
    # create some metadata
    dataset_dict = make_dataset_dict(hint="test-download-dataset")
    # post dataset creation request
    data = dataset_create(dataset_dict=dataset_dict, api=api)
    joblist = UploadQueue(api=api)
    joblist.new_job(dataset_id=data["id"],
                    paths=[dpath])
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
                 wait_for_resource_metadata=False):
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
                if uq.jobs_eternal:
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
