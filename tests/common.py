import json
import os
import pathlib
import tempfile
import time

from dcoraid.api import CKANAPI


CIRCLE = "dcoraid-circle"
COLLECTION = "dcoraid-collection"
DATASET = "dcoraid-dataset"
SERVER = "dcor-dev.mpl.mpg.de"
USER = "dcoraid"
USER_NAME = "DCOR-Aid tester"
TITLE = "DCOR-Aid test dataset"

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


def make_dataset_dict(hint=""):
    hint += " "
    dataset_dict = {
        "title": "A test dataset {}{}".format(hint, time.time()),
        "private": True,
        "license_id": "CC0-1.0",
        "owner_org": CIRCLE,
        "authors": USER_NAME,
    }
    return dataset_dict


def make_upload_task(task_id="123456789",
                     dataset_id=None,
                     dataset_dict=True,
                     resource_paths=[dpath],
                     resource_names=["gorgonzola.rtdc"],
                     resource_supplements=None,
                     ):
    """Return path to example task file"""
    if dataset_dict and not isinstance(dataset_dict, dict):
        dataset_dict = make_dataset_dict(hint="task_test")
    if dataset_dict and dataset_id is None:
        dataset_id = dataset_dict.get("dataset_id")
    uj_state = {
        "dataset_id": dataset_id,
        "task_id": task_id,
        "resource_paths": [str(pp) for pp in resource_paths],
        "resource_names": resource_names,
        "resource_supplements": resource_supplements,
    }
    data = {"upload_job": uj_state}
    if dataset_dict:
        data["dataset_dict"] = dataset_dict
    td = pathlib.Path(tempfile.mkdtemp(prefix="task_"))
    taskp = td / "test.dcoraid-task"
    with taskp.open("w") as fd:
        json.dump(data, fd,
                  ensure_ascii=False,
                  indent=2,
                  sort_keys=True)
    return str(taskp)
