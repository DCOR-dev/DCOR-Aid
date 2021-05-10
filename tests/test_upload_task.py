import json
import pathlib
import tempfile
import time

import pytest

from dcoraid.upload.dataset import create_dataset
from dcoraid.upload import task

import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def make_task(task_id="123456789",
              dataset_dict=None,
              resource_paths=[str(dpath)],
              resource_names=["gorgonzola.rtdc"],
              resource_supplements=None,
              ):
    """Return path to example task file"""
    if dataset_dict is None:
        dataset_dict = common.make_dataset_dict(hint="task_test")
    data = {
        "dataset_dict": dataset_dict,
        "task_id": task_id,
        "resource_paths": resource_paths,
        "resource_names": resource_names,
        "resource_supplements": resource_supplements,
    }
    td = pathlib.Path(tempfile.mkdtemp(prefix="task_"))
    taskp = td / "test.dcoraid-task"
    with taskp.open("w") as fd:
        json.dump(data, fd,
                  ensure_ascii=False,
                  indent=2,
                  sort_keys=True)
    return taskp


def test_load_basic():
    api = common.get_api()
    task_path = make_task(task_id="zpowiemsnh",
                          resource_names=["humdinger.rtdc"])
    uj = task.load_task(task_path, api=api)
    assert uj.task_id == "zpowiemsnh"
    assert uj.resource_names == ["humdinger.rtdc"]


def test_load_with_existing_dataset():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = create_dataset(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)
    task_path = make_task(dataset_dict=dataset_dict_with_id,
                          resource_paths=[str(dpath)],
                          resource_names=[dpath.name])
    uj = task.load_task(task_path, api=api)
    # skipping the upload should work, since it's already uploaded
    uj.set_state("online")
    uj.task_verify_resources()
    for ii in range(30):
        uj.task_verify_resources()
        if uj.state != "done":
            time.sleep(.1)
            continue
        else:
            break
    else:
        raise AssertionError("State not 'done' - No verification within 3s!")


def test_load_with_existing_dataset_map_from_task():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = create_dataset(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)
    task_path = make_task(dataset_dict=dataset_dict,
                          resource_paths=[str(dpath)],
                          resource_names=[dpath.name],
                          task_id="xwing")
    uj = task.load_task(
        task_path,
        api=api,
        map_task_to_dataset_id={"xwing": dataset_dict_with_id["id"]})
    assert uj.dataset_id == dataset_dict_with_id["id"]


def test_load_with_existing_dataset_map_from_task_control():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = create_dataset(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)
    task_path = make_task(dataset_dict=dataset_dict,
                          resource_paths=[str(dpath)],
                          resource_names=[dpath.name],
                          task_id="xwing")
    uj = task.load_task(
        task_path,
        api=api,
        map_task_to_dataset_id={"deathstar": dataset_dict_with_id["id"]})
    assert uj.dataset_id != dataset_dict_with_id["id"]


def test_no_ids():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    task_path = make_task(dataset_dict=dataset_dict,
                          resource_paths=[str(dpath)],
                          resource_names=[dpath.name],
                          task_id=None)
    with pytest.raises(ValueError, match="must contain 'task_id'"):
        task.load_task(task_path, api=api)


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
