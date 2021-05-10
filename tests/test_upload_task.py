import json
import pathlib
import tempfile
import time

import pytest

from dcoraid.upload.dataset import create_dataset
from dcoraid.upload import job, task

import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def make_task(task_id="123456789",
              dataset_id=None,
              dataset_dict=True,
              resource_paths=[str(dpath)],
              resource_names=["gorgonzola.rtdc"],
              resource_supplements=None,
              ):
    """Return path to example task file"""
    if dataset_dict and not isinstance(dataset_dict, dict):
        dataset_dict = common.make_dataset_dict(hint="task_test")
    if dataset_dict and dataset_id is None:
        dataset_id = dataset_dict.get("dataset_id")
    uj_state = {
        "dataset_id": dataset_id,
        "task_id": task_id,
        "resource_paths": resource_paths,
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
    return taskp


def test_custom_dataset_dict():
    api = common.get_api()
    # post dataset creation request
    task_path = make_task(dataset_dict=False,
                          resource_paths=[str(dpath)],
                          resource_names=[dpath.name])
    dataset_dict = common.make_dataset_dict()
    dataset_dict["authors"] = "Captain Hook!"
    uj = task.load_task(task_path, api=api,
                        dataset_kwargs=dataset_dict)
    # now make sure the authors were set correctly
    ddict = api.get("package_show", id=uj.dataset_id)
    assert ddict["authors"] == "Captain Hook!"


def test_custom_dataset_dict_2():
    api = common.get_api()
    # post dataset creation request
    task_path = make_task(dataset_dict=True,
                          resource_paths=[str(dpath)],
                          resource_names=[dpath.name])
    dataset_dict = common.make_dataset_dict()
    dataset_dict["authors"] = "Captain Hook!"
    uj = task.load_task(task_path, api=api,
                        dataset_kwargs=dataset_dict)
    # now make sure the authors were set correctly
    ddict = api.get("package_show", id=uj.dataset_id)
    assert ddict["authors"] == "Captain Hook!"


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
    assert uj.dataset_id == dataset_dict_with_id["id"]
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
    with pytest.raises(ValueError, match="must contain a task ID"):
        task.load_task(task_path, api=api)


def test_save_load():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[dpath], task_id="hanspeter")
    td = pathlib.Path(tempfile.mkdtemp(prefix="task_"))
    task_path = td / "test.dcoraid-task"
    task.save_task(uj, path=task_path)
    uj2 = task.load_task(task_path, api=api)
    assert uj.dataset_id == uj2.dataset_id
    assert uj.paths[0].samefile(uj2.paths[0])


def test_wrong_ids():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    dataset_dict["id"] = "peter"
    task_path = make_task(dataset_dict=dataset_dict,
                          dataset_id="hans",  # different id
                          resource_paths=[str(dpath)],
                          resource_names=[dpath.name])
    with pytest.raises(ValueError, match="do not match!"):
        task.load_task(task_path, api=api)


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
