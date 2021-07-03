import pathlib
import time
from unittest import mock

import pytest
import dclab.cli
from dcoraid.upload.dataset import create_dataset
from dcoraid.upload import job

import common


data_path = pathlib.Path(__file__).parent / "data"
rtdc_paths = [data_path / "calibration_beads_47.rtdc",
              data_path / "calibration_beads_47_nocomp.rtdc",
              ]


def test_initialize():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=rtdc_paths)
    assert uj.state == "init"


def test_full_upload():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=rtdc_paths)
    assert uj.state == "init"
    uj.task_compress_resources()
    assert uj.state == "parcel"
    uj.task_upload_resources()
    assert uj.state == "online"
    for ii in range(30):
        uj.task_verify_resources()
        if uj.state != "done":
            time.sleep(.1)
            continue
        else:
            break
    else:
        raise AssertionError("State not 'done' - No verification within 3s!")


def test_saveload():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=rtdc_paths, task_id="hanspeter")
    state = uj.__getstate__()
    assert state["dataset_id"] == dataset_dict["id"]
    assert rtdc_paths[0].samefile(state["resource_paths"][0])
    assert rtdc_paths[0].name == state["resource_names"][0]

    # now create a new job from the state
    uj2 = job.UploadJob.from_upload_job_state(state, api=api)
    state2 = uj2.__getstate__()
    assert state2["dataset_id"] == dataset_dict["id"]
    assert rtdc_paths[0].samefile(state2["resource_paths"][0])
    assert rtdc_paths[0].name == state2["resource_names"][0]
    assert state2["task_id"] == "hanspeter"


@mock.patch.object(job.shutil, "disk_usage")
def test_state_compress_disk_wait(disk_usage_mock):
    """If not space left on disk, upload job goes to state "disk-wait"""""
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[rtdc_paths[1]])
    assert uj.state == "init"

    class DiskUsageReturn1:
        free = 0

    class DiskUsageReturn2:
        free = 1024**3

    disk_usage_mock.side_effect = [DiskUsageReturn1, DiskUsageReturn2]

    t0 = time.perf_counter()
    uj.task_compress_resources()
    t1 = time.perf_counter()
    assert t1-t0 > 0.15, "function waits 0.2s in-between checks"
    disk_usage_mock.assert_called()


def test_state_compress_reuse():
    """Reuse previously compressed datasets"""""
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[rtdc_paths[1]])
    assert uj.state == "init"

    with mock.patch.object(job, "compress",
                           side_effect=dclab.cli.compress) as mockobj:
        uj.task_compress_resources()
        mockobj.assert_called_once()

    with mock.patch.object(job, "compress",
                           side_effect=dclab.cli.compress) as mockobj:
        uj.task_compress_resources()
        with pytest.raises(AssertionError):
            mockobj.assert_called()


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
