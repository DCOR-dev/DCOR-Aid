import pathlib
import time

from dcoraid.upload.dataset import create_dataset
from dcoraid.upload import job

import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_initialize():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[dpath])
    assert uj.state == "init"


def test_full_upload():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = create_dataset(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[dpath])
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
                       resource_paths=[dpath], task_id="hanspeter")
    state = uj.__getstate__()
    assert state["dataset_id"] == dataset_dict["id"]
    assert dpath.samefile(state["resource_paths"][0])
    assert dpath.name == state["resource_names"][0]

    # now create a new job from the state
    uj2 = job.UploadJob.from_upload_job_state(state, api=api)
    state2 = uj2.__getstate__()
    assert state2["dataset_id"] == dataset_dict["id"]
    assert dpath.samefile(state2["resource_paths"][0])
    assert dpath.name == state2["resource_names"][0]
    assert state2["task_id"] == "hanspeter"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
