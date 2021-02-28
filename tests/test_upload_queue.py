import os
import pathlib
import time

from dcoraid.upload import UploadQueue, create_dataset

import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


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
        "owner_org": common.CIRCLE,
        "authors": common.USER_NAME,
    }
    return dataset_dict


def test_queue_create_dataset_with_resource():
    api = common.get_api()
    # create some metadata
    dataset_dict = make_dataset_dict(hint="create-with-resource")
    # post dataset creation request
    data = create_dataset(dataset_dict=dataset_dict, api=api)
    joblist = UploadQueue(api=api)
    joblist.add_job(dataset_dict=data,
                    paths=[dpath])
    for _ in range(600):  # 60 seconds to upload
        if joblist[0].state == "done":
            break
        time.sleep(.1)
    else:
        assert False, "Job not finished: {}".format(joblist[0].get_status())


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
