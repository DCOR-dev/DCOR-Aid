import pathlib
import tempfile
import time

import pytest

from dcoraid.upload import UploadQueue, create_dataset
from dcoraid.upload.queue import PersistentUploadJobList
from dcoraid.upload.task import load_task

import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_queue_basic_functionalities():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-resource")
    # post dataset creation request
    data = create_dataset(dataset_dict=dataset_dict, api=api)
    joblist = UploadQueue(api=api)
    uj = joblist.new_job(dataset_id=data["id"],
                         paths=[dpath])
    assert uj.dataset_id == data["id"]
    assert uj in joblist

    samejob = joblist.get_job(data["id"])
    assert samejob is uj

    with pytest.raises(KeyError, match="Job 'peter' not found!"):
        joblist.get_job("peter")

    samejob2 = joblist[0]
    assert samejob2 is uj


def test_queue_create_dataset_with_resource():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-resource")
    # post dataset creation request
    data = create_dataset(dataset_dict=dataset_dict, api=api)
    joblist = UploadQueue(api=api)
    joblist.new_job(dataset_id=data["id"],
                    paths=[dpath])
    for _ in range(600):  # 60 seconds to upload
        if joblist[0].state == "done":
            break
        time.sleep(.1)
    else:
        assert False, "Job not finished: {}".format(joblist[0].get_status())


def test_queue_remove_job():
    """Remove a job from the queue and from the persistent list"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-resource")
    # post dataset creation request
    data = create_dataset(dataset_dict=dataset_dict, api=api)
    joblist = UploadQueue(api=api,
                          path_persistent_job_list=pujl_path)
    # disable all daemons, so no uploading happens
    joblist.daemon_compress.join()
    joblist.daemon_upload.join()
    joblist.daemon_verify.join()
    uj = joblist.new_job(dataset_id=data["id"],
                         paths=[dpath])
    assert uj.state == "init"
    joblist.remove_job(uj.dataset_id)
    assert uj not in joblist
    assert not joblist.jobs_eternal.job_exists(uj.dataset_id)

    # adding it again should work
    uj2 = joblist.new_job(dataset_id=data["id"],
                          paths=[dpath])
    assert uj2 in joblist
    assert uj2.__getstate__() == uj.__getstate__()


def test_persistent_upload_joblist_basic():
    """basic job tests"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)

    # add a job
    pujl.immortalize_job(uj)
    assert uj in pujl
    assert uj.dataset_id in pujl

    # find that job
    uj_same = pujl.summon_job(uj.dataset_id, api=api)
    assert uj_same is not uj, "not same instance"
    assert uj_same.__getstate__() == uj.__getstate__(), "same data"
    ids = pujl.get_queued_dataset_ids()
    assert uj.dataset_id in ids

    # remove a job
    assert pujl.job_exists(uj.dataset_id)
    assert pujl.is_job_queued(uj.dataset_id)
    assert not pujl.is_job_done(uj.dataset_id)
    pujl.obliterate_job(uj.dataset_id)
    assert uj not in pujl
    assert not pujl.job_exists(uj.dataset_id)


def test_persistent_upload_joblist_done():
    """test things when a job is done"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)
    pujl.immortalize_job(uj)
    pujl.set_job_done(uj.dataset_id)
    assert pujl.job_exists(uj.dataset_id)
    assert not pujl.is_job_queued(uj.dataset_id)
    assert pujl.is_job_done(uj.dataset_id)
    assert uj in pujl

    ids = pujl.get_queued_dataset_ids()
    assert uj.dataset_id not in ids


def test_persistent_upload_joblist_error_exists():
    """test things when a job is done"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)
    pujl.immortalize_job(uj)
    with pytest.raises(FileExistsError, match="already present at"):
        pujl.immortalize_job(uj)


def test_persistent_upload_joblist_error_exists_done():
    """test things when a job is done"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)
    pujl.immortalize_job(uj)
    pujl.set_job_done(uj.dataset_id)
    with pytest.raises(FileExistsError, match="is already done"):
        pujl.immortalize_job(uj)


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
