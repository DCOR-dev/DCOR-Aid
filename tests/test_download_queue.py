import pathlib
import tempfile
import time

import pytest

from dcoraid.download import DownloadJob, DownloadQueue
from dcoraid.download.queue import PersistentDownloadJobList

from . import common


data_path = pathlib.Path(__file__).parent / "data"
dpath = data_path / "calibration_beads_47.rtdc"


def test_queue_basic_functionalities():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    joblist = DownloadQueue(api=api)
    resource_id = ds_dict["resources"][0]["id"]
    dj = joblist.new_job(resource_id=resource_id,
                         download_path=td)

    assert dj.resource_id == resource_id
    assert dj in joblist

    samejob = joblist.get_job(resource_id)
    assert samejob is dj

    with pytest.raises(KeyError, match="Job 'peter' not found!"):
        joblist.get_job("peter")

    samejob2 = joblist[0]
    assert samejob2 is dj


def test_queue_condensed():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    joblist = DownloadQueue(api=api)
    resource_id = ds_dict["resources"][0]["id"]
    dj1 = joblist.new_job(resource_id=resource_id,
                          download_path=td,
                          condensed=False)
    dj2 = joblist.new_job(resource_id=resource_id,
                          download_path=td,
                          condensed=True)
    assert dj1.job_id != dj2.job_id
    assert not dj1.condensed
    assert dj2.condensed
    dpath1 = dj1.get_download_path()
    dpath2 = dj2.get_download_path()
    assert str(dpath1.parent) == str(dpath2.parent)
    assert dpath1.stem + "_condensed" == dpath2.stem
    assert dpath1.suffix == dpath2.suffix
    assert dj1 in joblist
    assert dj2 in joblist


def test_queue_remove_job():
    """Remove a job from the queue and from the persistent list"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_dj_list_"))
    pdjl_path = td / "joblistdir"
    # create some metadata
    ds_dict = common.make_dataset_for_download()
    joblist = DownloadQueue(api=api,
                            path_persistent_job_list=pdjl_path)
    # disable all daemons, so no downloading happens
    joblist.daemon_download.shutdown_flag.set()
    joblist.daemon_verify.shutdown_flag.set()
    time.sleep(.2)
    resource_id = ds_dict["resources"][0]["id"]
    dj = joblist.new_job(resource_id=resource_id,
                         download_path=td)
    assert dj.state == "init"
    joblist.remove_job(resource_id)
    assert dj not in joblist
    assert not joblist.jobs_eternal.job_exists(dj)

    # adding it again should work
    dj2 = joblist.new_job(resource_id=resource_id,
                          download_path=td)
    assert dj2 in joblist
    assert dj2.__getstate__() == dj.__getstate__()


def test_persistent_download_joblist_basic():
    """basic job tests"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_dj_list_"))
    pdjl_path = td / "joblistdir"
    # create some metadata
    ds_dict = common.make_dataset_for_download(seed="basic")
    joblist = DownloadQueue(api=api)
    resource_id = ds_dict["resources"][0]["id"]
    dj = joblist.new_job(resource_id=resource_id,
                         download_path=td)
    pdjl = PersistentDownloadJobList(pdjl_path)

    # add a job
    pdjl.immortalize_job(dj)
    assert dj in pdjl

    # find that job
    cur_jobs = pdjl.get_queued_jobs(api)
    dj_same = cur_jobs[0]
    assert dj_same is not dj, "not same instance"
    assert dj_same.__getstate__() == dj.__getstate__(), "same data"

    # remove a job
    assert pdjl.job_exists(dj)
    assert pdjl.is_job_queued(dj)
    pdjl.obliterate_job(dj)
    assert dj not in pdjl
    assert not pdjl.job_exists(dj)


def test_persistent_download_joblist_job_added_in_queue():
    """Test whether queuing works"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_dj_list_"))
    pdjl_path = td / "joblistdir"
    # create some metadata
    ds_dict = common.make_dataset_for_download(seed="job_added_in_queue")
    joblist = DownloadQueue(api=api)
    resource_id = ds_dict["resources"][0]["id"]
    dj = joblist.new_job(resource_id=resource_id,
                         download_path=td)
    pdjl = PersistentDownloadJobList(pdjl_path)

    uq = DownloadQueue(api=api, path_persistent_job_list=pdjl_path)
    uq.daemon_download.shutdown_flag.set()
    uq.daemon_verify.shutdown_flag.set()
    time.sleep(.2)

    assert pdjl.num_queued == 0
    uq.add_job(dj)
    assert pdjl.num_queued == 1


def test_persistent_download_joblist_error_exists():
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_dj_list_"))
    pdjl_path = td / "joblistdir"
    ds_dict = common.make_dataset_for_download()
    pdjl = PersistentDownloadJobList(pdjl_path)
    joblist = DownloadQueue(api=api)
    resource_id = ds_dict["resources"][0]["id"]
    dj = joblist.new_job(resource_id=resource_id,
                         download_path=td)
    pdjl.immortalize_job(dj)
    with pytest.raises(FileExistsError, match="already present at"):
        pdjl.immortalize_job(dj)


def test_persistent_download_joblist_skip_queued_resources():
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_dj_list_"))
    pdjl_path = td / "joblistdir"
    ds_dict = common.make_dataset_for_download()
    pdjl = PersistentDownloadJobList(pdjl_path)
    joblist1 = DownloadQueue(api=api)
    resource_id = ds_dict["resources"][0]["id"]
    dj = joblist1.new_job(resource_id=resource_id,
                          download_path=td)
    pdjl.immortalize_job(dj)

    dq = DownloadQueue(api=api, path_persistent_job_list=pdjl_path)
    dq.daemon_download.shutdown_flag.set()
    dq.daemon_verify.shutdown_flag.set()
    time.sleep(.2)

    assert len(dq) == 1
    assert dq.jobs_eternal.num_queued == 1

    # sanity check
    assert pdjl.is_job_queued(dj)

    same_job = DownloadJob.from_download_job_state(dj.__getstate__(), api=api)
    dq.add_job(same_job)
    assert len(dq) == 1
    assert dq.jobs_eternal.num_queued == 1
