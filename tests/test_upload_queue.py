import pathlib
import tempfile
import uuid

import pytest

from dcoraid.upload import UploadQueue, create_dataset
from dcoraid.upload.queue import DCORAidQueueWarning, PersistentUploadJobList
from dcoraid.upload.task import load_task

import common


data_path = pathlib.Path(__file__).parent / "data"
dpath = data_path / "calibration_beads_47.rtdc"


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
    common.wait_for_job(joblist, data["id"])


def test_queue_find_zombie_caches():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="create-with-resource")
    # post dataset creation request
    data = create_dataset(dataset_dict=dataset_dict, api=api)
    # post dataset creation request
    cache_dir = tempfile.mkdtemp("dcoraid_test_upload_cache_")
    fakecache = pathlib.Path(cache_dir) / f"compress-{uuid.uuid4()}"
    fakecache.mkdir()
    realcache = pathlib.Path(cache_dir) / f"compress-{data['id']}"
    joblist = UploadQueue(api=api, cache_dir=cache_dir)
    # disable all daemons, so no uploading happens
    joblist.daemon_compress.join()
    joblist.daemon_upload.join()
    joblist.daemon_verify.join()
    uj = joblist.new_job(
        dataset_id=data["id"],
        paths=[data_path / "calibration_beads_47_nocomp.rtdc"])
    assert uj.state == "init"
    zombies = joblist.find_zombie_caches()
    assert fakecache in zombies
    assert realcache not in zombies
    assert len(zombies) == 1


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


def test_persistent_upload_joblist_job_added_in_queue():
    """Test whether queuing works"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)

    uq = UploadQueue(api=api, path_persistent_job_list=pujl_path)
    uq.daemon_compress.join()
    uq.daemon_upload.join()
    uq.daemon_verify.join()

    assert pujl.num_queued == 0
    uq.add_job(uj)
    assert pujl.num_queued == 1
    assert pujl.num_completed == 0


def test_persistent_upload_joblist_job_added_and_finished_in_queue():
    """Test whether queuing works"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)

    uq = UploadQueue(api=api, path_persistent_job_list=pujl_path)

    assert pujl.num_queued == 0
    uq.add_job(uj)

    common.wait_for_job(uq, uj.dataset_id)

    assert pujl.num_queued == 0
    assert pujl.num_completed == 1


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
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)
    pujl.immortalize_job(uj)
    with pytest.raises(FileExistsError, match="already present at"):
        pujl.immortalize_job(uj)


def test_persistent_upload_joblist_skip_finished_resources():
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = pathlib.Path(common.make_upload_task())
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path,
                   api=api,
                   update_dataset_id=True)
    pujl.immortalize_job(uj)

    uq = UploadQueue(api=api, path_persistent_job_list=pujl_path)
    assert len(uq) == 1
    assert uq.jobs_eternal.num_queued == 1

    common.wait_for_job(uq, uj.dataset_id)

    # sanity check
    assert pujl.is_job_done(uj.dataset_id)

    # try to add the task again
    same_job = load_task(task_path,
                         api=api,
                         update_dataset_id=True)
    uq.add_job(same_job)
    assert len(uq) == 1
    assert uq.jobs_eternal.num_queued == 0


def test_persistent_upload_joblist_skip_missing_resources():
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = pathlib.Path(common.make_upload_task())
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path,
                   api=api,
                   update_dataset_id=True)
    pujl.immortalize_job(uj)

    # hide the original data directory so loading the task fails
    task_dir = task_path.parent
    temp_task_dir = task_path.parent.with_name("test_rename_temp")
    task_dir.rename(temp_task_dir)
    assert not task_dir.exists()
    with pytest.warns(DCORAidQueueWarning, match="resources are missing"):
        uq = UploadQueue(api=api, path_persistent_job_list=pujl_path)
    uq.daemon_compress.join()
    uq.daemon_upload.join()
    uq.daemon_verify.join()
    # sanity checks
    assert len(uq) == 0
    assert uq.jobs_eternal.num_queued == 1
    # now add the new task
    same_job = load_task(temp_task_dir / task_path.name,
                         api=api,
                         update_dataset_id=True)
    uq.add_job(same_job)
    assert len(uq) == 1
    assert uq.jobs_eternal.num_queued == 1


def test_persistent_upload_joblist_skip_queued_resources():
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = pathlib.Path(common.make_upload_task())
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path,
                   api=api,
                   update_dataset_id=True)
    pujl.immortalize_job(uj)

    uq = UploadQueue(api=api, path_persistent_job_list=pujl_path)
    uq.daemon_compress.join()
    uq.daemon_upload.join()
    uq.daemon_verify.join()

    assert len(uq) == 1
    assert uq.jobs_eternal.num_queued == 1

    # sanity check
    assert pujl.is_job_queued(uj.dataset_id)

    # try to add the task again
    same_job = load_task(task_path,
                         api=api,
                         update_dataset_id=True)
    uq.add_job(same_job)
    assert len(uq) == 1
    assert uq.jobs_eternal.num_queued == 1


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


def test_persistent_upload_joblist_warning_dataset_deleted_on_server():
    """warning when dataset has been deleted on server"""
    api = common.get_api()
    td = pathlib.Path(tempfile.mkdtemp(prefix="persistent_uj_list_"))
    pujl_path = td / "joblistdir"
    task_path = common.make_upload_task()
    pujl = PersistentUploadJobList(pujl_path)
    uj = load_task(task_path, api=api)
    pujl.immortalize_job(uj)

    # Now change the dataset ID of the job
    new_id = str(uuid.uuid4())
    pj_path_old = pujl.path_queued / (uj.dataset_id + ".json")
    pj_path_new = pujl.path_queued / (new_id + ".json")

    json_data = pj_path_old.read_text(encoding="utf-8")
    json_data = json_data.replace(uj.dataset_id, new_id)
    pj_path_new.write_text(json_data, encoding="utf-8")
    pj_path_old.unlink()

    with pytest.warns(UserWarning, match=f"{new_id} could not be found"):
        UploadQueue(api=api, path_persistent_job_list=pujl_path)


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
