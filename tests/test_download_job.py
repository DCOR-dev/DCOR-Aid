import tempfile
from unittest import mock

from dcoraid.download import job

from . import common


def test_initialize():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)
    assert dj.state == "init"


def test_download_resume():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)
    dj.task_download_resource()

    # now attempt a resume-download
    # create a truncated temporary file
    orig = dj.path_temp.with_name("orig.rtdc~")
    dj.path_temp.rename(orig)
    with orig.open("rb") as fd_o:
        start_bytes = fd_o.read(100)
    with dj.path_temp.open("wb") as fd_t:
        fd_t.write(start_bytes)

    # try to resume from that temporary file
    dj2 = job.DownloadJob(api=api,
                          resource_id=ds_dict["resources"][0]["id"],
                          download_path=td)
    dj2.task_download_resource()
    dj2.task_verify_resource()
    assert dj2.start_time is not None
    assert dj2.end_time is not None
    assert dj2.path.exists()
    assert dj2.file_bytes_downloaded == dj.file_size - 100
    assert dj2.file_size == dj2.path.stat().st_size


def test_full_download():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)
    assert dj.state == "init"
    dj.task_download_resource()
    assert dj.state == "downloaded"
    assert dj.path_temp.exists()
    assert not dj.path.exists()
    dj.task_verify_resource()
    assert dj.state == "done"
    assert not dj.path_temp.exists()
    assert dj.path.exists()


def test_full_download_file_exists():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)
    assert dj.path.is_dir()
    dj.task_download_resource()
    dj.task_verify_resource()
    assert dj.start_time is not None
    assert dj.end_time is not None
    assert dj.path.exists()
    assert not dj.path.is_dir()

    # now attempt to download the file a second time and make
    # sure that it is not actually downloaded
    dj2 = job.DownloadJob(api=api,
                          resource_id=ds_dict["resources"][0]["id"],
                          download_path=td)
    dj2.task_download_resource()
    dj2.task_verify_resource()
    assert dj2.start_time is None
    assert dj2.end_time is None
    assert dj2.path.exists()
    assert dj2.path.samefile(dj.path)


def test_get_status():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)
    size = dj.get_resource_dict()["size"]
    assert size != 0
    assert dj.get_status()["bytes total"] == size
    assert dj.get_status()["bytes downloaded"] == 0
    assert dj.get_status()["rate"] == 0
    dj.task_download_resource()
    assert dj.get_status()["bytes downloaded"] == size
    assert dj.get_status()["rate"] > 0


def test_saveload():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)

    state = dj.__getstate__()
    assert state["resource_id"] == ds_dict["resources"][0]["id"]

    # now create a new job from the state
    dj2 = job.DownloadJob.from_download_job_state(state, api=api)
    state2 = dj2.__getstate__()
    assert state2["resource_id"] == ds_dict["resources"][0]["id"]
    assert dj2.path.samefile(state["download_path"])


@mock.patch.object(job.shutil, "disk_usage")
def test_state_init_disk_wait(disk_usage_mock):
    """If not space left on disk, download job goes to state "disk-wait"""""
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)
    assert dj.state == "init"

    class DiskUsageReturn1:
        free = 0

    class DiskUsageReturn2:
        free = 1024**3

    disk_usage_mock.side_effect = [DiskUsageReturn1, DiskUsageReturn2]

    dj.task_download_resource()
    assert dj.state == "wait-disk"
    dj.task_download_resource()
    assert dj.state == "downloaded"
    disk_usage_mock.assert_called()
