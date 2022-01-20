import pathlib
import tempfile
from dcoraid.download import job, task

from . import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_save_load_basic():
    api = common.get_api()
    td = tempfile.mkdtemp(prefix="test-download")
    task_path = pathlib.Path(td) / "test.json"
    ds_dict = common.make_dataset_for_download()
    dj = job.DownloadJob(api=api,
                         resource_id=ds_dict["resources"][0]["id"],
                         download_path=td)
    task.save_task(dj, task_path)

    dj2 = task.load_task(task_path, api=api)
    assert dj2.resource_id == dj.resource_id
