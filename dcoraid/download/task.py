import json
import pathlib

from .job import DownloadJob


def save_task(download_job, path):
    """Save a download job to a JSON file

    Parameters
    ----------
    download_job: dcoraid.download.job.DownloadJob
        Download job from which to create a snapshot
    path: str or pathlib.Path
        Output path
    """
    path = pathlib.Path(path)
    dj_state = {"download_job": download_job.__getstate__()}
    with path.open("w") as fd:
        json.dump(dj_state, fd,
                  ensure_ascii=False,
                  indent=2,
                  sort_keys=True,
                  )


def load_task(path, api):
    """Open a task file and load it into a DownloadJob

    Parameters
    ----------
    path: str or pathlib.Path
        Path to the JSON-encoded task file
    api: dcoraid.api.CKANAPI
        The CKAN/DCOR API instance used for the download
    """
    path = pathlib.Path(path)
    state = json.loads(path.read_text())
    return DownloadJob.from_download_job_state(state["download_job"], api=api)
