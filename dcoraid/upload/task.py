"""Manage `UploadJob`s using json files

An upload "task" is like an item on a todo list. It has a
unique task ID, which is different from the CKAN/DCOR dataset ID.
DCOR-Aid can keep track of dataset IDs and task IDs to avoid
uploading the same dataset twice.

Once a task is imported in DCOR-Aid, it becomes an
:class:`dcoraid.upload.UploadJob`. An upload job
can be converted back to a task using hte :func:`save_task`
method. The new task now automatically has a dataset ID
(given to it by CKAN/DCOR).
"""
import json
import pathlib

from .dataset import create_dataset
from .job import UploadJob


def save_task(upload_job, path):
    """Save an upload job to a JSON file

    Parameters
    ----------
    upload_job: dcoraid.upload.job.UploadJob
        Upload job from which to create a snapshot
    path: str or pathlib.Path
        Output path
    """
    path = pathlib.Path(path)
    uj_state = upload_job.__getstate__()
    with path.open("w") as fd:
        json.dump(uj_state, fd,
                  ensure_ascii=False,
                  indent=2,
                  sort_keys=True,
                  )


def load_task(path, api, dataset_kwargs=None, map_task_to_dataset_id=None):
    """Open a task file and load it into an UploadJob

    Parameters
    ----------
    path: str or pathlib.Path
        Path to the JSON-encoded task file
    api: dclab.api.CKANAPI
        The CKAN/DCOR API instance used for the upload
    dataset_kwargs: dict
        Any additional dataset kwargs with which the "dataset_dict"
        dictionary in the task file should be updated. Note that
        it only make sense to change things like "owner_org" or
        "private" - you should e.g. never edit resource information
        (which will fail in an undefined manner).
    map_task_to_dataset_id: dict
        Dictionary-like object that maps previously encountered
        task IDs to CKAN/DCOR dataset IDs. This is only used
        if there is no dataset ID in the task file. Use this if
        you want to avoid uploading duplicate datasets.

    Returns
    -------
    upload_job: dcoraid.upload.job.UploadJob
        The corresponding upload job
    """
    path = pathlib.Path(path)
    with path.open() as fd:
        uj_state = json.load(fd)

    if dataset_kwargs is not None:
        uj_state["dataset_dict"].update(dataset_kwargs)

    dataset_id = uj_state["dataset_dict"].get("id")

    if dataset_id is None:
        # If no dataset ID is specified, then we *have* to have
        # a task ID.
        task_id = uj_state.get("task_id")
        if task_id is None:
            raise ValueError(f"The task file '{path}' must contain 'task_id' "
                             "or 'dataset_dict/id'!")
        if map_task_to_dataset_id is not None:
            # only call "get" once, because otherwise this may take
            # too long for large dictionaries or file-system based
            # implementations of `map_task_to_dataset_id`.
            dataset_id = map_task_to_dataset_id.get(task_id)
        if dataset_id is None:
            # The task_id was not in the dictionary which means we have
            # to create a draft dataset first.
            ddict = create_dataset(
                dataset_dict=uj_state["dataset_dict"],
                api=api,
                activate=False)
            dataset_id = ddict["id"]
        # Finally, set the dataset ID
        uj_state["dataset_dict"]["id"] = dataset_id

    # Proceed with instantiation of UploadJob
    uj = UploadJob.from_upload_job_state(uj_state, api=api)
    return uj
