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
    uj_state = {"upload_job": upload_job.__getstate__()}
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
        (which will fail in an undefined manner). Note that if the
        dataset already exists (dataset_id) is defined, then the
        entries in this dictionary have no effect. They only have
        an effect when the dataset has to be created.
    map_task_to_dataset_id: dict
        Dictionary-like object that maps previously encountered
        task IDs to CKAN/DCOR dataset IDs. This is only used
        if there is no dataset ID in the task file. Use this if
        you want to avoid uploading duplicate datasets. If task_id
        is given in the task file, then map_task_to_dataset_id will
        be updated with the dataset_id.

    Returns
    -------
    upload_job: dcoraid.upload.job.UploadJob
        The corresponding upload job
    """
    path = pathlib.Path(path)
    if map_task_to_dataset_id is None:
        # just set to empty dict so the code below may remain simple
        map_task_to_dataset_id = {}

    with path.open() as fd:
        task_dict = json.load(fd)

    if "dataset_dict" in task_dict:
        # separate "dataset_dict" from the
        dataset_dict = task_dict["dataset_dict"]
    else:
        dataset_dict = {}

    if dataset_kwargs is not None:
        dataset_dict.update(dataset_kwargs)

    uj_state = task_dict["upload_job"]

    # determine the dataset id...
    # ...from the upload job state
    id_u = uj_state.get("dataset_id")
    # ...from the dataset_dict
    id_d = dataset_dict.get("id")
    # ...from the shelf using the task_id
    task_id = uj_state.get("task_id")
    if task_id is None:
        id_m = None
        if id_u is None and id_d is None:
            # This is an important point for automation. We can't just
            # have people running around not specifying any task_ids.
            raise ValueError(
                f"If neither task_id or dataset_id are specified in '{path}' "
                "then you must specify a dataset_id via dataset_kwargs.")
    else:
        id_m = map_task_to_dataset_id.get(task_id)

    # use the ids to determine what to do next
    ids = [dd for dd in [id_u, id_d, id_m] if dd is not None]
    if len(set(ids)) == 0:
        # We have no dataset_id, so we create a dataset first
        ddict = create_dataset(
            dataset_dict=dataset_dict,
            api=api,
            activate=False)
        dataset_id = ddict["id"]
    elif len(set(ids)) == 1:
        # We have a unique dataset_id
        dataset_id = ids[0]
    else:
        # Something went wrong when creating the task file or the
        # user is insane.
        raise ValueError(
            "There are ambiguities regarding the dataset_id! I got the "
            + "following IDs: "
            + (f"from upload job state: {id_u}; " if id_u is not None else "")
            + (f"from dataset dict: {id_d}; " if id_d is not None else "")
            + (f"from task id shelve: {id_m}; " if id_m is not None else "")
            + "Please check your input data!"
        )

    # Finally, set the dataset ID
    uj_state["dataset_id"] = dataset_id

    if task_id is None:
        # also update the shelve
        map_task_to_dataset_id[task_id] = dataset_id

    # Proceed with instantiation of UploadJob
    uj = UploadJob.from_upload_job_state(uj_state, api=api)
    return uj
