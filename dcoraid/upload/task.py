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


class LocalTaskResourcesNotFoundError(FileNotFoundError):
    def __init__(self, missing_resources, *args, **kwargs):
        self.missing_resources = missing_resources
        super(LocalTaskResourcesNotFoundError, self).__init__(*args, **kwargs)


class PersistentTaskDatasetIDDict:
    def __init__(self, path):
        """A file-based dictionary with task_id as keys

        Features:

        - Not possible to override any keys with other values
        - allowed key characters are
          "0123456789-_abcdefghijklmnopqrstuvwxyz"
        - New keys are appended to the end of the file
        - No file locking (make sure that only one thread
          appends to the file)
        - Dictionary is loaded into memory on init, then for
          each new entry, the internal dictionary is updated
          and a new line is appended to the file on disk
        """
        self._path = pathlib.Path(path)
        self._path.touch(exist_ok=True)
        self._dict = {}
        # load the dictionary
        with self._path.open() as fd:
            lines = fd.readlines()
            for line in lines:
                task_id, dataset_id = line.strip().split()
                self._dict[task_id] = dataset_id

    def __contains__(self, task_id):
        return self._dict.__contains__(task_id)

    def __setitem__(self, task_id, dataset_id):
        if task_id in self._dict:
            if self[task_id] != dataset_id:
                raise ValueError("Cannot override entries in persistent dict!")
            else:
                # everything ok
                pass  # this line is covered, even though coverage disagrees
        else:
            valid_ch = "0123456789-_abcdefghijklmnopqrstuvwxyz"
            task_id_check = "".join([ch for ch in task_id if ch in valid_ch])
            if task_id_check != task_id:
                raise ValueError("task IDs may only contain numbers, "
                                 "lower-case characters, and '-_'."
                                 f"Got '{task_id}'!")
            # append to end of file
            with self._path.open("a") as fd:
                fd.write(f"{task_id} {dataset_id}\n")
            # append to dict
            self._dict[task_id] = dataset_id

    def __getitem__(self, task_id):
        return self._dict[task_id]

    def get(self, task_id, default=None):
        return self._dict.get(task_id, default)


def save_task(upload_job, path, dataset_dict=None):
    """Save an upload job to a JSON file

    Parameters
    ----------
    upload_job: dcoraid.upload.job.UploadJob
        Upload job from which to create a snapshot
    path: str or pathlib.Path
        Output path
    dataset_dict: dict
        An optional dataset dictionary. This dictionary is
        purely informative; it only contains redundant
        information (which is stored on the DCOR server).
    """
    path = pathlib.Path(path)
    uj_state = {"upload_job": upload_job.__getstate__()}
    if dataset_dict:
        uj_state["dataset_dict"] = dataset_dict
    with path.open("w") as fd:
        json.dump(uj_state, fd,
                  ensure_ascii=False,
                  indent=2,
                  sort_keys=True,
                  )


def load_task(path, api, dataset_kwargs=None, map_task_to_dataset_id=None,
              update_dataset_id=False, cache_dir=None):
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
    map_task_to_dataset_id: dict or PersistentTaskDatasetIDDict
        Dictionary-like object that maps previously encountered
        task IDs to CKAN/DCOR dataset IDs. This is only used
        if there is no dataset ID in the task file. Use this if
        you want to avoid uploading duplicate datasets. If task_id
        is given in the task file, then map_task_to_dataset_id will
        be updated with the dataset_id.
    update_dataset_id: bool
        If True, update the task file with the dataset identifier
        assigned to by the CKAN/DCOR server.
    cache_dir: str or pathlib.Path
        Cache directory for storing compressed .rtdc files;
        if not supplied, a temporary directory is created

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
        # separate "dataset_dict" from the task file
        # (e.g. the dataset_id and other things might be in here)
        dataset_dict = task_dict["dataset_dict"]
    else:
        dataset_dict = {}

    if dataset_kwargs is not None:
        dataset_dict.update(dataset_kwargs)

    uj_state = task_dict["upload_job"]

    # make sure the paths exist (if not, try with name relative to task path)
    missing_resources = []
    for ii in range(len(uj_state["resource_paths"])):
        pi = pathlib.Path(uj_state["resource_paths"][ii])
        pi_alt = pathlib.Path(path).parent / pi.name
        if pi.exists():
            pass
        elif pi_alt.exists():
            # replace with path relative to task file
            uj_state["resource_paths"][ii] = str(pi_alt)
        else:
            missing_resources.append(pi)

    if missing_resources:
        resstr = ", ".join([str(pp) for pp in missing_resources])
        raise LocalTaskResourcesNotFoundError(
            missing_resources,
            f"Task '{path}' is missing local resources files: {resstr}")

    num_res = len(uj_state["resource_paths"])
    if (uj_state["resource_names"]
            and len(uj_state["resource_names"]) != num_res):
        raise ValueError("Number of resource paths does not match number "
                         f"of resource names in '{path}'!")
    if uj_state["resource_supplements"]:
        if len(uj_state["resource_supplements"]) != num_res:
            raise ValueError("Number of resource paths does not match number "
                             f"of resource supplements in '{path}'!")
        for ii, pp in enumerate(uj_state["resource_paths"]):
            if (not str(pp).endswith(".rtdc")
                    and uj_state["resource_supplements"][ii]):
                raise ValueError("Resource supplements must be empty for "
                                 f"non-RT-DC datasets in '{path}'!")

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
                f"Neither task_id nor dataset_id are specified in '{path}'! "
                "Please update the task file or pass the dataset_id via "
                "the dataset_kwargs argument.")
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
            + (f"from ID dict: {id_m}; " if id_m is not None else "")
            + "Please check your input data!"
        )

    # Finally, set the dataset ID
    uj_state["dataset_id"] = dataset_id
    dataset_dict["id"] = dataset_id

    # Proceed with instantiation of UploadJob
    uj = UploadJob.from_upload_job_state(uj_state, api=api,
                                         cache_dir=cache_dir)

    if task_id is not None:
        # Also update the ID dictionary
        # This part here should come after instantiation of UploadJob,
        # because that just might fail if the specified dataset_id is invalid
        # (and we don't want invalid IDs in our dictionary).
        map_task_to_dataset_id[task_id] = dataset_id

    if update_dataset_id:
        # If everything went well so far, then we can safely update the
        # original json file with the new dictionary that contains
        # the dataset id.
        save_task(uj, path, dataset_dict=dataset_dict)

    return uj


def task_has_circle(path):
    """Check whether a circle (owner_org) is specified in a task file"""
    path = pathlib.Path(path)
    with path.open() as fd:
        task_dict = json.load(fd)
    has_org = ("owner_org" in task_dict.get("dataset_dict", {})
               and task_dict["dataset_dict"]["owner_org"])
    return has_org
