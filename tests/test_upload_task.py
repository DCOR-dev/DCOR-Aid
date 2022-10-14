import json
import pathlib
import shutil
import tempfile
import uuid

import pytest

import dcoraid
import dcoraid.api
from dcoraid.api import dataset_create
from dcoraid.upload import job, task

from . import common


dpath = pathlib.Path(__file__).parent / "data" / "calibration_beads_47.rtdc"


def test_create_task():
    tdir = pathlib.Path(tempfile.mkdtemp(prefix="create_task"))
    path_rtdc = tdir / dpath.name
    shutil.copy2(dpath, path_rtdc)
    path_task = tdir / "test.dcoraid-task"
    path_text = tdir / "green.txt"
    path_text.write_text("Peter!")
    dcoraid.create_task(
        path=path_task,
        dataset_dict={"authors": "Bruce Banner",
                      "license_id": "CC0-1.0",
                      "owner_org": common.CIRCLE},
        resource_dicts=[{"path": path_rtdc,
                         "name": "hulk.rtdc",
                         "supplements": {"chip": {"comments": "Loki bash"}}},
                        {"path": path_text}
                        ]
    )
    # Upload the task
    api = common.get_api()
    uj = task.load_task(path_task, api=api)
    uj.task_compress_resources()
    uj.task_upload_resources()
    uj.task_verify_resources()
    common.wait_for_job_no_queue(uj)
    # Now make sure that all the information is still there
    ddict = api.get("package_show", id=uj.dataset_id)
    assert ddict["authors"] == "Bruce Banner"
    assert ddict["license_id"] == "CC0-1.0"
    assert ddict["resources"][0]["name"] == "hulk.rtdc"
    assert ddict["resources"][0]["sp:chip:comments"] == "Loki bash"
    assert ddict["resources"][1]["name"] == "green.txt"
    assert ddict["resources"][1]["size"] == 6


def test_custom_dataset_dict():
    api = common.get_api()
    # post dataset creation request
    task_path = common.make_upload_task(dataset_dict=False,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name])
    dataset_dict = common.make_dataset_dict()
    dataset_dict["authors"] = "Captain Hook!"
    uj = task.load_task(task_path, api=api,
                        dataset_kwargs=dataset_dict)
    # now make sure the authors were set correctly
    ddict = api.get("package_show", id=uj.dataset_id)
    assert ddict["authors"] == "Captain Hook!"


def test_custom_dataset_dict_2():
    api = common.get_api()
    # post dataset creation request
    task_path = common.make_upload_task(dataset_dict=True,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name])
    dataset_dict = common.make_dataset_dict()
    dataset_dict["authors"] = "Captain Hook!"
    uj = task.load_task(task_path, api=api,
                        dataset_kwargs=dataset_dict)
    # now make sure the authors were set correctly
    ddict = api.get("package_show", id=uj.dataset_id)
    assert ddict["authors"] == "Captain Hook!"


def test_dataset_id_already_exists_active_fails():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api,
                                          activate=True)
    # create a new task with the same dataset ID but with different data
    task_path = common.make_upload_task(
        dataset_dict=dataset_dict_with_id,
        resource_paths=[str(dpath), str(dpath)],
        resource_names=["1.rtdc", "2.rtdc"])
    uj = task.load_task(task_path, api=api)
    assert len(uj.paths) == 2
    assert len(uj.resource_names) == 2
    assert uj.dataset_id == dataset_dict_with_id["id"]
    # attempt to upload the task
    uj.task_compress_resources()
    assert uj.state == "parcel"
    with pytest.raises(dcoraid.api.APIAuthorizationError,
                       match=""):
        uj.task_upload_resources()


def test_dataset_id_does_not_exist():
    api = common.get_api()
    # create a fake ID
    dataset_id = str(uuid.uuid4())
    # create a new task with the fake dataset ID
    task_path = common.make_upload_task(dataset_id=dataset_id)
    # create the upload job
    with pytest.raises(dcoraid.api.APINotFoundError,
                       match=dataset_id):
        task.load_task(task_path, api=api)


def test_load_basic():
    api = common.get_api()
    task_path = common.make_upload_task(task_id="zpowiemsnh",
                                        resource_names=["humdinger.rtdc"])
    assert task.task_has_circle(task_path)
    uj = task.load_task(task_path, api=api)
    assert uj.task_id == "zpowiemsnh"
    assert uj.resource_names == ["humdinger.rtdc"]


def test_load_with_existing_dataset():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)
    task_path = common.make_upload_task(dataset_dict=dataset_dict_with_id,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name])
    uj = task.load_task(task_path, api=api)
    assert uj.dataset_id == dataset_dict_with_id["id"]
    # skipping the upload should work, since it's already uploaded
    uj.set_state("online")
    uj.task_verify_resources()

    common.wait_for_job_no_queue(uj)


def test_load_with_existing_dataset_map_from_task():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)
    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id="xwing")
    uj = task.load_task(
        task_path,
        api=api,
        map_task_to_dataset_id={"xwing": dataset_dict_with_id["id"]})
    assert uj.dataset_id == dataset_dict_with_id["id"]


def test_load_with_existing_dataset_map_from_task_dict_update():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id="xwing")
    map_task_to_dataset_id = {}
    uj = task.load_task(
        task_path,
        api=api,
        map_task_to_dataset_id=map_task_to_dataset_id)
    assert uj.task_id == "xwing"
    assert map_task_to_dataset_id["xwing"] == uj.dataset_id


def test_load_with_existing_dataset_map_from_task_control():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)
    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id="xwing")
    uj = task.load_task(
        task_path,
        api=api,
        map_task_to_dataset_id={"deathstar": dataset_dict_with_id["id"]})
    assert uj.dataset_id != dataset_dict_with_id["id"]


def test_load_with_existing_dataset_id_does_not_exist_using_dataset_id_kwarg():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)

    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        dataset_id="wrong_id",
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id="xwing")

    # The dataset_id "wrong_id" does not exist on DCOR.
    with pytest.raises(dcoraid.api.APINotFoundError,
                       match="You may force the creation of a new"):
        task.load_task(
            task_path,
            api=api)

    # A new dataset_id is generated here
    uj2 = task.load_task(
        task_path,
        api=api,
        force_dataset_creation=True
    )
    assert uj2.dataset_id != dataset_dict_with_id["id"]
    assert uj2.dataset_id != "wrong_id", "sanity check"


def test_load_with_existing_dataset_id_does_not_exist_using_load_dict():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)

    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id="xwing")

    # The dataset_id "wrong_id" does not exist on DCOR.
    with pytest.raises(dcoraid.api.APINotFoundError,
                       match="You may force the creation of a new"):
        task.load_task(
            task_path,
            dataset_kwargs={"id": "wrong_id"},
            api=api)

    # A new dataset_id is generated here
    uj2 = task.load_task(
        task_path,
        api=api,
        force_dataset_creation=True
    )
    assert uj2.dataset_id != dataset_dict_with_id["id"]
    assert uj2.dataset_id != "wrong_id", "sanity check"


def test_load_with_existing_dataset_id_does_not_exist_using_persistent_dict():
    tmp_dir = pathlib.Path(tempfile.mkdtemp(prefix="test_task_persit_"))
    tmp_path = tmp_dir / "persistent_dict.txt"
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)

    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        dataset_id="unexistent_id",
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id="xwing")
    map_dict = task.PersistentTaskDatasetIDDict(tmp_path)
    map_dict["xwing"] = "unexistent_id"

    with pytest.raises(dcoraid.api.APINotFoundError,
                       match="You may force the creation of a new"):
        task.load_task(
            task_path,
            map_task_to_dataset_id=map_dict,
            api=api)

    # The dataset_id "wrong_id" does not exist on DCOR.
    uj = task.load_task(
        task_path,
        map_task_to_dataset_id=map_dict,
        force_dataset_creation=True,
        api=api)

    assert uj.dataset_id != dataset_dict_with_id["id"]
    assert uj.dataset_id != "wrong_id", "sanity check"


def test_load_with_existing_dataset_id_does_not_exist_using_task_dict():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    # post dataset creation request
    dataset_dict_with_id = dataset_create(dataset_dict=dataset_dict,
                                          resources=[dpath],
                                          api=api)
    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id="xwing")

    # The dataset_id "wrong_id" does not match the task_id in dict.
    with pytest.raises(dcoraid.api.APINotFoundError,
                       match="You may force the creation of a new"):
        task.load_task(
            task_path,
            api=api,
            map_task_to_dataset_id={"xwing": "wrong_id"})

    # A new dataset_id is generated here
    uj2 = task.load_task(
        task_path,
        api=api,
        map_task_to_dataset_id={"xwing": "wrong_id"},
        force_dataset_creation=True
    )
    assert uj2.dataset_id != dataset_dict_with_id["id"]
    assert uj2.dataset_id != "wrong_id", "sanity check"


def test_load_with_update():
    api = common.get_api()
    task_path = common.make_upload_task(task_id="blackfalcon",
                                        resource_names=["marvel.rtdc"])
    assert task.task_has_circle(task_path)
    uj = task.load_task(task_path, api=api, update_dataset_id=True)
    assert uj.task_id == "blackfalcon"
    assert uj.resource_names == ["marvel.rtdc"]
    # Load task and check dataset_id
    with open(task_path) as fd:
        task_dict = json.load(fd)
        assert task_dict["dataset_dict"]["id"] == uj.dataset_id


def test_missing_owner_org():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    dataset_dict.pop("owner_org")
    task_path = common.make_upload_task(dataset_dict=dataset_dict)
    assert not task.task_has_circle(task_path)
    with pytest.raises(dcoraid.api.APIConflictError,
                       match="Datasets must always be uploaded to a Circle."):
        task.load_task(task_path, api=api)


def test_no_ids():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name],
                                        task_id=None)
    with pytest.raises(ValueError,
                       match="or pass the dataset_id via the dataset_kwargs"):
        task.load_task(task_path, api=api)


def test_persistent_dict():
    tdir = tempfile.mkdtemp(prefix="upload_task_persistent_dict_")
    path = pathlib.Path(tdir) / "persistent_dict.txt"
    pd = task.PersistentTaskDatasetIDDict(path)
    pd["hans"] = "peter"
    pd["golem"] = "odin123"
    assert pd["hans"] == "peter"
    assert pd["golem"] == "odin123"
    assert pd.get("fritz") is None

    pd2 = task.PersistentTaskDatasetIDDict(path)
    assert pd2["hans"] == "peter"
    assert pd2["golem"] == "odin123"


def test_persistent_dict_bad_task_id():
    tdir = tempfile.mkdtemp(prefix="upload_task_persistent_dict_")
    path = pathlib.Path(tdir) / "persistent_dict.txt"
    pd = task.PersistentTaskDatasetIDDict(path)
    with pytest.raises(ValueError, match="task IDs may only contain"):
        pd["hans!"] = "peter"
    # make sure it is not stored
    pd2 = task.PersistentTaskDatasetIDDict(path)
    assert "hans!" not in pd2


def test_persistent_dict_override_forbidden():
    tdir = tempfile.mkdtemp(prefix="upload_task_persistent_dict_")
    path = pathlib.Path(tdir) / "persistent_dict.txt"
    pd = task.PersistentTaskDatasetIDDict(path)
    pd["peter"] = "pan"
    assert "peter" in pd
    assert pd["peter"] == "pan"
    pd["peter"] = "pan"  # once again
    with pytest.raises(ValueError, match="Cannot override entries"):
        pd["peter"] = "hook"
    # make sure it is not stored
    pd2 = task.PersistentTaskDatasetIDDict(path)
    assert pd2["peter"] == "pan"


def test_persistent_dict_override_method():
    tdir = tempfile.mkdtemp(prefix="upload_task_persistent_dict_")
    path = pathlib.Path(tdir) / "persistent_dict.txt"
    pd = task.PersistentTaskDatasetIDDict(path)
    pd["tony"] = "stark"
    pd["peter"] = "pan"
    pd["captain"] = "america"
    assert "peter" in pd
    assert pd["peter"] == "pan"
    pd.override_entry("peter", "hook")
    # make sure this worked
    assert pd["peter"] == "hook"
    assert pd["tony"] == "stark"
    assert pd["captain"] == "america"
    # make sure it's written to disk
    pd2 = task.PersistentTaskDatasetIDDict(path)
    assert pd2["peter"] == "hook"
    assert pd2["tony"] == "stark"
    assert pd2["captain"] == "america"


def test_resource_name_lengths():
    """Make sure ValueError is raised when list lengths do not match"""
    task_path = common.make_upload_task(
        resource_paths=[__file__, dpath],
        resource_names=["other_data.rtdc"],
        resource_supplements=[{},
                              {"chip": {"name": "7x2",
                                        "master name": "R1"}}])
    with pytest.raises(ValueError,
                       match="does not match number of resource names"):
        task.load_task(task_path, api=common.get_api())


def test_resource_path_is_relative():
    task_path = common.make_upload_task(resource_paths=["guess_my_name.rtdc"])
    new_data_path = pathlib.Path(task_path).parent / "guess_my_name.rtdc"
    shutil.copy2(dpath, new_data_path)
    uj = task.load_task(task_path, api=common.get_api())
    assert new_data_path.samefile(uj.paths[0])


def test_resource_path_not_found():
    task_path = common.make_upload_task(resource_paths=["/home/unknown.rtdc"])
    with pytest.raises(task.LocalTaskResourcesNotFoundError,
                       match="is missing local resources files"):
        task.load_task(task_path, api=common.get_api())


def test_resource_supplements():
    task_path = common.make_upload_task(
        resource_paths=[dpath],
        resource_supplements=[{"chip": {"name": "7x2",
                                        "master name": "R1"}}])
    uj = task.load_task(task_path, api=common.get_api())
    assert uj.supplements[0]["chip"]["name"] == "7x2"
    assert uj.supplements[0]["chip"]["master name"] == "R1"


def test_resource_supplements_must_be_empty_for_non_rtdc():
    task_path = common.make_upload_task(
        resource_paths=[__file__, dpath],
        resource_names=["test.py", "other_data.rtdc"],
        resource_supplements=[{"chip": {"name": "7x2",
                                        "master name": "R1"}},
                              {"chip": {"name": "7x2",
                                        "master name": "R1"}}
                              ])
    with pytest.raises(ValueError, match="supplements must be empty"):
        task.load_task(task_path, api=common.get_api())


def test_resource_supplements_with_other_files():
    task_path = common.make_upload_task(
        resource_paths=[__file__, dpath],
        resource_names=["test.py", "other_data.rtdc"],
        resource_supplements=[{},
                              {"chip": {"name": "7x2",
                                        "master name": "R1"}}])
    uj = task.load_task(task_path, api=common.get_api())
    assert len(uj.supplements[0]) == 0


def test_resource_supplements_lengths():
    """Make sure ValueError is raised when list lengths do not match"""
    task_path = common.make_upload_task(
        resource_paths=[__file__, dpath],
        resource_names=["test.py", "other_data.rtdc"],
        resource_supplements=[{"chip": {"name": "7x2",
                                        "master name": "R1"}}])
    with pytest.raises(ValueError,
                       match="does not match number of resource supplements"):
        task.load_task(task_path, api=common.get_api())


def test_save_load():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[dpath], task_id="hanspeter")
    td = pathlib.Path(tempfile.mkdtemp(prefix="task_"))
    task_path = td / "test.dcoraid-task"
    task.save_task(uj, path=task_path)
    uj2 = task.load_task(task_path, api=api)
    assert uj.dataset_id == uj2.dataset_id
    assert uj.paths[0].samefile(uj2.paths[0])


def test_wrong_ids():
    api = common.get_api()
    # create some metadata
    dataset_dict = common.make_dataset_dict(hint="task_test")
    dataset_dict["id"] = "peter"
    task_path = common.make_upload_task(dataset_dict=dataset_dict,
                                        dataset_id="hans",  # different id
                                        resource_paths=[str(dpath)],
                                        resource_names=[dpath.name])
    with pytest.raises(ValueError,
                       match="I got the following IDs: from upload job "
                             + "state: hans; from dataset dict: peter"):
        task.load_task(task_path, api=api)


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
