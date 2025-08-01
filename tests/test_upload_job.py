import logging
import pathlib
import re
import shutil
import tempfile
import time
from unittest import mock
import uuid
import warnings

import pytest

import dclab.cli
import h5py

from dcoraid.api import dataset_create
from dcoraid.upload import job

from . import common


data_path = pathlib.Path(__file__).parent / "data"
rtdc_paths = [data_path / "calibration_beads_47.rtdc",
              data_path / "calibration_beads_47_nocomp.rtdc",
              ]


def test_check_existence_for_dc_resource_1(tmp_path):
    (tmp_path / "test.csv").write_text("This is not a DC resource")
    (tmp_path / "test.txt").write_text("This is not a DC resource as well")

    api = common.get_api()

    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    with pytest.raises(job.AtLeastOneDCResourceRequiredPerDatasetError,
                       match="test.csv"):
        job.UploadJob(api=api,
                      dataset_id=dataset_dict["id"],
                      resource_paths=list(tmp_path.glob("*")))


def test_check_existence_for_dc_resource_2(tmp_path):
    (tmp_path / "test.rtdc").write_text("This is not a DC resource")

    api = common.get_api()

    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    with pytest.raises(job.AtLeastOneDCResourceRequiredPerDatasetError):
        job.UploadJob(api=api,
                      dataset_id=dataset_dict["id"],
                      resource_paths=list(tmp_path.glob("*")))


def test_check_existence_for_dc_resource_control(tmp_path):
    (tmp_path / "test.rtdc").write_text(
        "This is not a DC resource and running the upload job would fail,"
        "but we are only checking UploadJob instantiation here.")
    shutil.copy2(rtdc_paths[0], tmp_path / "test2.rtdc")

    api = common.get_api()

    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api,
                       dataset_id=dataset_dict["id"],
                       resource_paths=list(tmp_path.glob("*")))
    assert len(uj.paths) == 2


def test_collection():
    api = common.get_api()
    # create some metadata
    collection = f"test-collection-{uuid.uuid4()}"
    bare_dict = common.make_dataset_dict(hint="create-with-resource"
                                         )
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api,
                       dataset_id=dataset_dict["id"],
                       resource_paths=rtdc_paths,
                       collections=[collection],
                       )
    assert uj.state == "init"
    uj.task_compress_resources()
    assert uj.state == "parcel"
    uj.task_upload_resources()
    assert uj.state == "online"

    common.wait_for_job_no_queue(uj)

    ds_dict = api.get("package_show", id=uj.dataset_id)
    assert ds_dict["groups"][0]["name"] == collection


def test_resource_name_characters():
    assert re.match(job.VALID_RESOURCE_REGEXP, job.VALID_RESOURCE_CHARS)
    assert re.match(job.VALID_RESOURCE_REGEXP, job.VALID_RESOURCE_CHARS)
    assert re.match(job.VALID_RESOURCE_REGEXP, job.VALID_RESOURCE_CHARS + "0")
    assert not re.match(job.VALID_RESOURCE_REGEXP,
                        job.VALID_RESOURCE_CHARS + "?")
    assert not re.match(job.VALID_RESOURCE_REGEXP,
                        "ä" + job.VALID_RESOURCE_CHARS)
    assert not re.match(job.VALID_RESOURCE_REGEXP,
                        job.VALID_RESOURCE_CHARS + " ")


def test_external_links_not_allowed():
    api = common.get_api()
    # Create a temporary upload directory
    td = pathlib.Path(tempfile.mkdtemp(prefix="dcoraid_external_link_upload_"))
    h5path = td / "peter.rtdc"
    shutil.copy2(rtdc_paths[0], h5path)
    h5path_image = h5path.with_name("image.hdf5")
    # Dataset creation
    with h5py.File(h5path) as src, \
            h5py.File(h5path_image, "w") as h5:
        # write image data to separate file
        h5["image"] = src["/events/image"][:]

    # turn image into an external link
    with h5py.File(h5path, "a") as src:
        del src["/events/image"]
        src["/events/image"] = h5py.ExternalLink(
            str(h5path_image), "image"
        )

    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api,
                       dataset_id=dataset_dict["id"],
                       resource_paths=list(td.glob("*.rtdc")))
    assert uj.state == "init"
    with pytest.raises(IOError, match="The HDF5 file contains at least "
                                      "one external link"):
        uj.task_compress_resources()


def test_initialize():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=rtdc_paths)
    assert uj.state == "init"


def test_initialize_no_api_key(caplog):
    caplog.set_level(logging.WARNING)
    api = common.CKANAPI(server=common.SERVER,
                         api_key="")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert api.is_available(with_api_key=False)
        assert not w
        assert not api.is_available(with_api_key=True)
        assert not w
    # no warnings should be logged
    assert not caplog.records


def test_full_upload():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api,
                       dataset_id=dataset_dict["id"],
                       resource_paths=rtdc_paths)
    assert uj.state == "init"
    uj.task_compress_resources()
    assert uj.state == "parcel"
    uj.task_upload_resources()
    assert uj.state == "online"

    common.wait_for_job_no_queue(uj)


def test_full_upload_uppercase_extension_issue_81(tmp_path):
    """When the file suffix is upper-case, DCOR-Aid shold .lower() it"""
    shutil.copy2(rtdc_paths[0], tmp_path / "peter.RTDC")
    shutil.copy2(rtdc_paths[1], tmp_path / "peter2BIG.rtdc")
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api,
                       dataset_id=dataset_dict["id"],
                       resource_paths=tmp_path.glob("peter*"))
    assert uj.state == "init"
    uj.task_compress_resources()
    assert uj.state == "parcel"
    uj.task_upload_resources()
    assert uj.state == "online"
    common.wait_for_job_no_queue(uj)

    # make sure everything worked
    ds = api.get("package_show", id=uj.dataset_id)
    resources_act = sorted([r["name"] for r in ds["resources"]])
    resources_exp = ["peter.rtdc", "peter2BIG.rtdc"]
    assert resources_act == resources_exp


def test_full_upload_with_file_with_spaces_in_name(tmp_path):
    rtdc_path_with_space = tmp_path / "name questö spaces.rtdc"
    shutil.copy2(rtdc_paths[1], rtdc_path_with_space)
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api,
                       dataset_id=dataset_dict["id"],
                       resource_paths=[rtdc_path_with_space])
    assert uj.state == "init"
    uj.task_compress_resources()
    assert uj.state == "parcel"
    uj.task_upload_resources()
    assert uj.state == "online"

    common.wait_for_job_no_queue(uj)

    # make sure everything worked
    ds = api.get("package_show", id=uj.dataset_id)
    assert ds["resources"][0]["name"] == "name_quest._spaces.rtdc"


def test_saveload():
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=rtdc_paths, task_id="hanspeter")
    state = uj.__getstate__()
    assert state["dataset_id"] == dataset_dict["id"]
    assert rtdc_paths[0].samefile(state["resource_paths"][0])
    assert rtdc_paths[0].name == state["resource_names"][0]

    # now create a new job from the state
    uj2 = job.UploadJob.from_upload_job_state(state, api=api)
    state2 = uj2.__getstate__()
    assert state2["dataset_id"] == dataset_dict["id"]
    assert rtdc_paths[0].samefile(state2["resource_paths"][0])
    assert rtdc_paths[0].name == state2["resource_names"][0]
    assert state2["task_id"] == "hanspeter"


def test_sort_resources_according_to_basin_hierarchy(tmp_path):
    """Make sure resources are sorted by honoring run identifiers"""
    api = common.get_api()

    # Create base paths
    input_paths = [tmp_path / "p1.rtdc",
                   tmp_path / "p2.rtdc",
                   tmp_path / "p3.rtdc",
                   tmp_path / "p4.rtdc"]
    for pp in input_paths:
        shutil.copy2(rtdc_paths[0], pp)

    # Export some of the files
    with dclab.new_dataset(tmp_path / "p3.rtdc") as ds:
        ds.export.hdf5(tmp_path / "ba3.rtdc", basins=True)

    with dclab.new_dataset(tmp_path / "p4.rtdc") as ds:
        ds.export.hdf5(tmp_path / "bb4.rtdc", basins=True)

    # Create a deep hierarchy
    with dclab.new_dataset(tmp_path / "ba3.rtdc") as ds:
        ds.export.hdf5(tmp_path / "b03.rtdc", basins=True)

    # this is how we give it to dcoraid
    input_paths = [
        tmp_path / "b03.rtdc",
        tmp_path / "p3.rtdc",
        tmp_path / "p1.rtdc",
        tmp_path / "ba3.rtdc",
        tmp_path / "p2.rtdc",
        tmp_path / "bb4.rtdc",
        tmp_path / "p4.rtdc",
    ]

    # this is what we would expect based on the sorting algorithm
    sorted_paths = [
        tmp_path / "p3.rtdc",
        tmp_path / "p1.rtdc",
        tmp_path / "ba3.rtdc",
        tmp_path / "b03.rtdc",
        tmp_path / "p2.rtdc",
        tmp_path / "p4.rtdc",
        tmp_path / "bb4.rtdc",
    ]

    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=input_paths, task_id="hanspeter")

    # test this
    assert uj.paths == sorted_paths

    # Let's do a second test with a different order
    input_paths_2 = [
        tmp_path / "p2.rtdc",
        tmp_path / "b03.rtdc",
        tmp_path / "p1.rtdc",
        tmp_path / "ba3.rtdc",
        tmp_path / "bb4.rtdc",
        tmp_path / "p4.rtdc",
        tmp_path / "p3.rtdc",
    ]

    sorted_paths_2 = [
        tmp_path / "p2.rtdc",
        tmp_path / "p1.rtdc",
        tmp_path / "p4.rtdc",
        tmp_path / "bb4.rtdc",
        tmp_path / "p3.rtdc",
        tmp_path / "ba3.rtdc",
        tmp_path / "b03.rtdc",
    ]

    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=input_paths_2, task_id="hanspeter2")

    # test this
    assert uj.paths == sorted_paths_2


@mock.patch.object(job.shutil, "disk_usage")
def test_state_compress_disk_wait(disk_usage_mock):
    """If not space left on disk, upload job goes to state "disk-wait"""""
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[rtdc_paths[1]])
    assert uj.state == "init"

    class DiskUsageReturn1:
        free = 0

    class DiskUsageReturn2:
        free = 1024**3

    disk_usage_mock.side_effect = [DiskUsageReturn1, DiskUsageReturn2]

    t0 = time.perf_counter()
    uj.task_compress_resources()
    t1 = time.perf_counter()
    assert t1-t0 > 0.15, "function waits 0.2s in-between checks"
    disk_usage_mock.assert_called()


@mock.patch.object(job.shutil, "disk_usage")
def test_state_compress_remove_other_upload_jobs(disk_usage_mock):
    """
    A job may delete rogue upload cache data from jobs that would
    upload after it.
    """
    api = common.get_api()
    cache_dir = pathlib.Path(tempfile.mkdtemp(prefix="upload_job_cache_test_"))
    # create some metadata
    test_list = []
    for _ in range(3):
        bare_dict = common.make_dataset_dict(hint="create-with-resource")
        dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
        uj = job.UploadJob(api=api,
                           dataset_id=dataset_dict["id"],
                           resource_paths=[rtdc_paths[1]],
                           cache_dir=cache_dir,
                           )
        test_list.append((dataset_dict["id"], uj, dataset_dict))

    # sort according to dataset ID
    test_list = sorted(test_list)
    uj1, uj2, uj3 = [td[1] for td in test_list]

    class DiskUsageReturn1:
        free = 0

    class DiskUsageReturn2:
        free = 1024**3

    disk_usage_mock.side_effect = [
        DiskUsageReturn2,  # uj1
        DiskUsageReturn2,  # uj3
        DiskUsageReturn1,  # uj2 fail
        DiskUsageReturn2]  # uj2 success

    # compress the first and the third dataset
    uj1.task_compress_resources()
    uj3.task_compress_resources()
    assert uj1.cache_dir.exists()
    assert not uj2.cache_dir.exists()
    assert uj3.cache_dir.exists()

    # Now compress uj2 and tell it that the disk is full. It should delete
    # the cache_dir of uj3, but not of uj1.
    uj2.task_compress_resources()
    assert uj1.cache_dir.exists()
    assert uj2.cache_dir.exists()
    assert not uj3.cache_dir.exists()


def test_state_compress_reuse():
    """Reuse previously compressed datasets"""""
    api = common.get_api()
    # create some metadata
    bare_dict = common.make_dataset_dict(hint="create-with-resource")
    # create dataset (to get the "id")
    dataset_dict = dataset_create(dataset_dict=bare_dict, api=api)
    uj = job.UploadJob(api=api, dataset_id=dataset_dict["id"],
                       resource_paths=[rtdc_paths[1]])
    assert uj.state == "init"

    with mock.patch.object(job, "compress",
                           side_effect=dclab.cli.compress) as mockobj:
        uj.task_compress_resources()
        mockobj.assert_called_once()

    with mock.patch.object(job, "compress",
                           side_effect=dclab.cli.compress) as mockobj:
        uj.task_compress_resources()
        with pytest.raises(AssertionError):
            mockobj.assert_called()
