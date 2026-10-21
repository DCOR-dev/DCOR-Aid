from unittest import mock
import warnings
import uuid

import bagit
import pytest
import requests.exceptions

from dcoraid.bagit import archive
from dcoraid.download import DownloadJob

from . import common


def test_bag_circle(tmp_path):
    api = common.get_api()
    # create a circle for this purpose
    circle = f"circle-bag-{uuid.uuid4()}"
    api.post("organization_create", data={"name": circle})
    common.make_dataset_for_download(circle=circle)
    common.make_dataset_for_download(circle=circle, add_text_file=True)
    archive.bag_circle(api=api,
                       circle_name=circle,
                       target_path=tmp_path,
                       )

    assert (tmp_path / "circle.jsonlines").exists()

    bags = sorted([d for d in tmp_path.glob("*") if d.is_dir()])
    assert (bags[1] / "data" / "text_file.txt").exists()

    for bag_path in bags:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            bag = bagit.Bag(bag_path)
            assert bag.is_valid()


def test_bag_dataset(tmp_path):
    api = common.get_api()
    ds_dict = common.make_dataset_for_download()

    archive.bag_dataset(
        api=api,
        ds_dict=ds_dict,
        bag_path=tmp_path,
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        bag = bagit.Bag(tmp_path)
        assert bag.is_valid()


def test_bag_dataset_multiple_resources(tmp_path):
    api = common.get_api()
    ds_dict = common.make_dataset_for_download(add_text_file=True)

    archive.bag_dataset(
        api=api,
        ds_dict=ds_dict,
        bag_path=tmp_path,
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        bag = bagit.Bag(tmp_path)
        assert bag.is_valid()


def test_download_resource(tmp_path):
    api = common.get_api()
    ds_dict = common.make_dataset_for_download()
    res_dict = ds_dict["resources"][0]

    archive.download_resource(api=api,
                              bag_path=tmp_path,
                              res_dict=res_dict,
                              condensed=False,
                              )

    assert (tmp_path / "data" / res_dict["name"]).exists()
    assert (tmp_path / "data" / res_dict["name"]).stat().st_size == 899298


def test_download_resource_retry_connection_error(tmp_path):
    """A transient error during download triggers a retry (offline test)"""
    calls = []

    def task_download_resource(self, abort_event=None):
        calls.append(1)
        if len(calls) == 1:
            raise requests.exceptions.ConnectionError("Connection reset")
        self.set_state("downloaded")

    def task_verify_resource(self):
        self.set_state("done")

    res_dict = {"id": "1234", "name": "data.rtdc"}
    with mock.patch.object(DownloadJob, "task_download_resource",
                           task_download_resource), \
            mock.patch.object(DownloadJob, "task_verify_resource",
                              task_verify_resource):
        archive.download_resource(api=mock.MagicMock(),
                                  bag_path=tmp_path,
                                  res_dict=res_dict,
                                  condensed=False,
                                  retries_num=2,
                                  retries_wait=0.01,
                                  )
    assert len(calls) == 2


def test_download_resource_retry_exhausted(tmp_path):
    """Abort with an error after all retries failed (offline test)"""
    calls = []

    def task_download_resource(self, abort_event=None):
        calls.append(1)
        raise requests.exceptions.ConnectionError("Connection reset")

    res_dict = {"id": "1234", "name": "data.rtdc"}
    with mock.patch.object(DownloadJob, "task_download_resource",
                           task_download_resource):
        with pytest.raises(archive.ArchiveError,
                           match="Failed to download resource data.rtdc"):
            archive.download_resource(api=mock.MagicMock(),
                                      bag_path=tmp_path,
                                      res_dict=res_dict,
                                      condensed=False,
                                      retries_num=3,
                                      retries_wait=0.01,
                                      )
    # initial try plus three retries
    assert len(calls) == 4


def test_download_resource_retry_verification_error(tmp_path):
    """Corrupt downloads are removed and retried (offline test)"""
    temp_paths = []

    def task_download_resource(self, abort_event=None):
        self.path_temp = tmp_path / f"data.rtdc~{len(temp_paths)}"
        self.path_temp.write_text("corrupt")
        temp_paths.append(self.path_temp)
        self.set_state("downloaded")

    def task_verify_resource(self):
        self.traceback = "SHA256 sum check failed"
        self.set_state("error")

    res_dict = {"id": "1234", "name": "data.rtdc"}
    with mock.patch.object(DownloadJob, "task_download_resource",
                           task_download_resource), \
            mock.patch.object(DownloadJob, "task_verify_resource",
                              task_verify_resource):
        with pytest.raises(archive.ArchiveError,
                           match="SHA256 sum check failed"):
            archive.download_resource(api=mock.MagicMock(),
                                      bag_path=tmp_path,
                                      res_dict=res_dict,
                                      condensed=False,
                                      retries_num=1,
                                      retries_wait=0.01,
                                      )
    assert len(temp_paths) == 2
    for pp in temp_paths:
        assert not pp.exists()
