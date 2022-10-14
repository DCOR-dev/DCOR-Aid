import io
import json
import pathlib
import sys
from unittest import mock

import pytest

from dcoraid import cli

from .common import get_api, make_upload_task


def test_cli_basic(monkeypatch):
    def sys_exit(status):
        return status
    monkeypatch.setattr(sys, "exit", sys_exit)

    path_task = make_upload_task(resource_names=["cli_upload.rtdc"])
    api = get_api()
    uj = cli.upload_task(path_task, api.server, api.api_key, ret_job=True)
    pkg_dict = api.get("package_show", id=uj.dataset_id)
    assert pkg_dict["resources"][0]["name"] == "cli_upload.rtdc"
    assert pkg_dict["state"] == "active"


@pytest.mark.parametrize("entry,emsg",
                         [["license_id", "Please choose a license_id"],
                          ["owner_org", "must always be uploaded to a Circle"],
                          ["authors", r"authors: ['Missing value']"],
                          ])
def test_cli_fail_if_no_entries_missing(entry, emsg, monkeypatch):
    def sys_exit(status):
        return status
    monkeypatch.setattr(sys, "exit", sys_exit)

    path_task = pathlib.Path(
        make_upload_task(resource_names=["cli_upload.rtdc"]))
    data = json.loads(path_task.read_text())
    data["dataset_dict"].pop(entry)
    path_task.write_text(json.dumps(data))
    api = get_api()
    ret_val = cli.upload_task(path_task, api.server, api.api_key)
    assert ret_val != 0

    path_error = path_task.parent / (path_task.name + "_error.txt")
    assert path_error.exists()
    assert path_error.read_text().count(emsg)
    assert path_error.read_text().count("APIConflictError")


@mock.patch("dcoraid.upload.job.sha256sum",
            side_effect=["BAD SHA", "BAD SHA2", "BAD SHA3"])
@mock.patch('sys.stdout', new_callable=io.StringIO)
def test_cli_fail_upload(mock_stdout, mock_sha256sum, monkeypatch):
    def sys_exit(status):
        return status
    monkeypatch.setattr(sys, "exit", sys_exit)

    path_task = pathlib.Path(
        make_upload_task(resource_names=["cli_upload.rtdc"]))
    api = get_api()

    ret_val = cli.upload_task(path_task, api.server, api.api_key, ret_job=True)
    assert ret_val != 0
    path_error = path_task.parent / (path_task.name + "_error.txt")
    assert path_error.exists()
    assert path_error.read_text().count("See message above!")
    assert path_error.read_text().count("ValueError")

    stdout_printed = mock_stdout.getvalue()
    assert "SHA256 sum failed for resources" in stdout_printed
    assert "'cli_upload.rtdc' (BAD SHA vs." in stdout_printed
