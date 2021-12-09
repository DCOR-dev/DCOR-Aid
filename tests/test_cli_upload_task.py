import json
import pathlib

import pytest

from dcoraid import cli
from dcoraid.api import errors

from common import get_api, make_upload_task


def test_cli_basic():
    path_task = make_upload_task(resource_names=["cli_upload.rtdc"])
    api = get_api()
    uj = cli.upload_task(path_task, api.server, api.api_key)
    pkg_dict = api.get("package_show", id=uj.dataset_id)
    assert pkg_dict["resources"][0]["name"] == "cli_upload.rtdc"


@pytest.mark.parametrize("entry,emsg",
                         [["license_id", "Please choose a license_id"],
                          ["owner_org", "A circle must be provided"],
                          ["authors", r"authors: \['Missing value'\]"],
                          ])
def test_cli_fail_if_no_entries_missing(entry, emsg):
    path_task = pathlib.Path(
        make_upload_task(resource_names=["cli_upload.rtdc"]))
    data = json.loads(path_task.read_text())
    data["dataset_dict"].pop(entry)
    path_task.write_text(json.dumps(data))
    api = get_api()
    with pytest.raises(errors.APIConflictError, match=emsg):
        cli.upload_task(path_task, api.server, api.api_key)
