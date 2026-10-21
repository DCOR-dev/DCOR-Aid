import argparse
import io
import pathlib
import sys
from unittest import mock
import uuid
import warnings

import bagit
import pytest

import dcoraid
from dcoraid import cli

from .common import get_api, make_dataset_for_download


@pytest.fixture
def sys_exit_returns(monkeypatch):
    def sys_exit(status):
        return status
    monkeypatch.setattr(sys, "exit", sys_exit)


def make_circle_with_datasets():
    api = get_api()
    circle = f"circle-archive-cli-{uuid.uuid4()}"
    circle_dict = api.post("organization_create", data={"name": circle})
    make_dataset_for_download(circle=circle)
    make_dataset_for_download(circle=circle, add_text_file=True)
    return api, circle_dict


def assert_valid_bags(circle_path):
    assert (circle_path / "circle.jsonlines").exists()
    assert not (circle_path / "archive_circle_error.txt").exists()
    bags = sorted([d for d in circle_path.glob("*") if d.is_dir()])
    assert len(bags) == 2
    for bag_path in bags:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert bagit.Bag(str(bag_path)).is_valid()


@pytest.mark.parametrize("key", ["name", "id"])
def test_cli_archive_circle(key, tmp_path, sys_exit_returns):
    api, circle_dict = make_circle_with_datasets()
    ret_val = cli.archive_circle(circle=circle_dict[key],
                                 target_path=tmp_path,
                                 server=api.server,
                                 api_key=api.api_key)
    assert ret_val == 0
    # The circle name is always used for the subdirectory
    assert_valid_bags(tmp_path / circle_dict["name"])


def test_cli_archive_circle_api_key_from_environment(tmp_path,
                                                     monkeypatch,
                                                     sys_exit_returns):
    api, circle_dict = make_circle_with_datasets()
    monkeypatch.setenv("DCOR_API_KEY", api.api_key)
    ret_val = cli.archive_circle(circle=circle_dict["name"],
                                 target_path=tmp_path,
                                 server=api.server)
    assert ret_val == 0
    assert_valid_bags(tmp_path / circle_dict["name"])


def test_cli_archive_circle_resume(tmp_path, sys_exit_returns):
    api, circle_dict = make_circle_with_datasets()
    kwargs = {"circle": circle_dict["name"],
              "target_path": tmp_path,
              "server": api.server,
              "api_key": api.api_key}
    assert cli.archive_circle(**kwargs) == 0
    circle_path = tmp_path / circle_dict["name"]
    mtimes = {pp: pp.stat().st_mtime for pp in circle_path.rglob("*")}
    # Running again must not download anything
    assert cli.archive_circle(**kwargs) == 0
    assert mtimes == {pp: pp.stat().st_mtime for pp in circle_path.rglob("*")}


@mock.patch('sys.stdout', new_callable=io.StringIO)
def test_cli_archive_circle_does_not_exist(mock_stdout, tmp_path,
                                           sys_exit_returns):
    api = get_api()
    ret_val = cli.archive_circle(circle=f"does-not-exist-{uuid.uuid4()}",
                                 target_path=tmp_path,
                                 server=api.server,
                                 api_key=api.api_key)
    assert ret_val != 0
    assert "APINotFoundError" in mock_stdout.getvalue()
    assert not list(tmp_path.iterdir())


@mock.patch('sys.stdout', new_callable=io.StringIO)
def test_cli_archive_circle_api_key_missing(mock_stdout, tmp_path,
                                            monkeypatch, sys_exit_returns):
    monkeypatch.delenv("DCOR_API_KEY", raising=False)
    ret_val = cli.archive_circle(circle="my-circle",
                                 target_path=tmp_path,
                                 server="dcor.example.com")
    assert ret_val != 0
    assert "No API key specified" in mock_stdout.getvalue()


def test_parser():
    parser = cli.archive_circle_parser()
    args = parser.parse_args(["my-circle", "/path/to/archive",
                              "dcor.example.com"])
    assert args.circle == "my-circle"
    assert pathlib.Path(args.target_path) == pathlib.Path("/path/to/archive")
    assert args.server == "dcor.example.com"
    assert args.api_key is None


@mock.patch('sys.stderr', new_callable=io.StringIO)
def test_parser_server_required(mock_stderr):
    parser = cli.archive_circle_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["my-circle", "/path/to/archive"])
    assert "SERVER" in mock_stderr.getvalue()


@mock.patch('sys.stdout', new_callable=io.StringIO)
def test_version(mock_stdout, monkeypatch, sys_exit_returns):
    monkeypatch.setattr(sys, "argv", ["dcoraid-archive-circle", "--version"])
    parser = cli.archive_circle_parser()
    parser.parse_args()
    stdout_printed = mock_stdout.getvalue()
    assert stdout_printed.count("dcoraid-archive-circle")
    assert stdout_printed.count(dcoraid.__version__)


@mock.patch('sys.stdout', new_callable=io.StringIO)
def test_version_exit_status(mock_stdout, monkeypatch):
    with pytest.raises(SystemExit, match="0"):
        monkeypatch.setattr(argparse._sys, "argv", ["dcoraid-archive-circle",
                                                    "--version"])
        cli.archive_circle()
