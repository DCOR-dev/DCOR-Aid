import os.path as os_path
import pathlib
import shutil
import tempfile
import time

from PyQt5 import QtCore
from PyQt5.QtCore import QStandardPaths
import pytest

from dcoraid.api import APIConflictError

import common


TMPDIR = tempfile.mkdtemp(prefix=time.strftime(
    "dcoraid_test_%H.%M_"))

pytest_plugins = ["pytest-qt"]


def pytest_configure(config):
    """This is ran before all tests"""
    # disable update checking
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dcor.mpl.mpg.de")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.IniFormat)
    settings = QtCore.QSettings()
    settings.setIniCodec("utf-8")
    settings.value("user scenario", "dcor-dev")
    settings.setValue("auth/server", "dcor-dev.mpl.mpg.de")
    settings.setValue("auth/api key", common.get_api_key())
    settings.setValue("debug/without timers", 1)
    settings.sync()
    # remove persistent upload jobs
    shelf_path = os_path.join(
        QStandardPaths.writableLocation(
            QStandardPaths.AppLocalDataLocation),
        "persistent_upload_jobs")
    shutil.rmtree(shelf_path, ignore_errors=True)
    # remove persistent upload id dict
    path_id_dict = os_path.join(
        QStandardPaths.writableLocation(
            QStandardPaths.AppLocalDataLocation),
        "map_task_to_dataset_id.txt")
    path_id_dict = pathlib.Path(path_id_dict)
    if path_id_dict.exists():
        path_id_dict.unlink()
    # set global temp directory
    tempfile.tempdir = TMPDIR


def pytest_unconfigure(config):
    """
    called before test process is exited.
    """
    settings = QtCore.QSettings()
    settings.setIniCodec("utf-8")
    settings.remove("debug/without timers")
    settings.sync()
    # clear global temp directory
    shutil.rmtree(TMPDIR, ignore_errors=True)


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    api = common.get_api()
    api.get("group_show", id=common.COLLECTION)
    try:
        api.post("group_create", {"name": common.COLLECTION})
    except APIConflictError:
        pass
    try:
        api.post("organization_create", {"name": common.CIRCLE})
    except APIConflictError:
        pass
    user_dict = api.get("user_show", id=common.USER)
    user_dict["fullname"] = common.USER_NAME
    api.post("user_update", user_dict)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Writes report to failures file

    We need this to check whether the tests passed or failed
    du to the threading issue we have.
    https://github.com/DCOR-dev/DCOR-Aid/issues/14
    """
    # https://docs.pytest.org/en/stable/example/simple.html
    # #post-process-test-reports-failures
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # we only look at actual failing test calls, not setup/teardown
    if rep.when == "call" and rep.failed:
        mode = "a" if os_path.exists("pytest-failures.txt") else "w"
        with open("pytest-failures.txt", mode) as f:
            # let's also access a fixture for the fun of it
            if "tmpdir" in item.fixturenames:
                extra = " ({})".format(item.funcargs["tmpdir"])
            else:
                extra = ""
            f.write(rep.nodeid + extra + "\n")
