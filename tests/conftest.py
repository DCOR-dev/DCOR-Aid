import atexit
import os.path as os_path
import pathlib
import shutil
import tempfile
import time
import traceback

from PyQt6 import QtCore
from PyQt6.QtCore import QStandardPaths

from dcoraid.api import APIConflictError

from . import common


TMPDIR = tempfile.mkdtemp(prefix=time.strftime(
    "dcoraid_test_%H.%M_"))

# settings before testing
USER_SETTINGS = {}

pytest_plugins = ["pytest-qt"]


def cleanup_dcoraid_tasks():
    # disable update checking
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    # remove persistent upload jobs
    shelf_path = os_path.join(
        QStandardPaths.writableLocation(
            QStandardPaths.StandardLocation.AppLocalDataLocation),
        "persistent_upload_jobs")
    shutil.rmtree(shelf_path, ignore_errors=True)
    # remove persistent upload id dict
    path_id_dict = os_path.join(
        QStandardPaths.writableLocation(
            QStandardPaths.StandardLocation.AppLocalDataLocation),
        "map_task_to_dataset_id.txt")
    path_id_dict = pathlib.Path(path_id_dict)
    if path_id_dict.exists():
        path_id_dict.unlink()


def pytest_configure(config):
    """This is run before all tests"""
    # disable update checking
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)
    settings = QtCore.QSettings()
    change_settings = {
        "check for updates": "0",
        "user scenario": "dcor-dev",
        "auth/server": common.SERVER,
        "auth/api key": common.get_api_key(),
    }
    for key, value in change_settings.items():
        old_value = settings.value(key, None)
        if old_value is not None:
            USER_SETTINGS[key] = old_value

        settings.setValue(key, value)
    # removing timers breaks the user workflow, so it is not in change_settings
    settings.setValue("debug/without timers", "1")
    settings.setValue("skip database update on startup", "1")
    settings.sync()
    # cleanup
    cleanup_dcoraid_tasks()
    # set global temp directory
    tempfile.tempdir = TMPDIR
    atexit.register(shutil.rmtree, TMPDIR, ignore_errors=True)


def pytest_unconfigure(config):
    """
    called before test process is exited.
    """
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)
    settings = QtCore.QSettings()
    for key in USER_SETTINGS:
        settings.setValue(key, USER_SETTINGS[key])
    # always remove this setting, because it breaks the user workflow
    settings.remove("debug/without timers")
    settings.remove("skip database update on startup")
    settings.sync()
    # cleanup
    cleanup_dcoraid_tasks()


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    try:
        api = common.get_api()
    except BaseException:
        print("API TESTING DISABLED")
        print(traceback.format_exc())
    else:
        defaults = common.get_test_defaults()
        try:
            api.post("group_create", {"name": defaults["collection"]})
        except APIConflictError:
            pass
        try:
            api.post("organization_create", {"name": defaults["circle"]})
        except APIConflictError:
            pass
        user_dict = api.get("user_show")
        user_dict["fullname"] = defaults["user_name"]
        api.post("user_update", user_dict)
