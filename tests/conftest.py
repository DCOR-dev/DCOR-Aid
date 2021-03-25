import shutil
import tempfile
import time

from PyQt5 import QtCore

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
    settings.sync()
    # set global temp directory
    tempfile.tempdir = TMPDIR


def pytest_unconfigure(config):
    """
    called before test process is exited.
    """
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
