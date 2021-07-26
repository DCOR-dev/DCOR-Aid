import pathlib
import shutil
import tempfile

import time
import uuid

from dcoraid.gui.main import DCORAid
from dcoraid.gui.upload.dlg_upload import UploadDialog

import pytest
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QMessageBox

import common


@pytest.fixture(autouse=True)
def run_around_tests():
    # Code that will run before your test, for example:
    pass
    # Run test
    yield
    # Make sure that all daemons are gone
    time.sleep(2)
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents,
                                         3000)
    time.sleep(2)


def test_anonymous(qtbot):
    """Start DCOR-Aid in anonymous mode"""
    settings = QtCore.QSettings()
    spath = pathlib.Path(settings.fileName())
    # temporarily move settings to temporary location
    stmp = pathlib.Path(tempfile.mkdtemp(prefix="settings_stash_")) / "set.ini"
    shutil.copy2(spath, stmp)
    spath.unlink()
    spath.write_text("\n".join([
        "[General]",
        "user%20scenario = anonymous",
        "[auth]",
        "api%20key =",
        "server = dcor.mpl.mpg.de",
        ]))
    try:
        DCORAid()
        QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 3000)
    except BaseException:
        # cleanup first (copy back original settings for other tests)
        spath.unlink()
        shutil.copy2(stmp, spath)
        raise
    else:
        spath.unlink()
        shutil.copy2(stmp, spath)


def test_upload_simple(qtbot, monkeypatch):
    """Upload a test dataset"""
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)

    dlg = UploadDialog(mw.panel_upload)
    mw.panel_upload._dlg_manual = dlg
    dlg.finished.connect(mw.panel_upload.on_upload_manual_ready)
    # Fill data for testing
    dlg._autofill_for_testing()
    # Avoid message boxes
    monkeypatch.setattr(QMessageBox, "question", lambda *args: QMessageBox.Yes)
    # Commence upload
    dlg.on_proceed()
    assert dlg.dataset_id is not None
    for ii in range(200):  # give it 20secs to upload
        state = mw.panel_upload.jobs[0].get_status()["state"]
        if state == "done":
            break
        time.sleep(.1)
    else:
        raise ValueError("Job did not complete, state: '{}'".format(state))
    mw.close()


def test_upload_task(qtbot, monkeypatch):
    task_id = str(uuid.uuid4())
    tpath = common.make_upload_task(task_id=task_id)
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)
    monkeypatch.setattr(QtWidgets.QFileDialog, "getOpenFileNames",
                        lambda *args: ([tpath], None))
    act = QtWidgets.QAction("some unimportant text")
    act.setData("single")
    mw.panel_upload.on_upload_task(action=act)
    uj = mw.panel_upload.jobs[-1]
    assert uj.task_id == task_id
    mw.close()


def test_upload_task_missing_circle(qtbot, monkeypatch):
    """When the organization is missing, DCOR-Aid should ask for it"""
    task_id = str(uuid.uuid4())
    dataset_dict = common.make_dataset_dict(hint="task_upload_no_org_")
    dataset_dict.pop("owner_org")
    tpath = common.make_upload_task(task_id=task_id,
                                    dataset_dict=dataset_dict)
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)
    monkeypatch.setattr(QtWidgets.QFileDialog, "getOpenFileNames",
                        lambda *args: ([tpath], None))
    # We actually only need this monkeypatch if there is more than
    # one circle for the present user.
    monkeypatch.setattr(QtWidgets.QInputDialog, "getItem",
                        # return the first item in the circle list
                        lambda *args: (args[3][0], True))
    act = QtWidgets.QAction("some unimportant text")
    act.setData("single")
    mw.panel_upload.on_upload_task(action=act)
    uj = mw.panel_upload.jobs[-1]
    assert uj.task_id == task_id
    mw.close()


def test_upload_private(qtbot, monkeypatch):
    """Upload a private test dataset"""
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)

    dlg = UploadDialog(mw.panel_upload)
    mw.panel_upload._dlg_manual = dlg
    dlg.finished.connect(mw.panel_upload.on_upload_manual_ready)
    # Fill data for testing
    dlg._autofill_for_testing()
    # set visibility to private
    dlg.comboBox_vis.setCurrentIndex(dlg.comboBox_vis.findData("private"))
    # Avoid message boxes
    monkeypatch.setattr(QMessageBox, "question", lambda *args: QMessageBox.Yes)
    # Commence upload
    dlg.on_proceed()
    dataset_id = dlg.dataset_id
    assert dataset_id is not None
    for ii in range(200):  # give it 20secs to upload
        state = mw.panel_upload.jobs[0].get_status()["state"]
        if state == "done":
            break
        time.sleep(.1)
    else:
        raise ValueError("Job did not complete, state: '{}'".format(state))
    mw.close()
    # make sure the dataset is private
    api = common.get_api()
    dataset_dict = api.get(api_call="package_show", id=dataset_id)
    assert dataset_dict["private"]
    assert isinstance(dataset_dict["private"], bool)


def test_zzz_final():
    # give remaining threads time to join?
    time.sleep(5)
