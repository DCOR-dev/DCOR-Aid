import time

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


def test_upload_simple(qtbot, monkeypatch):
    """Upload a test dataset"""
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)

    dlg = UploadDialog(mw.panel_upload)
    dlg.finished.connect(mw.panel_upload.on_run_upload)
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


def test_upload_private(qtbot, monkeypatch):
    """Upload a private test dataset"""
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)

    dlg = UploadDialog(mw.panel_upload)
    dlg.finished.connect(mw.panel_upload.on_run_upload)
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
