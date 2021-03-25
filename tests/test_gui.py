import time

from dcoraid.gui.main import DCORAid
from dcoraid.gui.upload.dlg_upload import UploadDialog

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QMessageBox


def test_upload_simple(qtbot, monkeypatch):
    """Upload a test dataset"""
    # I get "Aborted (core dumped)" when putting this outside the function?

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
