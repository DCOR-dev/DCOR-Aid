# ATTENTION:
# For some reason pytest segfaults if not all GUI tests are in one file.
# This has something to do with Threading.
import time

from dcoraid.gui.main import DCORAid
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QMessageBox


def test_simple(qtbot):
    """Open the main window and close it again"""
    main_window = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)
    main_window.close()


def test_upload_simple(qtbot, monkeypatch):
    """Upload a test dataset"""
    # I get "Aborted (core dumped)" when putting this outside the function?
    from dcoraid.gui.upload.dlg_upload import UploadDialog

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
