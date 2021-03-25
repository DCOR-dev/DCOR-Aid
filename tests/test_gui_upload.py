import time

from dcoraid.gui.main import DCORAid

from PyQt5.QtWidgets import QMessageBox


def test_basic_upload(qtbot, monkeypatch):
    """Upload a test dataset"""
    # I get a segmentation fault when putting this outside the function?
    from dcoraid.gui.upload.dlg_upload import UploadDialog

    mw = DCORAid()
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
