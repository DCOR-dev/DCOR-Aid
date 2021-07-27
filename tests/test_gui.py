import pathlib
import shutil
import tempfile
from unittest import mock

import uuid

from dcoraid.gui.main import DCORAid
from dcoraid.gui.upload.dlg_upload import UploadDialog
from dcoraid.gui.upload import widget_upload

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
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents,
                                         3000)
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents,
                                         3000)


@pytest.mark.filterwarnings("ignore::UserWarning", match="No API key is set!")
def test_anonymous(qtbot):
    """Start DCOR-Aid in anonymous mode"""
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dcor.mpl.mpg.de")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.IniFormat)
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
        mw = DCORAid()
        QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 3000)
        # sanity check
        assert mw.settings.value("user scenario") == "anonymous"
        assert mw.settings.value("auth/server") == "dcor.mpl.mpg.de"
    except BaseException:
        # cleanup first (copy back original settings for other tests)
        spath.unlink()
        shutil.copy2(stmp, spath)
        raise
    else:
        spath.unlink()
        shutil.copy2(stmp, spath)
    mw.close()


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

    common.wait_for_job(upload_queue=mw.panel_upload.jobs,
                        dataset_id=mw.panel_upload.jobs[0].dataset_id)
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


def test_upload_task_missing_circle_multiple(qtbot, monkeypatch):
    """DCOR-Aid should only ask *once* for the circle (not for every task)"""
    task_id1 = str(uuid.uuid4())
    dataset_dict1 = common.make_dataset_dict(hint="task_upload_no_org_")
    dataset_dict1.pop("owner_org")
    tpath1 = common.make_upload_task(task_id=task_id1,
                                     dataset_dict=dataset_dict1)
    tpath1 = pathlib.Path(tpath1)

    task_id2 = str(uuid.uuid4())
    dataset_dict2 = common.make_dataset_dict(hint="task_upload_no_org_")
    dataset_dict2.pop("owner_org")
    tpath2 = common.make_upload_task(task_id=task_id2,
                                     dataset_dict=dataset_dict2)
    tpath2 = pathlib.Path(tpath2)

    tdir = pathlib.Path(tempfile.mkdtemp(prefix="recursive_task_"))
    shutil.copytree(tpath1.parent, tdir / tpath1.parent.name)
    shutil.copytree(tpath2.parent, tdir / tpath2.parent.name)

    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)
    monkeypatch.setattr(QtWidgets.QFileDialog, "getExistingDirectory",
                        lambda *args: str(tdir))
    # We actually only need this monkeypatch if there is more than
    # one circle for the present user.
    monkeypatch.setattr(QtWidgets.QInputDialog, "getItem",
                        # return the first item in the circle list
                        lambda *args: (args[3][0], True))
    act = QtWidgets.QAction("some unimportant text")
    act.setData("bulk")
    request_circle = widget_upload.circle_mgr.request_circle
    with mock.patch.object(widget_upload.circle_mgr,
                           "request_circle",
                           wraps=request_circle) as rw:
        mw.panel_upload.on_upload_task(action=act)
        uj1 = mw.panel_upload.jobs[-2]
        uj2 = mw.panel_upload.jobs[-1]
        assert {uj1.task_id, uj2.task_id} == {task_id1, task_id2}
        assert rw.call_count == 1
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

    common.wait_for_job(upload_queue=mw.panel_upload.jobs,
                        dataset_id=mw.panel_upload.jobs[0].dataset_id)
    mw.close()
    # make sure the dataset is private
    api = common.get_api()
    dataset_dict = api.get(api_call="package_show", id=dataset_id)
    assert dataset_dict["private"]
    assert isinstance(dataset_dict["private"], bool)
