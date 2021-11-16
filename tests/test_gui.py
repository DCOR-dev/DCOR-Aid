import pathlib
import shutil
import tempfile
from unittest import mock

import uuid

from dcoraid.gui.api import get_ckan_api
from dcoraid.gui.main import DCORAid
from dcoraid.gui.upload.dlg_upload import UploadDialog
from dcoraid.gui.upload import widget_upload

import pytest
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QInputDialog, QMessageBox

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


def test_mydata_dataset_add_to_collection(qtbot, monkeypatch):
    """Upload a dataset and add it to a collection"""
    # upload via task
    task_id = str(uuid.uuid4())
    tpath = pathlib.Path(common.make_upload_task(task_id=task_id))
    mw = DCORAid()
    # monkeypatch success message box
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *args: None)
    mw.panel_upload.on_upload_task(action=tpath)
    # get the dataset ID
    uj = mw.panel_upload.jobs[-1]
    ds_id = uj.dataset_id
    # wait for the job
    common.wait_for_job_no_queue(uj)
    # go to the "My Data" tab and click update
    mw.tabWidget.setCurrentIndex(1)
    qtbot.mouseClick(mw.pushButton_user_refresh,
                     QtCore.Qt.MouseButton.LeftButton)
    # select our dataset
    entries = mw.user_filter_chain.fw_datasets.get_entry_identifiers()
    index = entries.index(ds_id)
    # we cannot click on qtablewidgetitems (because they are not widgets)
    widget1 = mw.user_filter_chain.fw_datasets.tableWidget.item(index, 0)
    assert widget1 is not None
    widget1.setSelected(True)
    selected = mw.user_filter_chain.fw_datasets.get_entry_identifiers(
        selected=True)
    assert len(selected) == 1
    assert selected[0] == ds_id
    # mock the dialog
    api = get_ckan_api()
    grps = api.get("group_list_authz")
    grps = sorted(grps, key=lambda x: x["display_name"])
    for ii, item in enumerate(grps):
        if item["name"] == "dcoraid-collection":
            break
    else:
        assert False, "could not find dcoraid-collection"
    monkeypatch.setattr(QInputDialog,
                        "getItem",
                        lambda *args: (f"{ii}: {item['display_name']}", True))
    qtbot.mouseClick(mw.user_filter_chain.fw_datasets.toolButton_custom,
                     QtCore.Qt.MouseButton.LeftButton)
    # Now check whether that worked
    ds_dict = api.get("package_show", id=ds_id)
    assert "groups" in ds_dict
    assert len(ds_dict["groups"]) == 1
    assert ds_dict["groups"][0]["name"] == "dcoraid-collection"


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
    # monkeypatch success message box
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *args: None)
    act = QtWidgets.QAction("some unimportant text")
    act.setData("single")
    mw.panel_upload.on_upload_task(action=act)
    uj = mw.panel_upload.jobs[-1]
    assert uj.task_id == task_id
    mw.close()


def test_upload_task_bad_dataset_id_no(qtbot, monkeypatch):
    """When the dataset ID does not exist, DCOR-Aid should ask what to do"""
    task_id = str(uuid.uuid4())
    dataset_dict = common.make_dataset_dict(hint="task_upload_no_org_")
    tpath = common.make_upload_task(task_id=task_id,
                                    dataset_id="wrong_id",
                                    dataset_dict=dataset_dict)
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)
    # monkeypatch file selection dialog
    monkeypatch.setattr(QtWidgets.QFileDialog, "getOpenFileNames",
                        lambda *args: ([tpath], None))
    # monkeypatch yes-no dialog
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *args: QMessageBox.No)
    # monkeypatch success message box
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *args: None)
    act = QtWidgets.QAction("some unimportant text")
    act.setData("single")
    mw.panel_upload.on_upload_task(action=act)
    if len(mw.panel_upload.jobs):
        # there might be upload jobs from previous tests here
        assert mw.panel_upload.jobs[-1].task_id != task_id
    mw.close()


def test_upload_task_bad_dataset_id_yes(qtbot, monkeypatch):
    """When the dataset ID does not exist, DCOR-Aid should ask what to do"""
    task_id = str(uuid.uuid4())
    dataset_dict = common.make_dataset_dict(hint="task_upload_no_org_")
    tpath = common.make_upload_task(task_id=task_id,
                                    dataset_id="wrong_id",
                                    dataset_dict=dataset_dict)
    mw = DCORAid()
    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 300)
    # monkeypatch file selection dialog
    monkeypatch.setattr(QtWidgets.QFileDialog, "getOpenFileNames",
                        lambda *args: ([tpath], None))
    # monkeypatch yes-no dialog
    monkeypatch.setattr(QMessageBox, "question",
                        lambda *args: QMessageBox.Yes)
    # monkeypatch success message box
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *args: None)
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
    # monkeypatch success message box
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *args: None)
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
    # monkeypatch success message box
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *args: None)
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
    # monkeypatch success message box
    monkeypatch.setattr(QMessageBox, "information",
                        lambda *args: None)
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
