import pathlib
import shutil
import tempfile
from unittest import mock

import uuid

import dcoraid.gui.api
from dcoraid.gui.api import get_ckan_api
from dcoraid.gui.main import DCORAid
from dcoraid.gui.panel_uploads.dlg_upload import UploadDialog
from dcoraid.gui.panel_uploads import widget_upload

import pytest
from PyQt6 import QtCore, QtGui, QtTest, QtWidgets
from PyQt6.QtWidgets import QInputDialog, QMessageBox

from . import common


@pytest.fixture(scope="function", autouse=True)
def clear_ckan_api():
    dcoraid.gui.api._CKAN_API = None


@pytest.fixture
def mw(qtbot):
    # Always set server correctly, because there is a test that
    # makes sure DCOR-Aid starts with a wrong server.
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)
    settings = QtCore.QSettings()
    settings.setValue("auth/server", common.SERVER)
    QtWidgets.QApplication.processEvents(
        QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
    # Code that will run before your test
    mw = DCORAid()
    qtbot.addWidget(mw)
    QtWidgets.QApplication.setActiveWindow(mw)
    QtWidgets.QApplication.processEvents(
        QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
    # Run test
    yield mw
    # Make sure that all daemons are gone
    mw.close()
    # It is extremely weird, but this seems to be important to avoid segfaults!
    QtWidgets.QApplication.processEvents(
        QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)


@pytest.mark.filterwarnings("ignore::UserWarning",
                            match="No API token is set!")
def test_gui_anonymous(qtbot):
    """Start DCOR-Aid in anonymous mode"""
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)
    settings = QtCore.QSettings()
    spath = pathlib.Path(settings.fileName())
    # temporarily move settings to temporary location
    stmp = pathlib.Path(tempfile.mkdtemp(prefix="settings_stash_")) / "set.ini"
    shutil.copy2(spath, stmp)
    spath.unlink()
    spath.write_text("\n".join([
        "[General]",
        "user%20scenario = anonymous",
        "check for updates = 0",
        "skip database update on startup = 1",
        "[auth]",
        "api%20key =",
        "server = dcor.mpl.mpg.de",
        "[debug]",
        "without timers = 1",
        ]))
    try:
        mw = DCORAid()
        qtbot.addWidget(mw)
        QtWidgets.QApplication.setActiveWindow(mw)
        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
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
    QtWidgets.QApplication.processEvents(
        QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)


def test_gui_mydata_dataset_add_to_collection(mw, qtbot):
    """Upload a dataset and add it to a collection"""
    # upload via task
    task_id = str(uuid.uuid4())
    tpath = pathlib.Path(common.make_upload_task(task_id=task_id))
    # monkeypatch success message box
    with mock.patch.object(QMessageBox, "information",
                           return_value=None):
        mw.panel_upload.on_upload_task(action=tpath)
    # get the dataset ID
    uj = mw.panel_upload.jobs[-1]
    ds_id = uj.dataset_id
    # wait for the job
    common.wait_for_job_no_queue(uj)
    # go to the "My Data" tab and click update
    mw.tabWidget.setCurrentIndex(1)
    qtbot.mouseClick(mw.panel_my_data.pushButton_update_db,
                     QtCore.Qt.MouseButton.LeftButton)
    # select our dataset
    fw_datasets = mw.panel_my_data.user_filter_chain.fw_datasets
    entries = fw_datasets.get_entry_identifiers()
    index = entries.index(ds_id)
    # we cannot click on qtablewidgetitems (because they are not widgets)
    widget1 = fw_datasets.tableWidget.item(index, 0)
    assert widget1 is not None
    widget1.setSelected(True)
    selected = fw_datasets.get_entry_identifiers(
        selected=True)
    assert len(selected) == 1
    assert selected[0] == ds_id
    # mock the dialog
    api = get_ckan_api()
    grps = api.get("group_list_authz")
    grps = sorted(grps, key=lambda x: x["display_name"])
    defaults = common.get_test_defaults()
    for ii, item in enumerate(grps):
        if item["name"] == defaults["collection"]:
            break
    else:
        assert False, f"could not find {defaults['collection']}"
    with mock.patch.object(
            QInputDialog, "getItem",
            return_value=(f"{ii}: {item['display_name']}", True)):
        qtbot.mouseClick(fw_datasets.pushButton_custom,
                         QtCore.Qt.MouseButton.LeftButton)

    # Check whether dataset is in collection in database
    ds_dict_db = mw.database.get_dataset_dict(ds_id)
    assert "groups" in ds_dict_db
    assert len(ds_dict_db["groups"]) == 1
    assert ds_dict_db["groups"][0]["name"] == defaults["collection"]

    # Check whether dataset is in collection via API
    ds_dict = api.get("package_show", id=ds_id)
    assert "groups" in ds_dict
    assert len(ds_dict["groups"]) == 1
    assert ds_dict["groups"][0]["name"] == defaults["collection"]


def test_gui_start_with_bad_server(qtbot):
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)
    settings = QtCore.QSettings()
    settings.setValue("auth/server", "WRONG-dcor-dev.mpl.mpg.de")
    try:
        mw = DCORAid()
        qtbot.addWidget(mw)
        QtWidgets.QApplication.setActiveWindow(mw)
        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
        # just make sure that DCOR-Aid thinks it is offline
        assert not mw.panel_upload.isEnabled()
        assert not mw.panel_download.isEnabled()
        mw.close()
        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
    except BaseException:
        raise
    finally:
        # reset to testing defaults
        settings.setValue("auth/server", common.SERVER)


def test_gui_start_with_bad_api_key(qtbot):
    QtCore.QCoreApplication.setOrganizationName("DCOR")
    QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
    QtCore.QCoreApplication.setApplicationName("dcoraid")
    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)
    settings = QtCore.QSettings()
    good_key = settings.value("auth/api key")
    bad_key = good_key[:-2] + "00"
    settings.setValue("auth/api key", bad_key)
    try:
        mw = DCORAid()
        qtbot.addWidget(mw)
        QtWidgets.QApplication.setActiveWindow(mw)
        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
        # just make sure that DCOR-Aid thinks it is offline
        assert not mw.panel_upload.isEnabled()
        # downloads should still be possible
        assert mw.panel_download.isEnabled()
        mw.close()
        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
    except BaseException:
        raise
    finally:
        # reset to testing defaults
        settings.setValue("auth/api key", good_key)


def test_gui_upload_simple(mw, qtbot):
    """Upload a test dataset"""
    dlg = UploadDialog(mw.panel_upload)
    mw.panel_upload._dlg_manual = dlg
    dlg.finished.connect(mw.panel_upload.on_upload_manual_ready)
    # Fill data for testing
    dlg._autofill_for_testing()
    # Avoid message boxes
    with mock.patch.object(QMessageBox,
                           "question",
                           return_value=QMessageBox.StandardButton.Yes):
        # Commence upload
        dlg.on_proceed()
    assert dlg.dataset_id is not None

    common.wait_for_job(upload_queue=mw.panel_upload.jobs,
                        dataset_id=dlg.dataset_id,
                        set_job_done=False)

    mw.panel_upload.widget_jobs.on_update_job_status()
    QtWidgets.QApplication.processEvents(
        QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
    QtTest.QTest.qWait(500)
    assert mw.database.get_dataset_dict(dlg.dataset_id)


def test_gui_upload_task(mw, qtbot):
    task_id = str(uuid.uuid4())
    tpath = common.make_upload_task(task_id=task_id)
    with mock.patch.object(QtWidgets.QFileDialog, "getOpenFileNames",
                           return_value=([tpath], None)):
        with mock.patch.object(QMessageBox, "information",
                               return_value=None):
            act = QtGui.QAction("some unimportant text")
            act.setData("single")
            mw.panel_upload.on_upload_task(action=act)
    uj = mw.panel_upload.jobs[-1]
    assert uj.task_id == task_id


def test_gui_upload_task_bad_dataset_id_no(mw, qtbot):
    """When the dataset ID does not exist, DCOR-Aid should ask what to do"""
    task_id = str(uuid.uuid4())
    dataset_dict = common.make_dataset_dict(hint="task_upload_no_id_")
    tpath = common.make_upload_task(task_id=task_id,
                                    dataset_id="wrong_id",
                                    dataset_dict=dataset_dict)
    # monkeypatch file selection dialog
    with mock.patch.object(
            QtWidgets.QFileDialog, "getOpenFileNames",
            return_value=([tpath], None)), \
         mock.patch.object(QMessageBox, "question",
                           return_value=QMessageBox.StandardButton.No), \
         mock.patch.object(QMessageBox, "information", return_value=None):
        act = QtGui.QAction("some unimportant text")
        act.setData("single")
        mw.panel_upload.on_upload_task(action=act)
    if len(mw.panel_upload.jobs):
        # there might be upload jobs from previous tests here
        assert mw.panel_upload.jobs[-1].task_id != task_id


def test_gui_upload_task_bad_dataset_id_yes(mw, qtbot):
    """When the dataset ID does not exist, DCOR-Aid should ask what to do"""
    task_id = str(uuid.uuid4())
    dataset_dict = common.make_dataset_dict(hint="task_upload_user_id_")
    tpath = common.make_upload_task(task_id=task_id,
                                    dataset_id="wrong_id",
                                    dataset_dict=dataset_dict)
    with mock.patch.object(
            QtWidgets.QFileDialog, "getOpenFileNames",
            return_value=([tpath], None)), \
         mock.patch.object(QMessageBox, "question",
                           return_value=QMessageBox.StandardButton.Yes), \
         mock.patch.object(QMessageBox, "information", return_value=None):
        act = QtGui.QAction("some unimportant text")
        act.setData("single")
        mw.panel_upload.on_upload_task(action=act)
    uj = mw.panel_upload.jobs[-1]
    assert uj.task_id == task_id
    mw.panel_upload.jobs.daemon_compress.shutdown_flag.set()
    mw.panel_upload.jobs.daemon_compress.join()


def test_gui_upload_task_missing_circle_multiple(mw, qtbot):
    """DCOR-Aid should only ask *once* for the circle (not for every task)"""
    task_id1 = str(uuid.uuid4())
    dataset_dict1 = common.make_dataset_dict(hint="task_upload_ask_org_")
    dataset_dict1.pop("owner_org")
    tpath1 = common.make_upload_task(task_id=task_id1,
                                     dataset_dict=dataset_dict1)
    tpath1 = pathlib.Path(tpath1)

    task_id2 = str(uuid.uuid4())
    dataset_dict2 = common.make_dataset_dict(hint="task_upload_ask_org_")
    dataset_dict2.pop("owner_org")
    tpath2 = common.make_upload_task(task_id=task_id2,
                                     dataset_dict=dataset_dict2)
    tpath2 = pathlib.Path(tpath2)

    tdir = pathlib.Path(tempfile.mkdtemp(prefix="recursive_task_"))
    shutil.copytree(tpath1.parent, tdir / tpath1.parent.name)
    shutil.copytree(tpath2.parent, tdir / tpath2.parent.name)
    defaults = common.get_test_defaults()
    with mock.patch.object(
            QtWidgets.QFileDialog, "getExistingDirectory",
            return_value=str(tdir)), \
         mock.patch.object(QtWidgets.QInputDialog, "getItem",
                           return_value=(defaults["circle"], True)), \
         mock.patch.object(QMessageBox, "information", return_value=None):
        act = QtGui.QAction("some unimportant text")
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


def test_gui_upload_private(mw, qtbot):
    """Upload a private test dataset"""
    dlg = UploadDialog(mw.panel_upload)
    mw.panel_upload._dlg_manual = dlg
    dlg.finished.connect(mw.panel_upload.on_upload_manual_ready)
    # Fill data for testing
    dlg._autofill_for_testing()
    # set visibility to private
    dlg.comboBox_vis.setCurrentIndex(dlg.comboBox_vis.findData("private"))
    # Avoid message boxes
    with mock.patch.object(QMessageBox, "question",
                           return_value=QMessageBox.StandardButton.Yes), \
         mock.patch.object(QMessageBox, "information", return_value=None):
        # Commence upload
        dlg.on_proceed()
    dataset_id = dlg.dataset_id
    assert dataset_id is not None

    common.wait_for_job(upload_queue=mw.panel_upload.jobs,
                        dataset_id=dataset_id)

    # make sure the dataset is private
    api = common.get_api()
    dataset_dict = api.get(api_call="package_show", id=dataset_id)
    assert dataset_dict["private"]
    assert isinstance(dataset_dict["private"], bool)


def test_gui_upload_task_missing_circle(mw, qtbot):
    """When the organization is missing, DCOR-Aid should ask for it"""
    task_id = str(uuid.uuid4())
    dataset_dict = common.make_dataset_dict(hint="task_upload_missing_org_")
    dataset_dict.pop("owner_org")
    tpath = common.make_upload_task(task_id=task_id,
                                    dataset_dict=dataset_dict)
    QtWidgets.QApplication.processEvents(
        QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 200)
    defaults = common.get_test_defaults()
    with mock.patch.object(
            QtWidgets.QFileDialog, "getOpenFileNames",
            return_value=([tpath], None)), \
         mock.patch.object(QtWidgets.QInputDialog, "getItem",
                           return_value=(defaults["circle"], True)), \
         mock.patch.object(QMessageBox, "information", return_value=None):
        act = QtGui.QAction("some unimportant text")
        act.setData("single")
        mw.panel_upload.on_upload_task(action=act)
    uj = mw.panel_upload.jobs[-1]
    assert uj.task_id == task_id
