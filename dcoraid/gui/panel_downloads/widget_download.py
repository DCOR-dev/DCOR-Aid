from importlib import resources
import logging
import os.path as os_path
import threading
import traceback

from PyQt6 import uic, QtCore, QtWidgets
from PyQt6.QtCore import QStandardPaths

from ...common import is_dc_resource_dict
from ...download import DownloadQueue

from ..api import get_ckan_api
from ..tools import show_wait_cursor

from .widget_actions_download import TableCellActionsDownload


logger = logging.getLogger(__name__)


class DownloadWidget(QtWidgets.QWidget):
    download_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Manage running downloads
        """
        super(DownloadWidget, self).__init__(*args, **kwargs)
        ref_ui = resources.files(
            "dcoraid.gui.panel_downloads") / "widget_download.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        self.database = None

        self.settings = QtCore.QSettings()

        #: path to persistent shelf to be able to resume uploads on startup
        self.shelf_path = os_path.join(
            QStandardPaths.writableLocation(
                QStandardPaths.StandardLocation.AppLocalDataLocation),
            "persistent_download_jobs")

        #: DownloadQueue instance
        self._jobs = None
        self._jobs_init_lock = threading.Lock()

        self.setEnabled(False)
        self.init_timer = QtCore.QTimer(self)
        self.init_timer.setSingleShot(True)
        self.init_timer.setInterval(100)
        self.init_timer.timeout.connect(self.initialize)
        self.init_timer.start()

    def _jobs_init(self):
        with self._jobs_init_lock:
            if self._jobs is None:
                api = get_ckan_api()
                if api.is_available(with_correct_version=True):
                    self._jobs = DownloadQueue(
                        api=api,
                        path_persistent_job_list=self.shelf_path)
                    self.widget_jobs.set_job_list(self._jobs)

    @property
    def jobs(self):
        if self._jobs is None:
            self._jobs_init()
        if self._jobs is None:
            logger.error("Could not fetch download job list. Please make "
                         "sure a connection to DCOR is possible.")
        return self._jobs

    @show_wait_cursor
    @QtCore.pyqtSlot()
    def initialize(self):
        api = get_ckan_api()
        if api.is_available(with_correct_version=True):
            self.setEnabled(True)
            self._jobs_init()
            logger.info("Initialized download panel")
        else:
            # try again
            self.init_timer.setInterval(1000)
            self.init_timer.start()

    @QtCore.pyqtSlot(str, str, bool)
    def download_an_item(self, which, identifier, condensed=False):
        if which == "collection":
            self.download_collection(identifier, condensed)
        elif which == "dataset":
            self.download_dataset(identifier, condensed)
        elif which == "resource":
            self.download_resource(identifier, condensed)
        else:
            raise ValueError(f"Invalid download item specified: {which}")

    @QtCore.pyqtSlot(str, bool)
    def download_collection(self, collection_id, condensed=False):
        """Download all resources from all datasets in a collection"""
        # Get all datasets in that collection
        api = get_ckan_api()
        ds_list = api.get("package_search",
                          fq=f"+groups:{collection_id}",
                          include_private=True,
                          rows=1000)["results"]
        for ds_dict in ds_list:
            # use this opportunity to update our database
            self.database.update_dataset(ds_dict)
            # send this dataset on for downloading
            self.download_dataset(ds_dict["id"], condensed)

    @QtCore.pyqtSlot(str, bool)
    def download_dataset(self, dataset_id, condensed=False):
        """Download all resources in a dataset"""
        dl_path = self.get_download_path()
        ds_dict = self.database.get_dataset_dict(dataset_id)
        if self.jobs is not None:
            for res_dict in ds_dict.get("resources", []):
                cond_this = condensed
                if condensed and not is_dc_resource_dict(res_dict):
                    # Not a DC resource; cannot download condensed file!
                    cond_this = False
                self.jobs.new_job(res_dict["id"], dl_path, cond_this)

    @QtCore.pyqtSlot(str, bool)
    def download_resource(self, resource_id, condensed=False):
        """Download a resource"""
        dl_path = self.get_download_path()
        if self.jobs is not None:
            self.jobs.new_job(resource_id, dl_path, condensed)
        else:
            api = get_ckan_api()
            QtWidgets.QMessageBox.critical(
                self,
                f"Connection issue ({api.server})",
                f"We cannot connect to '{api.server}'. Please make "
                f"sure you are connected to the network.")

    def get_download_path(self):
        fallback = QStandardPaths.writableLocation(
                      QStandardPaths.StandardLocation.DownloadLocation)
        return self.settings.value("downloads/default path", fallback)

    def prepare_quit(self):
        """Should be called before the application quits"""
        self.init_timer.stop()
        if self.widget_jobs.timer is not None:
            self.widget_jobs.timer.stop()
        if self.jobs is not None:
            self.jobs.__del__()

    def set_database(self, database):
        self.database = database


class DownloadTableWidget(QtWidgets.QTableWidget):
    download_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        self._busy_updating_widgets_lock = threading.Lock()
        super(DownloadTableWidget, self).__init__(*args, **kwargs)
        self.jobs = []  # Will become DownloadQueue with self.set_job_list
        self._finished_downloads = []

        settings = QtCore.QSettings()
        if bool(int(settings.value("debug/without timers", "0"))):
            self.timer = None
        else:
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self.on_update_job_status)
            self.timer.start(500)

        # Set column count and horizontal header sizes
        self.setColumnCount(6)
        header = self.horizontalHeader()
        header.setSectionResizeMode(
            0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(
            1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(
            5, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)

    def set_job_list(self, jobs):
        """Set the current job list

        The job list can be a `list`, but it is actually
        a `DownloadQueue`.
        """
        # This is the actual initialization
        self.jobs = jobs

    @QtCore.pyqtSlot(str)
    def on_job_delete(self, job_id):
        with self._busy_updating_widgets_lock:
            self.jobs.remove_job(job_id)
        self.on_update_job_status()

    @QtCore.pyqtSlot(str)
    def on_job_retry(self, job_id):
        self.jobs.get_job(job_id).retry_download()

    @QtCore.pyqtSlot(str)
    def on_download_finished(self, job_id):
        """Triggers download_finished whenever a download is finished"""
        if job_id not in self._finished_downloads:
            dj = self.jobs.get_job(job_id)
            self.jobs.jobs_eternal.set_job_done(dj)
            self.download_finished.emit()
            logger.info(f"Download {job_id} finished")
            self._finished_downloads.append(job_id)

    @QtCore.pyqtSlot()
    def on_update_job_status(self):
        """Update UI with information from self.jobs (DownloadJobList)"""
        if not self.isVisible():
            # Don't update the UI if nobody is looking anyway.
            return

        if self._busy_updating_widgets_lock.locked():
            return

        with self._busy_updating_widgets_lock:
            self.update_job_status()

    @QtCore.pyqtSlot()
    def update_job_status(self):
        # make sure the length of the table is long enough
        self.setUpdatesEnabled(False)
        self.setRowCount(len(self.jobs))
        for row, job in enumerate(self.jobs):
            try:
                status = job.get_status()
                self.set_label_item(row, 0, job.job_id[:5])
                try:
                    title = get_download_title(job)
                except BaseException:
                    logger.error(traceback.format_exc())
                    # Probably a connection error
                    title = "-- error getting dataset title --"
                self.set_label_item(row, 1, title)
                self.set_label_item(row, 2, status["state"])
                self.set_label_item(row, 3, job.get_progress_string())
                self.set_label_item(row, 4, job.get_rate_string())
                self.set_actions_item(row, 5, job)
                if status["state"] == "done":
                    self.on_download_finished(job.job_id)
            except BaseException:
                job.set_state("error")
                job.traceback = traceback.format_exc()

            QtWidgets.QApplication.processEvents(
                QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)
        self.setUpdatesEnabled(True)

        header = self.horizontalHeader()
        header.setSectionResizeMode(
            5, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)

    def set_label_item(self, row, col, label):
        """Get/Create a Qlabel at the specified position

        User has to make sure that row and column count are set
        """
        label = f"{label}"
        item = self.item(row, col)
        if item is None:
            item = QtWidgets.QTableWidgetItem(label)
            item.setToolTip(label)
            item.setFlags(QtCore.Qt.ItemFlag.ItemIsEnabled)
            self.setItem(row, col, item)
        else:
            if item.text() != label:
                item.setText(label)
                item.setToolTip(label)

    def set_actions_item(self, row, col, job):
        """Set/Create a TableCellActions widget in the table

        Refreshes the widget and also connects signals.
        """
        wid = self.cellWidget(row, col)
        if wid is None:
            wid = TableCellActionsDownload(job, parent=self)
            wid.delete_job.connect(self.on_job_delete)
            self.setCellWidget(row, col, wid)
        else:
            wid.job = job
        wid.refresh_visibility(job)


def get_download_title(job):
    res_dict = job.get_resource_dict()
    ds_dict = job.get_dataset_dict()
    title = ds_dict.get("title")
    if not title:
        title = ds_dict.get("name")
    if job.condensed:
        title += " (condensed)"
    return f"{res_dict['name']} [{title}]"
