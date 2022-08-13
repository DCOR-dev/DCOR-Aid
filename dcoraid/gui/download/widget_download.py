import logging
import traceback
from functools import partial
import os
import os.path as os_path
import pkg_resources
import platform
import subprocess
import webbrowser

from PyQt5 import uic, QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QStandardPaths

from ...download import DownloadQueue

from ..api import get_ckan_api
from ..tools import show_wait_cursor


class DownloadWidget(QtWidgets.QWidget):
    download_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Manage running downloads
        """
        super(DownloadWidget, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.download", "widget_download.ui")
        uic.loadUi(path_ui, self)

        self.settings = QtCore.QSettings()

        #: path to persistent shelf to be able to resume uploads on startup
        self.shelf_path = os_path.join(
            QStandardPaths.writableLocation(
                QStandardPaths.AppLocalDataLocation),
            "persistent_download_jobs")

        #: DownloadQueue instance
        self.jobs = None

        self.setEnabled(False)
        self.init_timer = QtCore.QTimer(self)
        self.init_timer.setSingleShot(True)
        self.init_timer.setInterval(100)
        self.init_timer.timeout.connect(self.initialize)
        self.init_timer.start()

    @show_wait_cursor
    @QtCore.pyqtSlot()
    def initialize(self):
        api = get_ckan_api()
        if api.is_available(with_correct_version=True):
            self.setEnabled(True)
            self.jobs = DownloadQueue(api=api,
                                      path_persistent_job_list=self.shelf_path)
            self.widget_jobs.set_job_list(self.jobs)
        else:
            # try again
            self.init_timer.setInterval(3000)
            self.init_timer.start()

    @QtCore.pyqtSlot(str, bool)
    def download_resource(self, resource_id, condensed=False):
        fallback = QStandardPaths.writableLocation(
                      QStandardPaths.DownloadLocation)
        dl_path = self.settings.value("downloads/default path", fallback)
        self.widget_jobs.jobs.new_job(resource_id, dl_path, condensed)

    def stop_timers(self):
        """Should be called before the application quits"""
        self.init_timer.stop()
        if self.widget_jobs.timer is not None:
            self.widget_jobs.timer.stop()


class DownloadTableWidget(QtWidgets.QTableWidget):
    download_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(DownloadTableWidget, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.jobs = []  # Will become DownloadQueue with self.set_job_list

        settings = QtCore.QSettings()
        if bool(int(settings.value("debug/without timers", "0"))):
            self.timer = None
        else:
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self.update_job_status)
            self.timer.start(1000)
        self._finished_downloads = []

    def set_job_list(self, jobs):
        """Set the current job list

        The job list can be a `list`, but it is actually
        a `DownloadQueue`.
        """
        # This is the actual initialization
        self.jobs = jobs

    @QtCore.pyqtSlot(str)
    def on_job_abort(self, job_id):
        self.jobs.abort_job(job_id)

    @QtCore.pyqtSlot(str)
    def on_job_delete(self, job_id):
        self.jobs.remove_job(job_id)
        self.clearContents()
        self.update_job_status()

    @QtCore.pyqtSlot(str)
    def on_download_finished(self, job_id):
        """Triggers download_finished whenever a download is finished"""
        if job_id not in self._finished_downloads:
            self._finished_downloads.append(job_id)
            dj = self.jobs.get_job(job_id)
            self.jobs.jobs_eternal.set_job_done(dj)
            self.download_finished.emit()

    @QtCore.pyqtSlot()
    def update_job_status(self):
        """Update UI with information from self.jobs (DownloadJobList)"""
        # disable updates
        self.setUpdatesEnabled(False)
        # make sure the length of the table is long enough
        self.setRowCount(len(self.jobs))
        self.setColumnCount(6)

        for row, job in enumerate(self.jobs):
            status = job.get_status()
            self.set_label_item(row, 0, job.job_id[:5])
            try:
                title = get_download_title(job)
            except BaseException:
                self.logger.error(traceback.format_exc())
                # Probably a connection error
                title = "-- error getting dataset title --"
            self.set_label_item(row, 1, title)
            self.set_label_item(row, 2, status["state"])
            self.set_label_item(row, 3, job.get_progress_string())
            self.set_label_item(row, 4, job.get_rate_string())
            if status["state"] == "done":
                self.on_download_finished(job.job_id)
            self.set_actions_item(row, 5, job)

        # spacing (did not work in __init__)
        header = self.horizontalHeader()
        header.setSectionResizeMode(
            0, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.Stretch)
        # enable updates again
        self.setUpdatesEnabled(True)

    def set_label_item(self, row, col, label):
        """Get/Create a Qlabel at the specified position

        User has to make sure that row and column count are set
        """
        label = "{}".format(label)
        item = self.item(row, col)
        if item is None:
            item = QtWidgets.QTableWidgetItem(label)
            item.setFlags(QtCore.Qt.ItemIsEnabled)
            self.setItem(row, col, item)
        else:
            if item.text() != label:
                item.setText(label)

    def set_actions_item(self, row, col, job):
        """Set/Create a TableCellActions widget in the table

        Refreshes the widget and also connects signals.
        """
        widact = self.cellWidget(row, col)
        if widact is None:
            widact = QtWidgets.QWidget(self)
            horz_layout = QtWidgets.QHBoxLayout(widact)
            horz_layout.setContentsMargins(2, 0, 2, 0)

            spacer = QtWidgets.QSpacerItem(0, 0,
                                           QtWidgets.QSizePolicy.Expanding,
                                           QtWidgets.QSizePolicy.Minimum)
            horz_layout.addItem(spacer)

            res_dict = job.get_resource_dict()
            ds_dict = job.get_dataset_dict()
            dl_path = job.path
            if not dl_path.is_dir():
                dl_path = dl_path.parent
            actions = [
                {"icon": "eye",
                 "tooltip": f"view dataset {ds_dict['name']} online",
                 "function": partial(
                     webbrowser.open,
                     f"{job.api.server}/dataset/{ds_dict['id']}")
                 },
                {"icon": "folder",
                 "tooltip": "open local download directory",
                 "function": partial(open_file, str(dl_path))
                 },
                {"icon": "trash",
                 "tooltip": f"abort download {res_dict['name']}",
                 "function": partial(self.on_job_delete, job.job_id)
                 },
            ]
            for action in actions:
                tbact = QtWidgets.QToolButton(widact)
                icon = QtGui.QIcon.fromTheme(action["icon"])
                tbact.setIcon(icon)
                tbact.setToolTip(action["tooltip"])
                tbact.clicked.connect(action["function"])
                horz_layout.addWidget(tbact)
            self.setCellWidget(row, col, widact)


def get_download_title(job):
    res_dict = job.get_resource_dict()
    ds_dict = job.get_dataset_dict()
    title = ds_dict.get("title")
    if not title:
        title = ds_dict.get("name")
    if job.condensed:
        title += " (condensed)"
    return f"{res_dict['name']} [{title}]"


def open_file(path):
    if platform.system() == "Windows":
        os.startfile(path)
    elif platform.system() == "Darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])
