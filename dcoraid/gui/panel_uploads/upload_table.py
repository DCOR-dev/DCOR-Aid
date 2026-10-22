from __future__ import annotations

from functools import lru_cache
import logging
import threading
import traceback as tb

from PyQt6 import QtCore, QtWidgets

from ..api import get_ckan_api

from .widget_actions_upload import TableCellActionsUpload


class UploadTableWidget(QtWidgets.QTableWidget):
    upload_finished = QtCore.pyqtSignal(dict)
    job_selected = QtCore.pyqtSignal(object)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__).getChild("UploadTableWidget")
        self.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.setSelectionBehavior(
            QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)

        self._jobs = None
        self._finished_uploads = []
        self._on_update_job_status_lock = threading.Lock()

        settings = QtCore.QSettings()
        if bool(int(settings.value("debug/without timers", "0"))):
            self.timer = None
        else:
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self.on_update_job_status)
            self.timer.start(500)

        # signals
        self.itemSelectionChanged.connect(self.on_selection)
        self.itemClicked.connect(self.on_selection)

        # Set columns and spacing for upload table
        self.setColumnCount(6)
        header = self.horizontalHeader()
        header.setSectionResizeMode(
            0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(
            1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(
            5, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)

    @property
    def jobs(self):
        """Upload job list

        Returns
        -------
        jobs: .UploadQueue
            Object for managing upload jobs. Returns None
            if `set_job_list` has not been called before.
        """
        if self._jobs is None:
            self.logger.warning("Job list not initialized!")
        return self._jobs

    def get_dataset_title(self, job):
        try:
            title = get_dataset_title(job.dataset_id)
        except BaseException:
            self.logger.error(tb.format_exc())
            # Probably a connection error
            title = "-- error getting dataset title --"
        return title

    def set_job_list(self, jobs):
        """Set the current job list

        The job list can be a `list`, but it is actually
        an `UploadQueue`.
        """
        # This is the actual initialization of `self.jobs`
        self._jobs = jobs

    @QtCore.pyqtSlot(str)
    def on_job_abort(self, dataset_id):
        self.jobs.abort_job(dataset_id)
        self.on_update_job_status()

    @QtCore.pyqtSlot(str)
    def on_job_delete(self, dataset_id):
        self.jobs.remove_job(dataset_id)
        self.on_update_job_status()

    @QtCore.pyqtSlot()
    def on_selection(self):
        row = self.currentRow()
        if self.jobs:
            job = self.jobs[row]
            self.job_selected.emit(job)

    @QtCore.pyqtSlot(str)
    def on_upload_finished(self, dataset_id):
        """Triggers upload_finished whenever an upload is finished"""
        if dataset_id not in self._finished_uploads:
            self._finished_uploads.append(dataset_id)
            self.jobs.jobs_eternal.set_job_done(dataset_id)
            api = get_ckan_api()
            try:
                ds_dict = api.get("package_show",
                                  id=dataset_id,
                                  timeout=3)
                self.upload_finished.emit(ds_dict)
            except BaseException:
                self.logger.error(tb.format_exc())

    @QtCore.pyqtSlot()
    def on_update_job_status(self):
        """Update UI with information from self.jobs (UploadJobList)"""
        # Let everyone know when a job is done
        if self.jobs:
            for job in self.jobs:
                if job.state == "done":
                    self.on_upload_finished(job.dataset_id)

        if not self.isVisible() or not self.jobs:
            # Don't update the UI if nobody is looking anyway.
            return
        if self._on_update_job_status_lock.locked():
            return

        with self._on_update_job_status_lock:
            self.update_job_status()

    def update_job_status(self):
        # disable updates
        self.setUpdatesEnabled(False)
        # make sure the length of the table is long enough
        self.setRowCount(len(self.jobs))
        if self.jobs:
            for row, job in enumerate(self.jobs):
                status = job.get_status()
                self.set_label_item(row, 0, job.dataset_id[:5])
                self.set_label_item(row, 1, self.get_dataset_title(job))
                self.set_label_item(row, 2, status["state"])
                self.set_label_item(row, 3, job.get_progress_string())
                self.set_label_item(row, 4, job.get_rate_string())
                self.set_actions_item(row, 5, job)

                QtWidgets.QApplication.processEvents(
                    QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)

        # enable updates again
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
        """Set/Create a TableCellActionsUpload widget in the table

        Refreshes the widget and also connects signals.
        """
        wid = self.cellWidget(row, col)
        if wid is None:
            wid = TableCellActionsUpload(job, parent=self)
            wid.delete_job.connect(self.on_job_delete)
            wid.abort_job.connect(self.on_job_abort)
            self.setCellWidget(row, col, wid)
        else:
            wid.job = job
        wid.refresh_visibility(job)


@lru_cache(maxsize=10000)
def get_dataset_title(dataset_id):
    api = get_ckan_api()
    ddict = api.get("package_show", id=dataset_id)
    return ddict["title"]
