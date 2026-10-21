import logging
import threading
import traceback

from PyQt6 import QtCore, QtWidgets

from .widget_actions_download import TableCellActionsDownload


logger = logging.getLogger(__name__)


class DownloadTableWidget(QtWidgets.QTableWidget):
    download_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        self._busy_updating_widgets_lock = threading.Lock()
        super().__init__(*args, **kwargs)
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
                state = status["state"]
                self.set_label_item(row, 0, job.job_id[:5])
            except BaseException:
                logger.error(traceback.format_exc())
                job.set_state("error")
                job.traceback = traceback.format_exc()
                state = "error"

            try:
                title = get_download_title(job)
                progress = job.get_progress_string()
                rate = job.get_rate_string()
            except BaseException:
                logger.error(traceback.format_exc())
                # Probably a connection error
                title = "-- error getting dataset title --"
                progress = "unknown"
                rate = "unknown"

            self.set_label_item(row, 1, title)
            self.set_label_item(row, 2, state)
            self.set_label_item(row, 3, progress)
            self.set_label_item(row, 4, rate)
            self.set_actions_item(row, 5, job)
            if state == "done":
                self.on_download_finished(job.job_id)

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
