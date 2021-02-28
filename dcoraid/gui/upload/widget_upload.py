import pathlib
import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets

from ...upload import UploadQueue

from ..api import get_ckan_api
from ..tools import ShowWaitCursor

from .dlg_upload import UploadDialog
from .widget_tablecell_actions import TableCellActions


class UploadWidget(QtWidgets.QWidget):
    upload_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Manage running uploads
        """
        super(UploadWidget, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "widget_upload.ui")
        uic.loadUi(path_ui, self)

        self.toolButton_new_upload.clicked.connect(self.on_draft_upload)

        # Underlying upload class
        self.jobs = UploadQueue(api=get_ckan_api())
        self.widget_jobs.set_job_list(self.jobs)

        # upload finished signal
        self.widget_jobs.upload_finished.connect(self.upload_finished)

    @QtCore.pyqtSlot()
    def on_draft_upload(self):
        """Guides the user through the process of creating a dataset

        A draft dataset is created to which the resources are then
        uploaded.
        """
        with ShowWaitCursor():
            dlg = UploadDialog(self)
            dlg.finished.connect(self.on_run_upload)
        dlg.exec()

    @QtCore.pyqtSlot(object)
    def on_run_upload(self, upload_dialog):
        """Proceed with resource upload as defined by `update_dialog`

        `update_dialog` is an instance of `dlg_upload.UploadDialog`
        and contains all information necessary to run the resource
        upload. Supplementary resource metadata is extracted here
        as well.
        """
        # Remove all magic keys from the schema data (they are
        # only used internally by DCOR-Aid and don't belong on DCOR).
        rdata = upload_dialog.rvmodel.get_all_data(magic_keys=False)
        paths = []
        names = []
        supps = []
        for path in rdata:
            paths.append(pathlib.Path(path))
            names.append(rdata[path]["file"]["filename"])
            supps.append(rdata[path]["supplement"])
        dataset_dict = upload_dialog.dataset_dict
        # add the entry to the job list
        self.jobs.add_job(dataset_dict, paths=paths, resource_names=names,
                          supplements=supps)


class UploadTableWidget(QtWidgets.QTableWidget):
    upload_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(UploadTableWidget, self).__init__(*args, **kwargs)
        self.jobs = []  # Will become UploadJobList with self.set_job_list
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.update_job_status)
        self.timer.start(30)
        self._finished_uploads = []

    def set_job_list(self, jobs):
        """Set the current job list

        The job list can be a `list`, but it is actually
        an `UploadJobList`.
        """
        # This is the actual initialization
        self.jobs = jobs

    def on_job_abort(self, dataset_id):
        self.jobs.abort_job(dataset_id)

    def on_job_delete(self, dataset_id):
        self.jobs.remove_job(dataset_id)

    def on_upload_finished(self, dataset_id):
        """Triggers upload_finished whenever an upload is finished"""
        if dataset_id not in self._finished_uploads:
            self._finished_uploads.append(dataset_id)
            self.upload_finished.emit()

    @QtCore.pyqtSlot()
    def update_job_status(self):
        """Update UI with information from self.jobs (UploadJobList)"""
        # disable updates
        self.setUpdatesEnabled(False)
        # make sure the length of the table is long enough
        self.setRowCount(len(self.jobs))
        self.setColumnCount(6)

        for row, job in enumerate(self.jobs):
            status = job.get_status()
            self.set_label_item(row, 0, job.dataset_id[:5])
            self.set_label_item(row, 1, job.dataset_dict["title"])
            self.set_label_item(row, 2, status["state"])
            self.set_label_item(row, 3, job.get_progress_string())
            self.set_label_item(row, 4, job.get_rate_string())
            if status["state"] == "done":
                self.on_upload_finished(job.dataset_id)
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
        wid = self.cellWidget(row, col)
        if wid is None:
            wid = TableCellActions(job)
            wid.delete_job.connect(self.on_job_delete)
            wid.abort_job.connect(self.on_job_abort)
            self.setCellWidget(row, col, wid)
        wid.refresh_visibility(job)
