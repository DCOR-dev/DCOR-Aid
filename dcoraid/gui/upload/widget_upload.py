from functools import lru_cache
import os.path as os_path
import pathlib
import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets
from PyQt5.QtCore import QStandardPaths

from ...upload import UploadQueue
from ...upload import task

from ..api import get_ckan_api
from ..tools import ShowWaitCursor

from . import circle_mgr
from .dlg_upload import NoCircleSelectedError, UploadDialog
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

        self._dlg_manual = None

        # button for adding new dataset manually
        self.toolButton_new_upload.clicked.connect(self.on_upload_manual)

        # menu button for adding tasks
        menu = QtWidgets.QMenu()
        act1 = QtWidgets.QAction("Select task files from disk", self)
        act1.setData("single")
        menu.addAction(act1)
        act2 = QtWidgets.QAction(
            "Recursively find and load task files from a folder", self)
        act2.setData("bulk")
        menu.addAction(act2)
        menu.triggered.connect(self.on_upload_task)
        self.toolButton_load_upload_tasks.setMenu(menu)

        # Underlying upload class
        # use a persistent shelf to be able to resume uploads on startup
        shelf_path = os_path.join(
            QStandardPaths.writableLocation(
                QStandardPaths.AppLocalDataLocation),
            "persistent_upload_jobs")
        self.cache_dir = QtCore.QStandardPaths.writableLocation(
            QtCore.QStandardPaths.CacheLocation)
        self.jobs = UploadQueue(api=get_ckan_api(),
                                path_persistent_job_list=shelf_path,
                                cache_dir=self.cache_dir)
        self.widget_jobs.set_job_list(self.jobs)

        # upload finished signal
        self.widget_jobs.upload_finished.connect(self.upload_finished)

    @QtCore.pyqtSlot()
    def on_upload_manual(self):
        """Guides the user through the process of creating a dataset

        A draft dataset is created to which the resources are then
        uploaded.
        """
        try:
            self._dlg_manual = UploadDialog(self)
        except NoCircleSelectedError:
            self._dlg_manual = None
        else:
            self._dlg_manual.finished.connect(self.on_upload_manual_ready)
            self._dlg_manual.close()
            self._dlg_manual.exec()

    @QtCore.pyqtSlot(object)
    def on_upload_manual_ready(self, upload_dialog):
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
        self.jobs.new_job(dataset_id=dataset_dict["id"],
                          paths=paths,
                          resource_names=names,
                          supplements=supps)

    @QtCore.pyqtSlot(QtWidgets.QAction)
    def on_upload_task(self, action):
        """Import an UploadJob task file and add it to the queue

        This functionality is mainly used for automation. Another
        software creates upload tasks which are then loaded by
        DCOR-Aid.
        """
        if action.data() == "single":
            files, _ = QtWidgets.QFileDialog.getOpenFileNames(
                self,
                "Select DCOR-Aid task files",
                ".",
                "DCOR-Aid task files (*.dcoraid-task)",
            )
        else:
            tdir = QtWidgets.QFileDialog.getExistingDirectory(
                self,
                "Select folder to search for DCOR-Aid task files",
                ".",
                QtWidgets.QFileDialog.ShowDirsOnly,
            )
            files = pathlib.Path(tdir).rglob("*.dcoraid-task")

        # Keep track of a persistent task ID to dataset ID dictionary
        path_id_dict = os_path.join(
            QStandardPaths.writableLocation(
                QStandardPaths.AppLocalDataLocation),
            "map_task_to_dataset_id.txt")
        map_task_to_dataset_id = task.PersistentTaskDatasetIDDict(path_id_dict)
        api = get_ckan_api()
        settings = QtCore.QSettings()
        update_dataset_id = bool(int(settings.value(
            "uploads/update task with dataset id", "1")))
        dataset_kwargs = {}
        for pp in files:
            if (not task.task_has_circle(pp)
                    and "owner_org" not in dataset_kwargs):
                # Let the user choose a circle.
                # Note the above test for "owner_org": The user only
                # chooses the circle *once* for *all* task files.
                cdict = circle_mgr.request_circle(self)
                if cdict is None:
                    # The user aborted, so we won't continue!
                    break
                else:
                    dataset_kwargs["owner_org"] = cdict["name"]
            with ShowWaitCursor():
                upload_job = task.load_task(
                    path=pp,
                    map_task_to_dataset_id=map_task_to_dataset_id,
                    api=api,
                    dataset_kwargs=dataset_kwargs,
                    update_dataset_id=update_dataset_id,
                    cache_dir=self.cache_dir,
                )
                self.jobs.add_job(upload_job)


class UploadTableWidget(QtWidgets.QTableWidget):
    upload_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(UploadTableWidget, self).__init__(*args, **kwargs)
        self.jobs = []  # Will become UploadQueue with self.set_job_list

        settings = QtCore.QSettings()
        if bool(int(settings.value("debug/without timers", "0"))):
            self.timer = None
        else:
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self.update_job_status)
            self.timer.start(30)
        self._finished_uploads = []

    def set_job_list(self, jobs):
        """Set the current job list

        The job list can be a `list`, but it is actually
        an `UploadQueue`.
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
            self.jobs.jobs_eternal.set_job_done(dataset_id)
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
            self.set_label_item(row, 1, get_dataset_title(job.dataset_id))
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


@lru_cache(maxsize=10000)
def get_dataset_title(dataset_id):
    api = get_ckan_api()
    ddict = api.get("package_show", id=dataset_id)
    return ddict["title"]
