import logging
import warnings
from functools import lru_cache
import os.path as os_path
import pathlib
import pkg_resources
import time
import traceback as tb

from PyQt5 import uic, QtCore, QtWidgets
from PyQt5.QtCore import QStandardPaths

from ...api import APINotFoundError
from ...upload import queue, task

from ..api import get_ckan_api
from ..tools import ShowWaitCursor, show_wait_cursor

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

        # hide side panel at beginning
        self.widget_info.setVisible(False)

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

        #: path to persistent shelf to be able to resume uploads on startup
        self.shelf_path = os_path.join(
            QStandardPaths.writableLocation(
                QStandardPaths.AppLocalDataLocation),
            "persistent_upload_jobs")
        #: path to cache directory (compression)
        self.cache_dir = QtCore.QStandardPaths.writableLocation(
            QtCore.QStandardPaths.CacheLocation)

        # UploadQueue instance
        self._jobs = None

        self.setEnabled(False)
        self.init_timer = QtCore.QTimer(self)
        self.init_timer.setSingleShot(True)
        self.init_timer.setInterval(100)
        self.init_timer.timeout.connect(self.initialize)
        self.init_timer.start()

        # signals
        self.widget_jobs.job_selected.connect(self.on_show_job)

    def __del__(self):
        del self._jobs

    @property
    def jobs(self):
        for ii in range(50):
            if self._jobs is None:
                # force initialization
                self.initialize(retry_if_fail=False)
                time.sleep(.2)
            else:
                # This is the default route after initialization was complete.
                break
        else:
            api = get_ckan_api()
            raise ValueError("Could not initialize upload job list. Please "
                             f"verify your connection to '{api.server}'!")
        return self._jobs

    @show_wait_cursor
    @QtCore.pyqtSlot()
    def initialize(self, retry_if_fail=True):
        if self._jobs is not None:
            # Nothing to do
            return
        api = get_ckan_api()
        if api.is_available(with_api_key=True, with_correct_version=True):
            self.setEnabled(True)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                self._jobs = queue.UploadQueue(
                    api=get_ckan_api(),
                    path_persistent_job_list=self.shelf_path,
                    cache_dir=self.cache_dir)
                w = [wi for wi in w
                     if issubclass(wi.category,
                                   queue.DCORAidQueueMissingResourceWarning)]
                if w:
                    msg = QtWidgets.QMessageBox()
                    msg.setIcon(QtWidgets.QMessageBox.Information)
                    msg.setText(
                        "You have created upload jobs in a previous session "
                        + "and some of the resources cannot be located on "
                        + "this machine. You might need to insert an external "
                        + "hard drive or mount a network share and then "
                        + "restart DCOR-Aid. If these uploads were created "
                        + "from .dcoraid-task files that are now in a "
                        + "different location, it might help to load those "
                        + "tasks from that new location. For now, these "
                        + "uploads are not queued (but we have not forgotten "
                        + "them).")
                    msg.setDetailedText(
                        "\n\n".join([str(wi.message) for wi in w]))
                    msg.setWindowTitle("Resources for uploads missing")
                    msg.exec_()
            self.widget_jobs.set_job_list(self.jobs)
            # upload finished signal
            self.widget_jobs.upload_finished.connect(self.upload_finished)
        elif retry_if_fail:
            # try again
            self.init_timer.setInterval(1000)
            self.init_timer.start()

    @QtCore.pyqtSlot(object)
    def on_show_job(self, job):
        self.widget_info.setVisible(True)
        self.label_index.setText(f"{self.jobs.index(job) + 1}")
        self.lineEdit_id.setText(job.dataset_id)
        self.lineEdit_id.setCursorPosition(0)
        self.label_title.setText(self.widget_jobs.get_dataset_title(job))
        size = sum(job.file_sizes) / 1024 ** 3
        if size <= 0.01:
            size_str = f"{size * 1024:.2f} MB"
        else:
            size_str = f"{size:.2f} GB"
        self.label_size.setText(size_str)
        paths = [f"{p}" for p in job.paths]
        self.plainTextEdit_paths.setPlainText("\n".join(paths))

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
        if isinstance(action, pathlib.Path):
            # special case for docs generation
            files = [action]
        elif action.data() == "single":
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
        jobs_known_count = 0
        jobs_imported_count = 0
        jobs_total_count = 0
        jobs_ignored_count = 0
        for pp in files:
            jobs_total_count += 1
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
            load_kw = dict(
                path=pp,
                map_task_to_dataset_id=map_task_to_dataset_id,
                api=api,
                dataset_kwargs=dataset_kwargs,
                update_dataset_id=update_dataset_id,
                cache_dir=self.cache_dir)
            try:
                with ShowWaitCursor():
                    upload_job = task.load_task(**load_kw)
            except APINotFoundError:
                # This may happen when the dataset is not found online (#19).
                # Give the user the option to create a new dataset.
                ret = QtWidgets.QMessageBox.question(
                    self,
                    "Dataset ID in task file not found on DCOR server",
                    f"The dataset speficied in {pp} does not exist on "
                    + f"{api.server}. Possible reasons:"
                    + "<ul>"
                    + "<li>You deleted an upload job or a draft dataset.</li>"
                    + "<li>This task file was created by a different "
                    + f"user on the {api.server} instance.</li>"
                    + "<li>This task file was created using a different DCOR "
                    + "instance.</li>"
                    + "</ul>"
                    + "Would you like to create a new dataset for this "
                    + f"task file on {api.server} (select 'No' if in doubt)?"
                )
                if ret == QtWidgets.QMessageBox.Yes:
                    # retry, this time forcing the creation of a new dataset
                    upload_job = task.load_task(force_dataset_creation=True,
                                                **load_kw)
                else:
                    # ignore this task
                    jobs_ignored_count += 1
                    continue

            # proceed with adding the job
            job_msg = self.jobs.add_job(upload_job)
            if job_msg == "new":
                jobs_imported_count += 1
            else:
                jobs_known_count += 1

        # sanity check
        assert jobs_total_count == \
               jobs_known_count + jobs_imported_count + jobs_ignored_count

        # give the user some stats
        messages = [f"Found {jobs_total_count} task(s) and imported "
                    + f"{jobs_imported_count} task(s)."]
        if jobs_known_count:
            messages.append(
                f"{jobs_known_count} tasks were already imported in a "
                + "previous DCOR-Aid session.")
        if jobs_ignored_count:
            messages.append(
                f"{jobs_ignored_count} tasks were not imported, because their "
                + "dataset IDs are not known on the present DCOR instance.")

        # Display a message box telling the user that this job is known
        QtWidgets.QMessageBox.information(
            self,
            "Task import complete",
            "\n\n".join(messages),
        )

    def stop_timers(self):
        """Should be called before the application quits"""
        self.init_timer.stop()
        if self.widget_jobs.timer is not None:
            self.widget_jobs.timer.stop()


class UploadTableWidget(QtWidgets.QTableWidget):
    upload_finished = QtCore.pyqtSignal()
    job_selected = QtCore.pyqtSignal(object)

    def __init__(self, *args, **kwargs):
        super(UploadTableWidget, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__).getChild("UploadTableWidget")
        self.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        self._jobs = None

        settings = QtCore.QSettings()
        if bool(int(settings.value("debug/without timers", "0"))):
            self.timer = None
        else:
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self.update_job_status)
            self.timer.start(1000)
        self._finished_uploads = []

        # signals
        self.itemSelectionChanged.connect(self.on_selection)
        self.itemClicked.connect(self.on_selection)

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

    @QtCore.pyqtSlot(str)
    def on_job_delete(self, dataset_id):
        self.jobs.remove_job(dataset_id)

    @QtCore.pyqtSlot()
    def on_selection(self):
        row = self.currentRow()
        job = self.jobs[row]
        self.job_selected.emit(job)

    @QtCore.pyqtSlot(str)
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
        if self.jobs:
            self.setRowCount(len(self.jobs))
            self.setColumnCount(6)

            for row, job in enumerate(self.jobs):
                status = job.get_status()
                self.set_label_item(row, 0, job.dataset_id[:5])
                self.set_label_item(row, 1, self.get_dataset_title(job))
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
            wid = TableCellActions(job, parent=self)
            wid.delete_job.connect(self.on_job_delete)
            wid.abort_job.connect(self.on_job_abort)
            self.setCellWidget(row, col, wid)
        wid.refresh_visibility(job)


@lru_cache(maxsize=10000)
def get_dataset_title(dataset_id):
    api = get_ckan_api()
    ddict = api.get("package_show", id=dataset_id)
    return ddict["title"]
