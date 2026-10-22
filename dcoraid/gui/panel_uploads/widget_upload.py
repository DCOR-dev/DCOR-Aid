from __future__ import annotations

import warnings
import os.path as os_path
import pathlib
import time

from PyQt6 import QtCore, QtGui, QtWidgets
from PyQt6.QtCore import QStandardPaths

from ...api import APINotFoundError
from ...upload import queue, task

from ..api import get_ckan_api
from ..tools import ShowWaitCursor

from . import circle_mgr
from .dlg_upload import NoCircleSelectedError, UploadDialog
from .widget_upload_ui import Ui_Form


class UploadWidget(QtWidgets.QWidget):
    upload_finished = QtCore.pyqtSignal(dict)

    def __init__(self, *args, **kwargs):
        """Manage running uploads
        """
        super().__init__(*args, **kwargs)
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        # hide side panel at beginning
        self.ui.widget_info.setVisible(False)

        self._dlg_manual = None

        # button for adding new dataset manually
        self.ui.toolButton_new_upload.clicked.connect(self.on_upload_manual)

        # menu button for adding tasks
        menu = QtWidgets.QMenu()
        act1 = QtGui.QAction("Select task files from disk", self)
        act1.setData("single")
        menu.addAction(act1)
        act2 = QtGui.QAction(
            "Recursively find and load task files from a folder", self)
        act2.setData("bulk")
        menu.addAction(act2)
        menu.triggered.connect(self.on_upload_task)
        self.ui.toolButton_load_upload_tasks.setMenu(menu)

        #: path to persistent shelf to be able to resume uploads on startup
        self.shelf_path = os_path.join(
            QStandardPaths.writableLocation(
                QStandardPaths.StandardLocation.AppLocalDataLocation),
            "persistent_upload_jobs")

        # UploadQueue instance
        self._jobs = None

        self.setEnabled(False)
        self.init_timer = QtCore.QTimer(self)
        self.init_timer.setSingleShot(True)
        self.init_timer.setInterval(100)
        self.init_timer.timeout.connect(self.initialize)
        self.init_timer.start()

        # signals
        self.ui.widget_jobs.job_selected.connect(self.on_show_job)

    def __del__(self):
        del self._jobs

    @property
    def cache_dir(self):
        """path to cache directory (compression)"""
        fallback = QStandardPaths.writableLocation(
            QStandardPaths.StandardLocation.CacheLocation)
        settings = QtCore.QSettings()
        cache_path = settings.value("uploads/cache path", fallback)
        return cache_path

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

    def dragEnterEvent(self, e):
        """Whether files are accepted"""
        if e.mimeData().hasUrls():
            e.accept()
        else:
            e.ignore()

    def dropEvent(self, e):
        """Add dropped files to view"""
        urls = e.mimeData().urls()
        for ff in urls:
            pp = pathlib.Path(ff.toLocalFile())
            if pp.is_dir():
                self.on_upload_task(sorted(pp.rglob("*.dcoraid-task")))
            elif pp.suffix == ".dcoraid-task":
                self.on_upload_task(pp)

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
                    msg.setIcon(QtWidgets.QMessageBox.Icon.Information)
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
                    msg.exec()
            if self.parent().parent().isVisible():
                self.ui.widget_jobs.set_job_list(self.jobs)
                # upload finished signal
                self.ui.widget_jobs.upload_finished.connect(
                    self.upload_finished)
        elif retry_if_fail:
            # try again
            self.init_timer.setInterval(1000)
            self.init_timer.start()

    @QtCore.pyqtSlot(object)
    def on_show_job(self, job):
        self.ui.widget_info.setVisible(True)
        self.ui.label_index.setText(f"{self.jobs.index(job) + 1}")
        self.ui.lineEdit_id.setText(job.dataset_id)
        self.ui.lineEdit_id.setCursorPosition(0)
        self.ui.label_title.setText(self.ui.widget_jobs.get_dataset_title(job))
        size = sum(job.file_sizes) / 1024 ** 3
        if size <= 0.01:
            size_str = f"{size * 1024:.2f} MB"
        else:
            size_str = f"{size:.2f} GB"
        self.ui.label_size.setText(size_str)
        paths = [f"{p}" for p in job.paths]
        self.ui.plainTextEdit_paths.setPlainText("\n".join(paths))

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

    @QtCore.pyqtSlot(QtGui.QAction)
    def on_upload_task(
            self,
            action: QtGui.QAction | pathlib.Path | list[pathlib.Path]):
        """Import an UploadJob task file and add it to the queue

        This functionality is mainly used for automation. Another
        software creates upload tasks which are then loaded by
        DCOR-Aid.
        """
        if isinstance(action, pathlib.Path):
            # special case for drag&drop and for docs generation
            files = [action]
        elif isinstance(action, list):
            # special case for drag and drop
            files = [f for f in action if isinstance(f, pathlib.Path)]
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
                QtWidgets.QFileDialog.Option.ShowDirsOnly,
            )
            files = pathlib.Path(tdir).rglob("*.dcoraid-task")

        # Keep track of a persistent task ID to dataset ID dictionary
        path_id_dict = os_path.join(
            QStandardPaths.writableLocation(
                QStandardPaths.StandardLocation.AppLocalDataLocation),
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
            load_kw = {
                "path": pp,
                "map_task_to_dataset_id": map_task_to_dataset_id,
                "api": api,
                "dataset_kwargs": dataset_kwargs,
                "update_dataset_id": update_dataset_id,
                "cache_dir": self.cache_dir}
            try:
                with ShowWaitCursor():
                    upload_job = task.load_task(**load_kw)
            except APINotFoundError:
                # This may happen when the dataset is not found online (#19).
                # Give the user the option to create a new dataset.
                ret = QtWidgets.QMessageBox.question(
                    self,
                    "Dataset not found on DCOR server",
                    f"The dataset specified in, or inferred from '{pp}' does "
                    f"not exist on '{api.server}'. "
                    f"Possible reasons:"
                    f"<ul>"
                    f"<li>"
                    f"<b>You deleted an upload job or a draft dataset</b>: "
                    f"Creating a new dataset is safe in this case."
                    f"</li>"
                    f"<li>"
                    f"<b>This task file was created by a different user</b> "
                    f"on the '{api.server}' instance: You should not "
                    f"continue and instead set the correct API token in the "
                    f"settings."
                    f"</li>"
                    f"<li>"
                    f"<b>This task file was created using a different "
                    f"DCOR instance</b>: You should not continue and set the "
                    f"correct DCOR server and API token in the settings."
                    f"</li>"
                    f"</ul>"
                    f"Would you like to create a new dataset for this "
                    f"task file on '{api.server}' (select 'No' if in doubt)?"
                )
                if ret == QtWidgets.QMessageBox.StandardButton.Yes:
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

    def prepare_quit(self):
        """Should be called before the application quits"""
        self.init_timer.stop()
        if self.ui.widget_jobs.timer is not None:
            self.ui.widget_jobs.timer.stop()
        if self._jobs is not None:
            self._jobs.__del__()
