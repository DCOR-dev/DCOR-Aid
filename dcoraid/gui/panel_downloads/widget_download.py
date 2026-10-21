import logging
import os.path as os_path
import threading

from PyQt6 import QtCore, QtWidgets
from PyQt6.QtCore import QStandardPaths

from ...common import is_dc_resource_dict
from ...download import DownloadQueue

from ..api import get_ckan_api
from ..tools import show_wait_cursor
from .widget_download_ui import Ui_Form


logger = logging.getLogger(__name__)


class DownloadWidget(QtWidgets.QWidget):
    download_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Manage running downloads
        """
        super().__init__(*args, **kwargs)
        self.ui = Ui_Form()
        self.ui.setupUi(self)

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
                    self.ui.widget_jobs.set_job_list(self._jobs)

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
        if self.ui.widget_jobs.timer is not None:
            self.ui.widget_jobs.timer.stop()
        if self.jobs is not None:
            self.jobs.__del__()

    def set_database(self, database):
        self.database = database
