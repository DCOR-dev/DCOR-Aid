from os import path as os_path
import shutil

import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets
from PyQt5.QtCore import QStandardPaths

from ...api import dataset_draft_remove_all
from ...upload import PersistentUploadJobList

from ..api import get_ckan_api
from ..main import DCORAid
from ..tools import ShowWaitCursor


class MaintenanceWidget(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        """Maintenance tasks
        """
        super(MaintenanceWidget, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.maintenance", "widget_maintenance.ui")
        uic.loadUi(path_ui, self)

        self.toolButton_cache.clicked.connect(self.on_clear_cache)
        self.toolButton_drafts.clicked.connect(self.on_remove_drafts)

    @staticmethod
    def find_main_window():
        # Global function to find the (open) QMainWindow in application
        app = QtWidgets.QApplication.instance()
        for widget in app.topLevelWidgets():
            if isinstance(widget, DCORAid):
                return widget

    @QtCore.pyqtSlot()
    def on_clear_cache(self):
        """Clear local upload cache"""
        mw = self.find_main_window()
        queue = mw.panel_upload.jobs
        dirs = queue.find_zombie_caches()
        msg = QtWidgets.QMessageBox()
        msg.setIcon(QtWidgets.QMessageBox.Information)
        if dirs:
            [shutil.rmtree(pp, ignore_errors=True) for pp in dirs]
            details = [f"- {pp}" for pp in dirs]
            msg.setText(f"Directories removed: {len(dirs)}")
            msg.setDetailedText("\n".join(details))
            msg.setWindowTitle("Success")
        else:
            msg.setText("No zombie cache data found.")
            msg.setWindowTitle("Nothing to do")
        msg.exec_()

    @QtCore.pyqtSlot()
    def on_remove_drafts(self):
        with ShowWaitCursor():
            # get all dataset IDs that should not be removed
            pers_job_path = os_path.join(
                QStandardPaths.writableLocation(
                    QStandardPaths.AppLocalDataLocation),
                "persistent_upload_jobs")
            pers_datasets = PersistentUploadJobList(pers_job_path)
            # perform deletion
            deleted, ignored = dataset_draft_remove_all(
                api=get_ckan_api(),
                ignore_dataset_ids=pers_datasets)
        msg = QtWidgets.QMessageBox()
        msg.setIcon(QtWidgets.QMessageBox.Information)
        del_titles = [f"{d['name']}" for d in deleted]
        ign_titles = [f"{d['name']}" for d in ignored]
        if del_titles + ign_titles:
            msg.setText(f"Drafts removed: {len(del_titles)}\n"
                        + f"Ignored: {len(ign_titles)}\n")
            msg.setDetailedText("Ignored (pending upload):\n"
                                + "\n".join(ign_titles)
                                + "\n\nDeleted:\n"
                                + "\n".join(del_titles))
            msg.setWindowTitle("Success")
        else:
            msg.setText("No drafts found.")
            msg.setWindowTitle("Nothing to do")
        msg.exec_()
