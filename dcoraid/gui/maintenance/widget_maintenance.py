import shutil

import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets

from ...upload.dataset import remove_all_drafts

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
            data = remove_all_drafts(api=get_ckan_api())
        msg = QtWidgets.QMessageBox()
        msg.setIcon(QtWidgets.QMessageBox.Information)
        if len(data):
            details = [f"{d['title']} ({d['name']})" for d in data]
            msg.setText(f"Drafts removed: {len(data)}")
            msg.setDetailedText("\n".join(details))
            msg.setWindowTitle("Success")
        else:
            msg.setText("No drafts found.")
            msg.setWindowTitle("Nothing to do")
        msg.exec_()
