import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets

from ...upload.dataset import remove_all_drafts

from ..api import get_ckan_api
from ..tools import ShowWaitCursor


class MaintenanceWidget(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        """Maintenance tasks
        """
        super(MaintenanceWidget, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.maintenance", "widget_maintenance.ui")
        uic.loadUi(path_ui, self)

        self.toolButton_drafts.clicked.connect(self.on_remove_drafts)

    @QtCore.pyqtSlot()
    def on_remove_drafts(self):
        with ShowWaitCursor():
            data = remove_all_drafts(api=get_ckan_api())
        msg = QtWidgets.QMessageBox()
        msg.setIcon(QtWidgets.QMessageBox.Information)
        if len(data):
            details = ["{} ({})".format(d["title"], d["name"]) for d in data]
            msg.setText("Drafts removed: {}".format(len(data)))
            msg.setDetailedText("\n".join(details))
            msg.setWindowTitle("Success")
        else:
            msg.setText("No drafts found.")
            msg.setWindowTitle("Nothing to do")
        msg.exec_()
