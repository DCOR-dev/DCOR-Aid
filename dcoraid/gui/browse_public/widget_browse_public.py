import traceback as tb

from importlib import resources

from PyQt5 import uic, QtCore, QtWidgets

from ...common import ConnectionTimeoutErrors
from ...dbmodel import APIInterrogator

from ..api import get_ckan_api
from ..main import DCORAid


class BrowsePublic(QtWidgets.QWidget):
    request_download = QtCore.pyqtSignal(str, bool)

    def __init__(self, *args, **kwargs):
        """Browse public DCOR data"""
        super(BrowsePublic, self).__init__(*args, **kwargs)
        ref_ui = resources.files(
            "dcoraid.gui.browse_public") / "widget_browse_public.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        # Signals for public data browser
        self.pushButton_public_search.clicked.connect(self.on_public_search)
        self.public_filter_chain.download_resource.connect(
            self.request_download)

    @QtCore.pyqtSlot()
    def on_public_search(self):
        self.setCursor(QtCore.Qt.WaitCursor)
        api = get_ckan_api(
            public=not self.checkBox_public_include_private.isChecked())
        try:
            ai = APIInterrogator(api=api)
            dbextract = ai.search_dataset(
                self.lineEdit_public_search.text(),
                limit=self.spinBox_public_rows.value())
            self.public_filter_chain.set_db_extract(dbextract)
        except ConnectionTimeoutErrors:
            self.logger.error(tb.format_exc())
            QtWidgets.QMessageBox.critical(
                self,
                f"Failed to connect to {api.server}",
                tb.format_exc(limit=1))
        self.setCursor(QtCore.Qt.ArrowCursor)

    @staticmethod
    def find_main_window():
        # Global function to find the (open) QMainWindow in application
        app = QtWidgets.QApplication.instance()
        for widget in app.topLevelWidgets():
            if isinstance(widget, DCORAid):
                return widget
