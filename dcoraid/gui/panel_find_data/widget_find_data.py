import logging

from importlib import resources

from PyQt6 import uic, QtCore, QtWidgets

from ..main import DCORAid
from ..status_widget import StatusWidget


logger = logging.getLogger(__name__)


class WidgetFindData(QtWidgets.QWidget):
    download_item = QtCore.pyqtSignal(str, str, bool)

    def __init__(self, *args, **kwargs):
        """Browse DCOR data"""
        super(WidgetFindData, self).__init__(*args, **kwargs)
        ref_ui = resources.files(
            "dcoraid.gui.panel_find_data") / "widget_find_data.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        settings = QtCore.QSettings()
        server = settings.value("auth/server", "dcor.mpl.mpg.de")
        title = StatusWidget.get_title(server)
        self.label_search.setText(f"Search {title or 'DCOR'}")

        self.database = None

        # Signals for data browser
        self.pushButton_search.clicked.connect(self.on_search)
        self.pushButton_update_db.clicked.connect(self.on_update_db)
        self.public_filter_chain.download_item.connect(self.download_item)

    @QtCore.pyqtSlot()
    def on_search(self):
        mv = self.find_main_window()
        mv.check_update_database()
        if mv.database:
            self.setCursor(QtCore.Qt.CursorShape.WaitCursor)
            dbe = self.database.search_dataset(
                self.lineEdit_search.text(),
                limit=self.spinBox_public_rows.value())
            self.public_filter_chain.set_db_extract(dbe)
            self.setCursor(QtCore.Qt.CursorShape.ArrowCursor)

    @QtCore.pyqtSlot()
    def on_update_db(self):
        self.find_main_window().check_update_database(force=True)

    @staticmethod
    def find_main_window():
        # Global function to find the (open) QMainWindow in application
        app = QtWidgets.QApplication.instance()
        for widget in app.topLevelWidgets():
            if isinstance(widget, DCORAid):
                return widget

    def set_database(self, database):
        self.database = database
