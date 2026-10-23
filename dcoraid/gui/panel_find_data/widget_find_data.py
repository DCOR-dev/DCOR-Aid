import logging

from PyQt6 import QtCore, QtWidgets

from ..api import get_ckan_api
from ..status_widget import StatusWidget
from .widget_find_data_ui import Ui_Form


logger = logging.getLogger(__name__)


class WidgetFindData(QtWidgets.QWidget):
    download_item = QtCore.pyqtSignal(str, str, bool)

    def __init__(self, *args, **kwargs):
        """Browse DCOR data"""
        super().__init__(*args, **kwargs)
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        # Use the server URL of the API, which contains the scheme
        # (the server might be stored without scheme in the settings).
        title = StatusWidget.get_title(get_ckan_api().server)
        self.ui.label_search.setText(f"Search {title or 'DCOR'}")

        self.database = None

        # Signals for data browser
        self.ui.pushButton_search.clicked.connect(self.on_search)
        self.ui.pushButton_update_db.clicked.connect(self.on_update_db)
        self.ui.public_filter_chain.download_item.connect(self.download_item)

    @QtCore.pyqtSlot()
    def on_search(self):
        mv = self.find_main_window()
        mv.check_update_database()
        if mv.database:
            self.setCursor(QtCore.Qt.CursorShape.WaitCursor)
            dbe = self.database.search_dataset(
                self.ui.lineEdit_search.text(),
                limit=self.ui.spinBox_public_rows.value())
            self.ui.public_filter_chain.set_db_extract(dbe)
            self.setCursor(QtCore.Qt.CursorShape.ArrowCursor)

    @QtCore.pyqtSlot()
    def on_update_db(self):
        self.find_main_window().check_update_database(force=True)

    @staticmethod
    def find_main_window():
        # import here to avoid circular import via main_ui
        from ..main import DCORAid
        # Global function to find the (open) QMainWindow in application
        app = QtWidgets.QApplication.instance()
        for widget in app.topLevelWidgets():
            if isinstance(widget, DCORAid):
                return widget

    def set_database(self, database):
        self.database = database
