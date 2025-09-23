import logging
import traceback as tb

from importlib import resources

from PyQt6 import uic, QtCore, QtWidgets

from ...common import ConnectionTimeoutErrors
from ...dbmodel import DBExtract

from ..api import get_ckan_api
from ..main import DCORAid


logger = logging.getLogger(__name__)


class WidgetMyData(QtWidgets.QWidget):
    download_item = QtCore.pyqtSignal(str, str, bool)

    def __init__(self, *args, **kwargs):
        """Browse public DCOR data"""
        super(WidgetMyData, self).__init__(*args, **kwargs)
        ref_ui = resources.files(
            "dcoraid.gui.panel_my_data") / "widget_my_data.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        # Signals for user datasets (my data)
        self.pushButton_user_refresh.clicked.connect(
            self.on_refresh_private_data)
        self.user_filter_chain.download_item.connect(self.download_item)
        self.user_filter_chain.added_datasets_to_collection.connect(
            self.on_added_datasets_to_collection)

    @QtCore.pyqtSlot(dict, list)
    def on_added_datasets_to_collection(self, collection, dataset_ids):
        """User manually added a bunch of datasets to a collection"""
        # Get the collection
        for col in self.database.get_collections(refresh=True):
            if col == collection["name"]:
                cid = collection["id"]
                break
        else:
            raise KeyError(f"Collection {collection['id']} not found!")
        # Append it to each dataset
        for ds_id in dataset_ids:
            ds_dict = self.database.get_dataset_dict(ds_id)
            collections = [g["id"] for g in ds_dict["groups"]]
            if cid in collections:
                c_idx = collections.index(cid)
                ds_dict["groups"][c_idx].update(collection)
            else:
                ds_dict["groups"].append(collection)
            self.database.update_dataset(ds_dict)

    @QtCore.pyqtSlot()
    def on_refresh_private_data(self):
        self.find_main_window().check_update_database(force=True)
        self.setCursor(QtCore.Qt.CursorShape.WaitCursor)
        api = get_ckan_api()
        data = DBExtract()
        if api.is_available() and api.api_key:
            try:
                if self.checkBox_user_following.isChecked():
                    data += self.database.get_datasets_user_following()
                if self.checkBox_user_owned.isChecked():
                    data += self.database.get_datasets_user_owned()
                if self.checkBox_user_shared.isChecked():
                    data += self.database.get_datasets_user_shared()
                self.user_filter_chain.set_db_extract(data)
            except ConnectionTimeoutErrors:
                logger.error(tb.format_exc())
                QtWidgets.QMessageBox.critical(
                    self,
                    f"Failed to connect to {api.server}",
                    tb.format_exc(limit=1))
        self.setCursor(QtCore.Qt.CursorShape.ArrowCursor)

    @staticmethod
    def find_main_window():
        # Global function to find the (open) QMainWindow in application
        app = QtWidgets.QApplication.instance()
        for widget in app.topLevelWidgets():
            if isinstance(widget, DCORAid):
                return widget

    def set_database(self, database):
        self.database = database
