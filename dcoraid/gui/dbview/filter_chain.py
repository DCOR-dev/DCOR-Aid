import copy

from PyQt6 import QtCore, QtWidgets

from ...common import is_dc_resource_dict
from .filter_chain_ui import Ui_Form


class FilterChain(QtWidgets.QWidget):
    download_item = QtCore.pyqtSignal(str, str, bool)

    def __init__(self, *args, **kwargs):
        """Filter chain widget with multiple filter views
        """
        super().__init__(*args, **kwargs)
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        #: Current database extract
        #: (instance of :class:`dcoraid.dbmodel.core.DBExtract`)
        self.db_extract = None

        # Signals and slots
        # circle selection changed
        self.ui.fw_circles.selection_changed.connect(self.on_select_circles)
        # collection selection changed
        self.ui.fw_collections.selection_changed.connect(self.update_datasets)
        # dataset selection changed
        self.ui.fw_datasets.selection_changed.connect(self.update_resources)
        # resource filtered by .rtdc
        self.ui.fw_resources.ui.checkBox.stateChanged.connect(
            self.update_resources)
        # request resource downloads
        self.ui.fw_circles.download_item.connect(self.download_item)
        self.ui.fw_collections.download_item.connect(self.download_item)
        self.ui.fw_datasets.download_item.connect(self.download_item)
        self.ui.fw_resources.download_item.connect(self.download_item)

    @property
    def selected_circles(self):
        """Circles currently selected"""
        circs = self.ui.fw_circles.get_entry_identifiers(selected=True,
                                                         which="name")
        if not circs:
            circs = self.ui.fw_circles.get_entry_identifiers(
                selected=False, which="name")
        return circs

    @property
    def selected_collections(self):
        """Collections currently selected"""
        return self.ui.fw_collections.get_entry_identifiers(selected=True,
                                                            which="name")

    @property
    def selected_datasets(self):
        """Datasets currently selected"""
        return self.ui.fw_datasets.get_entry_identifiers(selected=True,
                                                         which="name")

    @QtCore.pyqtSlot()
    def on_select_circles(self):
        self.update_collections()
        self.update_datasets()

    def set_db_extract(self, db_extract):
        """Set the database model

        Parameters
        ----------
        db_extract: dcoraid.dbmodel.db_core.DBExtract
            Subclass of DBModel
        """
        self.db_extract = db_extract
        # reset all lists
        self.ui.fw_circles.set_entries(
            copy.deepcopy(self.db_extract.circles))
        # collections
        self.update_collections()
        # datasets
        self.update_datasets()

    @QtCore.pyqtSlot()
    def update_collections(self):
        self.ui.fw_collections.blockSignals(True)
        self.ui.fw_collections.set_entries(
            copy.deepcopy(self.db_extract.collections))
        self.ui.fw_collections.blockSignals(False)

    @QtCore.pyqtSlot()
    def update_datasets(self):
        circles = self.selected_circles
        collections = self.selected_collections
        dataset_items = []
        for ds in self.db_extract.datasets:
            if ds["organization"]["name"] not in circles:
                # dataset is not part of the circle
                continue
            if collections:
                ds_collections = [g.get("name") for g in ds.get("groups", {})]
                if not set(collections) & set(ds_collections):
                    # collections have been selected and the dataset is not
                    # part of any
                    continue
            dataset_items.append(ds)
        self.ui.fw_datasets.blockSignals(True)
        self.ui.fw_datasets.set_entries(dataset_items)
        self.ui.fw_datasets.blockSignals(False)
        self.update_resources()

    @QtCore.pyqtSlot()
    def update_resources(self):
        rs_entries = []
        for dn in self.selected_datasets:
            ddict = self.db_extract.get_dataset_dict(dn)
            for rs in ddict.get("resources", []):
                if (self.ui.fw_resources.ui.checkBox.isChecked()
                        and not is_dc_resource_dict(rs)):
                    # Ignore non-RT-DC mimetypes
                    continue
                else:
                    rs_entries.append(rs)
        self.ui.fw_resources.set_entries(rs_entries)
