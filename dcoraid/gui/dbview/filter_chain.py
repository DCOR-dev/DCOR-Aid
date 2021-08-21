import copy

import pkg_resources

from PyQt5 import QtCore, QtWidgets, uic


class FilterChain(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        """Filter chain widget with multiple filter views
        """
        super(FilterChain, self).__init__(*args, **kwargs)
        QtWidgets.QMainWindow.__init__(self)
        path_ui = pkg_resources.resource_filename("dcoraid.gui.dbview",
                                                  "filter_chain.ui")
        uic.loadUi(path_ui, self)

        #: Current database extract
        #: (instance of :class:`dcoraid.dbmodel.core.DBExtract`)
        self.db_extract = None

        # Signals and slots
        # circle selection changed
        self.fw_circles.selection_changed.connect(self.on_select_circles)
        # collection selection changed
        self.fw_collections.selection_changed.connect(self.update_datasets)
        # dataset selection changed
        self.fw_datasets.selection_changed.connect(self.update_resources)
        # resource filtered by .rtdc
        self.fw_resources.checkBox.stateChanged.connect(self.update_resources)

    @property
    def selected_circles(self):
        """Circles currently selected"""
        circs = self.fw_circles.get_entry_identifiers(selected=True)
        if not circs:
            circs = self.fw_circles.get_entry_identifiers(selected=False)
        return circs

    @property
    def selected_collections(self):
        """Collections currently selected"""
        return self.fw_collections.get_entry_identifiers(selected=True)

    @property
    def selected_datasets(self):
        """Datasets currently selected"""
        return self.fw_datasets.get_entry_identifiers(selected=True)

    @QtCore.pyqtSlot()
    def on_select_circles(self):
        self.update_collections()
        self.update_datasets()

    def set_db_extract(self, db_extract):
        """Set the database model

        Parameters
        ----------
        db_extract: dcoraid.dbmodel.core.DBExtract
            Subclass of DBModel
        """
        self.db_extract = db_extract
        # reset all lists
        self.fw_circles.set_entries(
            copy.deepcopy(self.db_extract.circles))
        # collections
        self.update_collections()
        # datasets
        self.update_datasets()

    def update_collections(self):
        self.fw_collections.blockSignals(True)
        self.fw_collections.set_entries(
            copy.deepcopy(self.db_extract.collections))
        self.fw_collections.blockSignals(False)

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
        self.fw_datasets.blockSignals(True)
        self.fw_datasets.set_entries(dataset_items)
        self.fw_datasets.blockSignals(False)
        self.update_resources()

    @QtCore.pyqtSlot()
    def update_resources(self):
        rs_entries = []
        for dn in self.selected_datasets:
            ddict = self.db_extract.get_dataset_dict(dn)
            for rs in ddict["resources"]:
                if (self.fw_resources.checkBox.isChecked()
                    and ("mimetype" in rs
                         and rs["mimetype"] != "RT-DC")):
                    # Ignore non-RT-DC mimetypes
                    continue
                else:
                    rs_entries.append(rs)
        self.fw_resources.set_entries(rs_entries)
