from collections import OrderedDict
import pkg_resources
import warnings

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
        circs = self.fw_circles.get_item_keys(selected=True)
        if not circs:
            circs = self.fw_circles.get_item_keys(selected=False)
        return circs

    @property
    def selected_collections(self):
        """Collections currently selected"""
        return self.fw_collections.get_item_keys(selected=True)

    @property
    def selected_datasets(self):
        """Datasets currently selected"""
        return self.fw_datasets.get_item_keys(selected=True)

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
        # circles
        circle_items = OrderedDict()
        for ci in self.db_extract.circles:
            warnings.warn("Have to implement global circle cache")
            circle_items[ci] = ci
        self.fw_circles.set_items(circle_items)
        # collections
        self.update_collections()
        # datasets
        self.update_datasets()

    def update_collections(self):
        collection_items = OrderedDict()
        for co in self.db_extract.collections:
            warnings.warn("Have to implement global collection cache")
            collection_items[co] = co
        self.fw_collections.set_items(collection_items)

    @QtCore.pyqtSlot()
    def update_datasets(self):
        circles = self.selected_circles
        collections = self.selected_collections
        dataset_items = OrderedDict()
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
            dataset_items[ds["name"]] = ds["title"]
        self.fw_datasets.set_items(dataset_items)
        self.update_resources()

    @QtCore.pyqtSlot()
    def update_resources(self):
        rs_items = OrderedDict()
        for dn in self.selected_datasets:
            ddict = self.db_extract.get_dataset_dict(dn)
            for rs in ddict["resources"]:
                if (self.fw_resources.checkBox.isChecked()
                    and ("mimetype" in rs
                         and rs["mimetype"] != "RT-DC")):
                    # Ignore non-RT-DC mimetypes
                    continue
                else:
                    key = rs["id"]
                    name = rs["name"]
                    if "dc:experiment:event count" in rs:
                        name += " ({} events)".format(
                            rs["dc:experiment:event count"])
                    rs_items[key] = name
        self.fw_resources.set_items(rs_items)
