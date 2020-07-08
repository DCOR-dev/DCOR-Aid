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
        path_ui = pkg_resources.resource_filename("dcor_manager.gui.dbview",
                                                  "filter_chain.ui")
        uic.loadUi(path_ui, self)

        #: Currently visible datasets
        self.datasets = []

        #: Signals and slots
        self.fw_datasets.selection_changed.connect(self.show_resources)

    @property
    def selected_circles(self):
        """Circles currently selected"""
        return self.fw_circles.get_item_keys(selected=True)

    @property
    def selected_collections(self):
        """Collections currently selected"""
        return self.fw_collections.get_item_keys(selected=True)

    def set_db_extract(self, db_extract):
        """Set the database model

        Parameters
        ----------
        db_model: dcor_manager.dbmodel.core.DBExtract
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
        collection_items = OrderedDict()
        for co in self.db_extract.collections:
            warnings.warn("Have to implement global collection cache")
            collection_items[co] = co
        self.fw_collections.set_items(collection_items)
        # datasets
        dataset_items = OrderedDict()
        for ds in self.db_extract.datasets:
            dataset_items[ds["name"]] = ds["title"]
        self.fw_datasets.set_items(dataset_items)

    @QtCore.pyqtSlot(list)
    def show_resources(self, dataset_names):
        rs_items = OrderedDict()
        for dn in dataset_names:
            ddict = self.db_extract.get_dataset_dict(dn)
            for rs in ddict["resources"]:
                key = rs["id"]
                name = "{} ({} events)".format(
                    rs["name"], rs["dc:experiment:event count"])
                rs_items[key] = name
        self.fw_resources.set_items(rs_items)
