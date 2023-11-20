import copy

from importlib import resources

from PyQt5 import QtCore, QtWidgets, uic

from ..tools import ShowWaitCursor
from ..api import get_ckan_api


class FilterChain(QtWidgets.QWidget):
    download_resource = QtCore.pyqtSignal(str, bool)

    def __init__(self, *args, **kwargs):
        """Filter chain widget with multiple filter views
        """
        super(FilterChain, self).__init__(*args, **kwargs)
        QtWidgets.QMainWindow.__init__(self)
        ref_ui = resources.files("dcoraid.gui.dbview") / "filter_chain.ui"
        with resources.as_file(ref_ui) as path_ui:
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
        # request resource downloads
        self.fw_circles.download_resource.connect(self.download_resource)
        self.fw_collections.download_resource.connect(self.download_resource)
        self.fw_datasets.download_resource.connect(self.download_resource)
        self.fw_resources.download_resource.connect(self.download_resource)

    @property
    def selected_circles(self):
        """Circles currently selected"""
        circs = self.fw_circles.get_entry_identifiers(selected=True,
                                                      which="name")
        if not circs:
            circs = self.fw_circles.get_entry_identifiers(selected=False,
                                                          which="name")
        return circs

    @property
    def selected_collections(self):
        """Collections currently selected"""
        return self.fw_collections.get_entry_identifiers(selected=True,
                                                         which="name")

    @property
    def selected_datasets(self):
        """Datasets currently selected"""
        return self.fw_datasets.get_entry_identifiers(selected=True,
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
        self.fw_circles.set_entries(
            copy.deepcopy(self.db_extract.circles))
        # collections
        self.update_collections()
        # datasets
        self.update_datasets()

    @QtCore.pyqtSlot()
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
            for rs in ddict.get("resources", []):
                if (self.fw_resources.checkBox.isChecked()
                    and ("mimetype" in rs
                         and rs["mimetype"] != "RT-DC")):
                    # Ignore non-RT-DC mimetypes
                    continue
                else:
                    rs_entries.append(rs)
        self.fw_resources.set_entries(rs_entries)


class FilterChainUser(FilterChain):

    def __init__(self, *args, **kwargs):
        """Filter chain with user-related features"""
        super(FilterChainUser, self).__init__(*args, **kwargs)

        # Enable the "add to collection tool box"
        self.fw_datasets.toolButton_custom.setText(
            "Add selected datasets to a collection...")
        self.fw_datasets.toolButton_custom.setVisible(True)
        self.fw_datasets.toolButton_custom.clicked.connect(
            self.on_add_datasets_to_collection)

    @QtCore.pyqtSlot()
    def on_add_datasets_to_collection(self):
        """Add all datasets currently selected to a collection

        Displays a dialog where the user can choose a collection
        she has write-access to.
        """
        # get current selection
        dataset_ids = self.fw_datasets.get_entry_identifiers(selected=True)
        if not dataset_ids:
            # no datasets selected
            QtWidgets.QMessageBox.information(
                self, "No datasets selected",
                "Please select at least one dataset.")
        else:
            # get list of writable collections
            with ShowWaitCursor():
                api = get_ckan_api()
                grps = api.get("group_list_authz")
            grps = sorted(grps, key=lambda x: x["display_name"])
            item, ok = QtWidgets.QInputDialog.getItem(
                self,
                "Select a collection",
                f"Please choose a collection for {len(dataset_ids)} datasets.",
                [f"{ii}: {g['display_name']}" for ii, g in enumerate(grps)],
                0,  # current index
                False,  # editable
                )
            if ok:
                index = int(item.split(":")[0])
                collection = grps[index]
                with ShowWaitCursor():
                    # add all datasets to that collection
                    for did in dataset_ids:
                        api.post(
                            "member_create",
                            data={"id": collection["id"],
                                  "object": did,
                                  "object_type": "package",
                                  # "capacity" should not be necessary
                                  # https://github.com/ckan/ckan/issues/6543
                                  "capacity": "member"})
