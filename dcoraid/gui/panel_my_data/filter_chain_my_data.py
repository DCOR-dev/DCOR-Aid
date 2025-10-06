from PyQt6 import QtCore, QtWidgets

from ..dbview import FilterChain
from ..tools import ShowWaitCursor
from ..api import get_ckan_api

from .dlg_share import ShareDialog


class FilterChainMyData(FilterChain):
    removed_datasets_from_collection = QtCore.pyqtSignal(dict, list)
    added_datasets_to_collection = QtCore.pyqtSignal(dict, list)
    share_item = QtCore.pyqtSignal(str, str)

    def __init__(self, *args, **kwargs):
        """Filter chain with user-related features"""
        super(FilterChainMyData, self).__init__(*args, **kwargs)

        # Enable "create collection" toolbutton
        self.fw_collections.pushButton_custom.setText(
            "Create new collection...")
        self.fw_collections.pushButton_custom.setVisible(True)
        self.fw_collections.pushButton_custom.clicked.connect(
            self.on_create_collection)

        # Enable the "add to collection" toolbutton
        self.fw_datasets.pushButton_custom.setText(
            "Add selected datasets to a collection...")
        self.fw_datasets.pushButton_custom.setVisible(True)
        self.fw_datasets.pushButton_custom.clicked.connect(
            self.on_add_datasets_to_collection)

        # Enable share buttons for collections and datasets
        self.fw_datasets.active_actions.append("share")
        self.fw_datasets.active_actions.append("remove-from-collection")
        self.fw_collections.active_actions.append("share")
        self.fw_datasets.share_item.connect(self.on_share_item)
        self.fw_collections.share_item.connect(self.on_share_item)
        self.fw_datasets.remove_item.connect(self.on_remove_item)

        self._dlg = None

    def choose_collaborator(self, what_for=None):
        """Let the user choose a collaborator from a dropdown list"""
        # Fetch a list of users
        with ShowWaitCursor():
            api = get_ckan_api()
            users = api.get("user_autocomplete", q="", limit=100)

        for_what_for = f" for {what_for}"
        label = f"Please choose a collaborator{for_what_for}."
        ignored_users = ["default", "adminpaul", api.user_name]
        user_choices = sorted([
            f"{u['fullname'] or u['name'].capitalize()} ({u['name']})"
            for u in users if u["name"] not in ignored_users])
        item, ok = QtWidgets.QInputDialog.getItem(
            self,
            "Choose a user",
            label,
            user_choices,
            0,  # current index
            False,  # editable
        )
        if ok:
            username = item.rsplit("(", 1)[1].strip(")")
            return username
        else:
            return None

    @QtCore.pyqtSlot()
    def on_add_datasets_to_collection(self):
        """Add all datasets currently selected to a collection

        Displays a dialog where the user can choose a collection
        they have write-access to.
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
            collection = None
            if grps:
                item, ok = QtWidgets.QInputDialog.getItem(
                    self,
                    "Select a collection",
                    f"Please choose a collection for "
                    f"{len(dataset_ids)} datasets.",
                    [f"{i}: {g['display_name']} ({g['name']})"
                     for i, g in enumerate(grps)],
                    0,  # current index
                    False,  # editable
                    )
                if ok:
                    index = int(item.split(":")[0])
                    collection = grps[index]
            else:
                text, ok = QtWidgets.QInputDialog.getText(
                    self,
                    "Create a collection",
                    "Type the name of a collection to create",
                )
                if ok:
                    # create a valid name
                    name = "".join(
                        [c.lower() if c.isalnum() else "-" for c in text])
                    name = name.strip("-")
                    # create the collection
                    collection = api.require_collection(name=name,
                                                        title=text,
                                                        exist_ok=False)
            if ok:
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
                    self.added_datasets_to_collection.emit(collection,
                                                           dataset_ids)

    @QtCore.pyqtSlot()
    def on_create_collection(self):
        text, ok = QtWidgets.QInputDialog.getText(
            self,
            "Create a collection",
            "Type the name of a collection to create",
        )
        if ok:
            api = get_ckan_api()
            # create a valid name
            name = "".join(
                [c.lower() if c.isalnum() else "-" for c in text])
            name = name.strip("-")
            collection = api.require_collection(name=name,
                                                title=text,
                                                exist_ok=False)
            QtWidgets.QMessageBox.information(
                self,
                "Collection created",
                f"You may now add datasets to "
                f"collection '{collection['name']}'.",
            )

    @QtCore.pyqtSlot(str, str)
    def on_remove_item(self, id_type, identifier):
        """Remove a dataset from a collection"""
        if id_type != "dataset":
            raise NotImplementedError("`id_type` must be 'dataset'")
        # Fetch the currently selected collection(s).
        selected = self.fw_collections.get_entry_identifiers(selected=True)
        if selected:
            api = get_ckan_api()
            for colid in selected:
                api.post("member_delete",
                         data={"id": colid,
                               "object": identifier,
                               "object_type": "package"})
                self.removed_datasets_from_collection.emit({"id": colid},
                                                           [identifier])
        else:
            QtWidgets.QMessageBox.information(
                self,
                "Must first select a collection",
                "If you would like to remove a dataset from a collection, "
                "then you must first select the collection in the top right "
                "pane."
            )

    @QtCore.pyqtSlot(str, str)
    def on_share_item(self, id_type, identifier):
        """Share a collection or dataset with a collaborator"""
        self._dlg = ShareDialog(self,
                                which=id_type,
                                identifier=identifier
                                )
        self._dlg.exec()
