import random

from PyQt6 import QtCore, QtWidgets

from ...api import errors

from ..dbview import FilterChain
from ..tools import ShowWaitCursor
from ..api import get_ckan_api

from .dlg_share import ShareDialog


class FilterChainMyData(FilterChain):
    added_datasets_to_collection = QtCore.pyqtSignal(dict, list)
    share_item = QtCore.pyqtSignal(str, str)

    def __init__(self, *args, **kwargs):
        """Filter chain with user-related features"""
        super(FilterChainMyData, self).__init__(*args, **kwargs)

        # Enable the "add to collection tool box"
        self.fw_datasets.pushButton_custom.setText(
            "Add selected datasets to a collection...")
        self.fw_datasets.pushButton_custom.setVisible(True)
        self.fw_datasets.pushButton_custom.clicked.connect(
            self.on_add_datasets_to_collection)

        # Enable share buttons for collections and datasets
        self.fw_datasets.active_actions.append("share")
        self.fw_collections.active_actions.append("share")
        self.fw_datasets.share_item.connect(self.on_share_item)
        self.fw_collections.share_item.connect(self.on_share_item)

        self._dlg = None

    def choose_collaborator(self, what_for=None):
        """Let the user choose a collaborator from a dropdown list"""
        # Fetch a list of users
        with ShowWaitCursor():
            api = get_ckan_api()
            # TODO: let the user search for collaborators and add them to a
            #       memoized list.
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
                    [f"{i}: {g['display_name']}" for i, g in enumerate(grps)],
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
                # create a valid name
                name = "".join(
                    [c.lower() if c.isalnum() else "-" for c in text])
                name = name.strip("-")
                for ii in range(10):
                    try:
                        collection = api.post("group_create",
                                              {"title": text.strip(),
                                               "name": name,
                                               })
                    except errors.APIConflictError:
                        name = name + random.choice("abcdefghijkm0123456789")
                    else:
                        break
                else:
                    raise ValueError("Could not create collection")
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

    @QtCore.pyqtSlot(str, str)
    def on_share_item(self, id_type, identifier):
        """Share a collection or dataset with a collaborator"""
        self._dlg = ShareDialog(self,
                                which=id_type,
                                identifier=identifier
                                )
        self._dlg.exec()
