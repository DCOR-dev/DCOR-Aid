from PyQt6 import QtCore, QtWidgets

from ..dbview import FilterChain
from ..tools import ShowWaitCursor
from ..api import get_ckan_api


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
        self.fw_datasets.share_item.connect(self.on_share_dataset)
        self.fw_collections.share_item.connect(self.on_share_collections)

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
                    self.added_datasets_to_collection.emit(collection,
                                                           dataset_ids)

    @QtCore.pyqtSlot(str, str)
    def on_share_collections(self, id_type, identifier):
        """Share a collection with a collaborator"""
        if id_type != "collection":
            raise ValueError(
                "`on_share_collections` only works for collections")

        # TODO: Let user list and remove collaborators

        user = self.choose_collaborator(f"collection {identifier}")

        if user:
            api = get_ckan_api()
            api.post("member_create",
                     data={"id": identifier,
                           "object": user,
                           "object_type": "user",
                           "capacity": "member"})
            # TODO: add a success message in the status widget

    @QtCore.pyqtSlot(str, str)
    def on_share_dataset(self, id_type, identifier):
        """Share a collection with another user"""
        if id_type != "dataset":
            raise ValueError(
                "`on_share_dataset` only works for datasets")

        # TODO: Let user list and remove collaborators

        user = self.choose_collaborator(f"dataset {identifier}")

        if user:
            api = get_ckan_api()
            api.post("package_collaborator_create",
                     data={"id": identifier,
                           "user_id": user,
                           "capacity": "member"})
            # TODO: add a success message in the status widget
