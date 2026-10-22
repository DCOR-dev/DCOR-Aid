from __future__ import annotations

import logging
from typing import Literal

from PyQt6 import QtCore, QtWidgets

from ...api import errors
from ..api import get_ckan_api
from .dlg_share_ui import Ui_Dialog


class ShareDialog(QtWidgets.QDialog):
    def __init__(self,
                 parent,
                 which: Literal["collection", "dataset"],
                 identifier: str,
                 *args, **kwargs):
        """Create a new window for setting up a file upload
        """
        super().__init__(parent, *args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)

        self.api = get_ckan_api()

        self.which = which
        self.identifier = identifier

        self.setWindowTitle(f"Share {self.which}")
        self.ui.label.setText(f"Share {self.which} '{self.identifier}'")

        self.ui.toolButton_share.clicked.connect(self.on_user_add)
        self.ui.comboBox_users.currentIndexChanged.connect(
            self.on_combobox_users)
        self.ui.comboBox_users.currentTextChanged.connect(self.on_search)

        self.ui.toolButton_share.setEnabled(False)

        self.ui.tableWidget_users.user_remove.connect(self.on_user_remove)

        # Determine users that currently have access
        try:
            if self.which == "collection":
                group = self.api.get("group_show",
                                     id=self.identifier,
                                     include_users=True
                                     )
                users = group["users"]
            else:
                users = self.api.get("package_collaborator_list",
                                     id=self.identifier)
        except errors.APIAuthorizationError:
            QtWidgets.QMessageBox.critical(
                self,
                "Insufficient permissions",
                f"You do not have sufficient authorization to modify "
                f"the {self.which} '{self.identifier}'.")
            self.close()
        else:
            self.ui.tableWidget_users.users += users
            self.ui.tableWidget_users.update_user_table()

    @QtCore.pyqtSlot()
    def on_user_add(self):
        user = self.ui.comboBox_users.currentData()
        if self.which == "dataset":
            self.api.post("package_collaborator_create",
                          data={"id": self.identifier,
                                "user_id": user["id"],
                                "capacity": "member"})
        else:
            self.api.post("group_member_create",
                          data={"id": self.identifier,
                                "username": user["id"],
                                "role": "member"})
        self.ui.tableWidget_users.on_user_add(user)

    @QtCore.pyqtSlot(dict)
    def on_user_remove(self, user_dict):
        """Remove a user from the given collection or dataset"""
        if self.which == "dataset":
            self.api.post("package_collaborator_delete",
                          data={"id": self.identifier,
                                "user_id": user_dict["id"]})
        else:
            self.api.post("group_member_delete",
                          data={"id": self.identifier,
                                "username": user_dict["id"]})

    @QtCore.pyqtSlot()
    def on_combobox_users(self):
        index = self.ui.comboBox_users.currentIndex()
        self.ui.toolButton_share.setEnabled(index >= 0)

    @QtCore.pyqtSlot()
    def on_search(self):
        """Search for users given the current string and set combobox items"""
        search_text = self.ui.comboBox_users.currentText()
        if len(search_text) <= 3:
            self.ui.toolButton_share.setEnabled(False)
            return

        users = self.api.get("user_autocomplete",
                             q=search_text,
                             limit=100)
        # Populate the combobox
        if users:
            cur_users = []
            for ii in range(self.ui.comboBox_users.count()):
                cur_users.append(self.ui.comboBox_users.itemData(ii)["id"])
            if set(cur_users) != {u["id"] for u in users}:
                # Update users listed in combobox
                self.ui.comboBox_users.clear()
                for ii, user in enumerate(users):
                    user = self.ui.tableWidget_users.get_full_user_dict(user)
                    self.ui.comboBox_users.blockSignals(True)
                    self.ui.comboBox_users.addItem(user["displayname"], user)
                    self.ui.comboBox_users.blockSignals(False)
