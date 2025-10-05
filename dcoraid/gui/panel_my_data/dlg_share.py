from functools import partial
from importlib import resources
import json
import logging
import pathlib
from typing import Literal

from PyQt6 import uic, QtCore, QtGui, QtWidgets

from ...api import errors
from ..api import get_ckan_api


class ShareDialog(QtWidgets.QDialog):
    def __init__(self,
                 parent,
                 which: Literal["collection", "dataset"],
                 identifier: str,
                 *args, **kwargs):
        """Create a new window for setting up a file upload
        """
        super(ShareDialog, self).__init__(parent, *args, **kwargs)
        self.logger = logging.getLogger(__name__)
        ref_ui = resources.files(
            "dcoraid.gui.panel_my_data") / "dlg_share.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        self.api = get_ckan_api()

        self.which = which
        self.identifier = identifier

        self.setWindowTitle(f"Share {self.which}")
        self.label.setText(f"Share {self.which} '{self.identifier}'")

        self.toolButton_share.clicked.connect(self.on_user_add)
        self.comboBox_users.currentIndexChanged.connect(self.on_combobox_users)
        self.comboBox_users.currentTextChanged.connect(self.on_search)

        self.toolButton_share.setEnabled(False)

        self.tableWidget_users.user_remove.connect(self.on_user_remove)

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
            self.tableWidget_users.users += users
            self.tableWidget_users.update_user_table()

    @QtCore.pyqtSlot()
    def on_user_add(self):
        user = self.comboBox_users.currentData()
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
        self.tableWidget_users.on_user_add(user)

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
        index = self.comboBox_users.currentIndex()
        self.toolButton_share.setEnabled(index >= 0)

    @QtCore.pyqtSlot()
    def on_search(self):
        """Search for users given the current string and set combobox items"""
        search_text = self.comboBox_users.currentText()
        if len(search_text) <= 3:
            self.toolButton_share.setEnabled(False)
            return

        users = self.api.get("user_autocomplete",
                             q=search_text,
                             limit=100)
        # Populate the combobox
        if users:
            cur_users = []
            for ii in range(self.comboBox_users.count()):
                cur_users.append(self.comboBox_users.itemData(ii)["id"])
            if set(cur_users) != set([u["id"] for u in users]):
                # Update users listed in combobox
                self.comboBox_users.clear()
                for ii, user in enumerate(users):
                    user = self.tableWidget_users.get_full_user_dict(user)
                    self.comboBox_users.blockSignals(True)
                    self.comboBox_users.addItem(user["displayname"], user)
                    self.comboBox_users.blockSignals(False)


class UserTableWidget(QtWidgets.QTableWidget):
    user_remove = QtCore.pyqtSignal(dict)

    def __init__(self, *args, **kwargs):
        super(UserTableWidget, self).__init__(*args, **kwargs)

        self.api = get_ckan_api()
        self.user_cache_location = pathlib.Path(
            QtCore.QStandardPaths.writableLocation(
                    QtCore.QStandardPaths.StandardLocation.CacheLocation)
            ) / self.api.hostname / "user_dicts"
        self.user_cache_location.mkdir(parents=True, exist_ok=True)

        # Set column count and horizontal header sizes
        self.setColumnCount(2)
        header = self.horizontalHeader()
        header.setSectionResizeMode(
            0, QtWidgets.QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(
            1, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)

        # Add current users
        self.users = []
        self.update_user_table()

    def get_full_user_dict(self, user_dict):
        """Cache and/or return a user dictionary"""
        uid = user_dict.get("id", user_dict.get("user_id"))
        user_cache = self.user_cache_location / f"{uid}.json"
        if user_cache.exists():
            cur_dict = json.loads(user_cache.read_text())
        else:
            cur_dict = {"id": uid}
        if "name" in user_dict:
            cur_dict["name"] = user_dict["name"]
        if "fullname" in user_dict:
            cur_dict["fullname"] = user_dict["fullname"]
        if cur_dict.get("fullname") and cur_dict.get("name"):
            dname = f"{cur_dict['fullname']} ({cur_dict['name']})"
        else:
            dname = cur_dict.get('name', cur_dict.get("id", "unknown")[:7])
        cur_dict["displayname"] = dname
        user_cache.write_text(json.dumps(cur_dict))
        return cur_dict

    def update_user_table(self):
        """Add user to table widget"""
        self.setRowCount(len(self.users))
        for row, user in enumerate(self.users):
            user = self.get_full_user_dict(user)
            self.set_label_item(row, 0, user["displayname"])
            if user["id"] != self.api.user_id:
                self.set_actions_item(row, 1, user)
            QtWidgets.QApplication.processEvents(
                QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)

    @QtCore.pyqtSlot(dict)
    def on_user_add(self, user_dict):
        self.users.append(self.get_full_user_dict(user_dict))
        self.update_user_table()

    @QtCore.pyqtSlot(str)
    def on_user_remove(self, user_dict):
        self.user_remove.emit(user_dict)
        ids = [self.get_full_user_dict(u)["id"] for u in self.users]
        idx = ids.index(user_dict["id"])
        self.users.pop(idx)
        self.update_user_table()

    def set_label_item(self, row, col, label):
        """Get/Create a Qlabel at the specified position

        User has to make sure that row and column count are set
        """
        label = f"{label}"
        item = self.item(row, col)
        if item is None:
            item = QtWidgets.QTableWidgetItem(label)
            item.setToolTip(label)
            item.setFlags(QtCore.Qt.ItemFlag.ItemIsEnabled)
            self.setItem(row, col, item)
        else:
            if item.text() != label:
                item.setText(label)
                item.setToolTip(label)

    def set_actions_item(self, row, col, user_dict):
        """Set/Create a TableCellActions widget in the table

        Refreshes the widget and also connects signals.
        """
        widact = self.cellWidget(row, col)
        if widact is None:
            widact = QtWidgets.QWidget(self)
            horz_layout = QtWidgets.QHBoxLayout(widact)
            horz_layout.setContentsMargins(2, 0, 2, 0)

            actions = [
                {"icon": "trash",
                 "tooltip": f"Remove user {user_dict['displayname']}",
                 "function": partial(self.on_user_remove, user_dict)
                 }
                ]
            for action in actions:
                tbact = QtWidgets.QToolButton(widact)
                icon = QtGui.QIcon.fromTheme(action["icon"])
                tbact.setIcon(icon)
                tbact.setToolTip(action["tooltip"])
                tbact.clicked.connect(action["function"])
                horz_layout.addWidget(tbact)
            self.setCellWidget(row, col, widact)
