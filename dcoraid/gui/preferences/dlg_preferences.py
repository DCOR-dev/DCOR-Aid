import pkg_resources
import traceback as tb

from PyQt5 import uic, QtCore, QtWidgets

from ...api import CKANAPI, APIKeyError
from ...settings import SettingsFile
from ..tools import show_wait_cursor


class PreferencesDialog(QtWidgets.QMainWindow):
    show_server = QtCore.pyqtSignal()
    show_user = QtCore.pyqtSignal()
    server_changed = QtCore.pyqtSignal()

    def __init__(self, parent=None):
        """Create a new window for preferences
        """
        super(PreferencesDialog, self).__init__(parent)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.preferences", "dlg_preferences.ui")
        uic.loadUi(path_ui, self)

        self.setWindowTitle("DCOR-Aid Preferences")
        self.show_server.connect(self.on_show_server)
        self.show_user.connect(self.on_show_user)
        self.tabWidget.currentChanged.connect(self.on_tab_changed)
        self.toolButton_user_update.clicked.connect(self.on_update_user)
        self.toolButton_server_update.clicked.connect(self.on_update_server)
        self.toolButton_api_key_purge.clicked.connect(self.on_api_key_purge)
        self.server_changed.connect(self.show_server)

        self.settings = SettingsFile()
        if self.settings.get_string("api key"):
            # hidden initially if user already entered an API key
            self.hide()

    @QtCore.pyqtSlot()
    def on_api_key_purge(self):
        self.settings.delete_key("api key")
        self.server_changed.emit()

    @QtCore.pyqtSlot()
    def on_show_server(self):
        self.comboBox_server.clear()
        for server in self.settings.get_string_list("server list"):
            self.comboBox_server.addItem(server)
        self.comboBox_server.setCurrentText(self.settings.get_string("server"))
        self.lineEdit_api_key.setText(self.settings.get_string("api key"))
        self.tabWidget.setCurrentIndex(0)  # server settings
        self.show()
        self.activateWindow()

    @QtCore.pyqtSlot()
    @show_wait_cursor
    def on_show_user(self):
        api = CKANAPI(server=self.settings.get_string("server"),
                      api_key=self.settings.get_string("api key"))
        try:
            user_dict = api.get_user_dict()
        except (ConnectionError, APIKeyError):
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Warning)
            msg.setText("No connection or wrong server or invalid API key!")
            msg.setWindowTitle("Warning")
            msg.setDetailedText(tb.format_exc())
            msg.exec_()
            self.on_show_server()
        else:
            self.lineEdit_user_id.setText(user_dict["name"])
            self.lineEdit_user_name.setText(user_dict["fullname"])
            self.lineEdit_user_email.setText(user_dict["email"])
            self.plainTextEdit_user_about.setPlainText(user_dict["about"])
            self.show()
            self.activateWindow()

    @QtCore.pyqtSlot(int)
    def on_tab_changed(self, index):
        if index == 0:
            self.on_show_server()
        elif index == 1:
            self.on_show_user()

    @QtCore.pyqtSlot()
    @show_wait_cursor
    def on_update_user(self):
        api = CKANAPI(server=self.settings.get_string("server"),
                      api_key=self.settings.get_string("api key"))
        try:
            user_dict = api.get_user_dict()
        except (ConnectionError, APIKeyError):
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Warning)
            msg.setText("No connection or wrong server or invalid API key!")
            msg.setWindowTitle("Warning")
            msg.setDetailedText(tb.format_exc())
            msg.exec_()
            self.on_show_server()
        update_dict = {}
        update_dict["id"] = user_dict["id"]
        update_dict["fullname"] = self.lineEdit_user_name.text()
        update_dict["email"] = self.lineEdit_user_email.text()
        update_dict["about"] = self.plainTextEdit_user_about.toPlainText()
        api.post("user_update", data=update_dict)

    @QtCore.pyqtSlot()
    @show_wait_cursor
    def on_update_server(self):
        api_key = self.lineEdit_api_key.text()
        api_key = "".join([ch for ch in api_key if ch in "0123456789abcdef-"])
        server = self.comboBox_server.currentText().strip()
        # Test whether that works
        try:
            api = CKANAPI(server=server, api_key=api_key)
            api.get_user_dict()
        except BaseException:
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Critical)
            msg.setText("Bad server / API key combination!")
            msg.setWindowTitle("Error")
            msg.setDetailedText(tb.format_exc())
            msg.exec_()
        else:
            self.settings.set_string("api key", api_key)
            self.settings.set_string("server", server)
            self.server_changed.emit()
