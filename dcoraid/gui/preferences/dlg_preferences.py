import logging
import pathlib
import pkg_resources
import random
import socket
import string
import traceback as tb

from PyQt5 import uic, QtCore, QtWidgets
from PyQt5.QtCore import QStandardPaths

from ...api import NoAPIKeyError, CKANAPI
from ..tools import show_wait_cursor

from ..api import get_ckan_api


class PreferencesDialog(QtWidgets.QMainWindow):
    show_server = QtCore.pyqtSignal()
    show_user = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Create a new window for preferences
        """
        super(PreferencesDialog, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.preferences", "dlg_preferences.ui")
        uic.loadUi(path_ui, self)

        self.setWindowTitle("DCOR-Aid Preferences")
        # server
        self.show_server.connect(self.on_show_server)
        self.show_user.connect(self.on_show_user)
        self.toolButton_server_update.clicked.connect(self.on_update_server)
        self.tabWidget.currentChanged.connect(self.on_tab_changed)
        self.toolButton_api_token_renew.clicked.connect(
            self.on_api_token_renew)
        self.toolButton_api_token_revoke.clicked.connect(
            self.on_api_token_revoke)
        self.toolButton_eye.clicked.connect(self.on_toggle_api_password_view)
        # uploads
        self.toolButton_uploads_apply.clicked.connect(self.on_uploads_apply)
        # downloads
        self.toolButton_downloads_browse.clicked.connect(
            self.on_downloads_browse)
        self.toolButton_downloads_apply.clicked.connect(
            self.on_downloads_apply)
        # account
        self.toolButton_user_update.clicked.connect(self.on_update_user)

        self.settings = QtCore.QSettings()
        self.on_uploads_init()
        self.on_downloads_init()

        self.logger = logging.getLogger(__name__)

        # hidden initially
        self.hide()

    def ask_change_server_or_api_key(self):
        """Ask user whether he really wants to change things

        ...because it implies a restart of DCOR-Aid.
        """
        button_reply = QtWidgets.QMessageBox.question(
            self,
            'DCOR-Aid restart required',
            "Changing the server or API token requires a restart of "
            + "DCOR-Aid. If you choose 'No', then the original server "
            + "and API key are NOT changed. Do you really want to quit "
            + "DCOR-Aid?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No)
        if button_reply == QtWidgets.QMessageBox.Yes:
            return True
        else:
            return False

    @QtCore.pyqtSlot()
    def on_toggle_api_password_view(self):
        cur_em = self.lineEdit_api_key.echoMode()
        if cur_em == QtWidgets.QLineEdit.Normal:
            new_em = QtWidgets.QLineEdit.PasswordEchoOnEdit
        else:
            new_em = QtWidgets.QLineEdit.Normal
        self.lineEdit_api_key.setEchoMode(new_em)

    @QtCore.pyqtSlot()
    def on_api_token_renew(self):
        if self.ask_change_server_or_api_key():
            api_key = self.settings.value("auth/api key")
            if len(api_key) == 36:
                # deprecated API key
                ret = QtWidgets.QMessageBox.question(
                    self,
                    "Deprecated API key",
                    "You are using an API key instead of an API token. "
                    + "API keys are deprecated and cannot be invalidated. "
                    + "DCOR-Aid can only remove it locally. A new API token "
                    + "will be created. Proceed?"
                )
                if ret != QtWidgets.QMessageBox.Yes:
                    # Abort
                    return
            # Create a new token
            api = get_ckan_api()
            # create a new token
            user_dict = api.get_user_dict()
            token_name = "DCOR-Aid {} {}".format(
                socket.gethostname(),
                ''.join(random.choice(string.ascii_letters) for _ in range(5)))

            tret = api.post("api_token_create",
                            data={"user": user_dict["id"],
                                  "name": token_name})
            self.settings.setValue("auth/api key", tret["token"])

            if len(api_key) != 36:
                # revoke the old API token
                api.post("api_token_revoke",
                         data={"token": api_key})
            self.logger.info("Exiting, because user renewed API token.")
            QtWidgets.QApplication.quit()

    @QtCore.pyqtSlot()
    def on_api_token_revoke(self):
        if self.ask_change_server_or_api_key():
            api_key = self.settings.value("auth/api key")
            if len(api_key) == 36:
                # deprecated API key
                ret = QtWidgets.QMessageBox.question(
                    self,
                    "Deprecated API key",
                    "You are using an API key instead of an API token. "
                    + "API keys are deprecated and cannot be invalidated. "
                    + "DCOR-Aid can only remove it locally. Proceed?"
                )
                if ret != QtWidgets.QMessageBox.Yes:
                    # Abort
                    return
            else:
                # API token
                api = get_ckan_api()
                api.post("api_token_revoke",
                         data={"token": self.settings.value("auth/api key")})
            self.settings.remove("auth/api key")
            self.logger.info("Exiting, because user revoked API token.")
            QtWidgets.QApplication.quit()

    def on_downloads_apply(self):
        path = self.lineEdit_downloads_path.text()
        self.settings.setValue("downloads/default path", path)

    def on_downloads_browse(self):
        default = self.settings.value("downloads/default path", ".")
        path = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            "Choose download location",
            default,
        )
        if path and pathlib.Path(path).exists():
            self.lineEdit_downloads_path.setText(path)

    def on_downloads_init(self):
        fallback = QStandardPaths.writableLocation(
                      QStandardPaths.DownloadLocation)
        dl_path = self.settings.value("downloads/default path", fallback)
        self.lineEdit_downloads_path.setText(dl_path)

    @QtCore.pyqtSlot()
    def on_uploads_apply(self):
        utwdid = int(self.checkBox_upload_write_task_id.isChecked())
        self.settings.setValue("uploads/update task with dataset id", utwdid)

    @QtCore.pyqtSlot()
    def on_uploads_init(self):
        utwdid = int(self.settings.value("uploads/update task with dataset id",
                                         "1"))
        self.checkBox_upload_write_task_id.blockSignals(True)
        self.checkBox_upload_write_task_id.setChecked(bool(utwdid))
        self.checkBox_upload_write_task_id.blockSignals(False)

    @QtCore.pyqtSlot()
    def on_show_server(self):
        self.comboBox_server.clear()
        for server in self.settings.value("server list",
                                          ["dcor.mpl.mpg.de"]):
            self.comboBox_server.addItem(server)
        self.comboBox_server.setCurrentText(
            self.settings.value("auth/server", "dcor.mpl.mpg.de"))
        self.lineEdit_api_key.setText(self.settings.value("auth/api key", ""))
        self.tabWidget.setCurrentIndex(0)  # server settings
        self.lineEdit_api_key.setEchoMode(
            QtWidgets.QLineEdit.PasswordEchoOnEdit)
        self.show()
        self.activateWindow()

    @QtCore.pyqtSlot()
    @show_wait_cursor
    def on_show_user(self):
        api = get_ckan_api()
        try:
            user_dict = api.get_user_dict()
        except (ConnectionError, NoAPIKeyError):
            self.logger.error(tb.format_exc())
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
        widget = self.tabWidget.widget(index)
        if widget is self.tab_server:
            self.on_show_server()
        elif widget is self.tab_user:
            self.on_show_user()

    @QtCore.pyqtSlot()
    @show_wait_cursor
    def on_update_user(self):
        api = get_ckan_api()
        try:
            user_dict = api.get_user_dict()
        except (ConnectionError, NoAPIKeyError):
            self.logger.error(tb.format_exc())
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
        old_server = self.settings.value("auth/server", "")
        old_api_key = self.settings.value("auth/api key", "")
        api_key = self.lineEdit_api_key.text()
        if len(api_key) == 36:
            # deprecated API Key (UUID)
            valid = "0123456789abcdef-"
        else:
            # new API tokens
            valid = "0123456789" \
                    + "abcdefghijklmnopqrstuvwxyz" \
                    + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" \
                    + "._-"
        api_key = "".join([ch for ch in api_key if ch in valid])
        server = self.comboBox_server.currentText().strip()
        # Test whether that works
        try:
            cur_api = get_ckan_api()  # maybe only name or api key changed
            api = CKANAPI(server=server, api_key=api_key,
                          ssl_verify=cur_api.verify)
            api.get_user_dict()  # raises an exception if credentials are wrong
        except BaseException:
            self.logger.error(tb.format_exc())
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Critical)
            msg.setText("Bad server / API key combination!")
            msg.setWindowTitle("Error")
            msg.setDetailedText(tb.format_exc())
            msg.exec_()
        else:
            if old_server != server or old_api_key != api_key:
                if self.ask_change_server_or_api_key():
                    self.settings.setValue("auth/api key", api_key)
                    self.settings.setValue("auth/server", server)
                    self.logger.info("Exiting, because of new credentials.")
                    QtWidgets.QApplication.quit()
