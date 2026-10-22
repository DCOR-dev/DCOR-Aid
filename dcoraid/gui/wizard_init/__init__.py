import sys

from dclab.rtdc_dataset.fmt_dcor import access_token
from PyQt6 import QtCore, QtWidgets
from PyQt6.QtWidgets import QMessageBox, QWizard

from ...api import create_dcor_dev_user, is_valid_api_key

from .wizard_init_ui import Ui_Wizard


def get_dcor_dev_api_key():
    """Return a valid DCOR-dev user API token

    If there are no credentials available in the settings file,
    then a new user is created on DCOR-dev.
    """
    settings = QtCore.QSettings()
    api_key = settings.value("auth/api key", "")
    server = "dcor-dev.mpl.mpg.de"
    if not is_valid_api_key(server=server, api_key=api_key):
        api_key = create_dcor_dev_user(server=server)
    return api_key


class InitWizard(QtWidgets.QWizard):
    """DCOR-Aid setup wizard"""

    def __init__(self, *args, **kwargs):
        super().__init__(None)
        self.ui = Ui_Wizard()
        self.ui.setupUi(self)

        self.ui.pushButton_path_access_token.clicked.connect(
            self.on_browse_access_token)
        self.button(QWizard.WizardButton.FinishButton).clicked.connect(
            self._finalize)

    @QtCore.pyqtSlot()
    def _finalize(self):
        """Update settings"""
        user_scenario = self.get_user_scenario()
        server = self.get_server()
        api_key = self.get_api_key()
        settings = QtCore.QSettings()
        old_server = settings.value("auth/server", "")
        old_api_key = settings.value("auth/api key", "")
        old_user_scenario = settings.value("user scenario", "")
        if (user_scenario != old_user_scenario
            or old_api_key != api_key
                or old_server != server):
            if old_api_key:
                msg = "Changing the server or API token requires a restart " \
                      + "of DCOR-Aid. If you choose 'No', then the original " \
                      + "server and API token are NOT changed. Do you " \
                      + "really want to quit DCOR-Aid?"
                button_reply = QMessageBox.question(
                    self,
                    'DCOR-Aid restart required',
                    msg,
                    QMessageBox.StandardButton.Yes |
                    QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No)
                if button_reply == QMessageBox.StandardButton.Yes:
                    proceed = True
                else:
                    proceed = False
            else:
                QMessageBox.information(
                    self,
                    "DCOR-Aid restart required",
                    "Please restart DCOR-Aid to proceed."
                )
                proceed = True

            if proceed:
                settings.setValue("user scenario",
                                  self.get_user_scenario())
                settings.setValue("auth/server", server)
                settings.setValue("auth/api key", api_key)
                if user_scenario == "medical":
                    # save the server certificate
                    cert_data = access_token.get_certificate(
                        self.ui.lineEdit_access_token_path.text(),
                        self.ui.lineEdit_access_token_password.text())
                    settings.setValue("auth/certificate", cert_data)
                elif settings.contains("auth/certificate"):
                    settings.remove("auth/certificate")
                settings.sync()
                QtWidgets.QApplication.processEvents(
                    QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)
                QtWidgets.QApplication.quit()
                sys.exit(0)  # if the above does not work

    def get_api_key(self):
        user_scenario = self.get_user_scenario()
        if user_scenario == "dcor":
            return self.ui.lineEdit_apikey_dcor.text()
        elif user_scenario == "medical":
            return access_token.get_api_key(
                self.ui.lineEdit_access_token_path.text(),
                self.ui.lineEdit_access_token_password.text())
        elif user_scenario == "dcor-dev":
            return get_dcor_dev_api_key()
        elif user_scenario == "anonymous":
            return ""
        elif user_scenario == "custom":
            return self.lineEdit_apikey_custom.text()
        else:
            raise ValueError("Undefined!")

    def get_server(self):
        user_scenario = self.get_user_scenario()
        if user_scenario == "dcor":
            return "dcor.mpl.mpg.de"
        elif user_scenario == "medical":
            return access_token.get_hostname(
                self.ui.lineEdit_access_token_path.text(),
                self.ui.lineEdit_access_token_password.text())
        elif user_scenario == "dcor-dev":
            return "dcor-dev.mpl.mpg.de"
        elif user_scenario == "anonymous":
            return "dcor.mpl.mpg.de"
        elif user_scenario == "custom":
            return self.ui.lineEdit_server_custom.text()
        else:
            raise ValueError("Undefined!")

    def get_user_scenario(self):
        """Return string representing the user scenario (from first page)"""
        if self.ui.radioButton_dcor.isChecked():
            return "dcor"
        elif self.ui.radioButton_med.isChecked():
            return "medical"
        elif self.ui.radioButton_play.isChecked():
            return "dcor-dev"
        elif self.ui.radioButton_anonym.isChecked():
            return "anonymous"
        elif self.ui.radioButton_custom.isChecked():
            return "custom"
        else:
            raise ValueError("Undefined!")

    def nextId(self):
        """Determine the next page based on the current page data"""
        user_scenario = self.get_user_scenario()
        page = self.currentPage()
        page_dict = {}
        for ii in self.pageIds():
            page_dict[self.page(ii)] = ii
        if page == self.ui.page_welcome:
            if user_scenario == "dcor":
                return page_dict[self.ui.page_dcor]
            elif user_scenario == "medical":
                return page_dict[self.ui.page_med]
            elif user_scenario in ["dcor-dev", "anonymous"]:
                return -1
            elif user_scenario == "custom":
                return page_dict[self.ui.page_custom]
            else:
                raise ValueError("No Rule!")
        else:
            return -1

    @QtCore.pyqtSlot()
    def on_browse_access_token(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, 'Open access token', '', 'DCOR access token (*.dcor-access)',
            'DCOR access token (*.dcor-access)')
        if path:
            self.ui.lineEdit_access_token_path.setText(path)
