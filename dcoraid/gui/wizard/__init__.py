from importlib import resources
import sys
import uuid

from dclab.rtdc_dataset.fmt_dcor import access_token
from PyQt6 import uic, QtCore, QtWidgets
from PyQt6.QtWidgets import QMessageBox, QWizard

from ...api import (
    APIAuthorizationError, APINotFoundError, NoAPIKeyError, CKANAPI)


def get_dcor_dev_api_key():
    """Return a valid DCOR-dev user API token

    If there are no credentials available in the settings file,
    then a new user is created on DCOR-dev.
    """
    settings = QtCore.QSettings()
    api_key = settings.value("auth/api key", "")
    api = CKANAPI(server="https://dcor-dev.mpl.mpg.de",
                  api_key=api_key,
                  ssl_verify=True)
    try:
        api.get_user_dict()
    except (APIAuthorizationError, APINotFoundError, NoAPIKeyError):
        # create a new user
        rstr = str(uuid.uuid4())
        pwd = str(uuid.uuid4())[:8]
        usr = f"dcoraid-{rstr[:5]}"
        user_dict = api.post(
            "user_create",
            data={"name": usr,
                  "fullname": f"Player {rstr[:5]}",
                  "email": f"{usr}@dcor-dev.mpl.mpg.de",
                  "password": pwd,
                  "with_apitoken": True,
                  })
        api_key = user_dict["token"]
    return api_key


class SetupWizard(QtWidgets.QWizard):
    """DCOR-Aid setup wizard"""

    def __init__(self, *args, **kwargs):
        super(SetupWizard, self).__init__(None)
        ref_ui = resources.files("dcoraid.gui.wizard") / "wizard.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        self.pushButton_path_access_token.clicked.connect(
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
                        self.lineEdit_access_token_path.text(),
                        self.lineEdit_access_token_password.text())
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
            return self.lineEdit_apikey_dcor.text()
        elif user_scenario == "medical":
            return access_token.get_api_key(
                self.lineEdit_access_token_path.text(),
                self.lineEdit_access_token_password.text())
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
                self.lineEdit_access_token_path.text(),
                self.lineEdit_access_token_password.text())
        elif user_scenario == "dcor-dev":
            return "dcor-dev.mpl.mpg.de"
        elif user_scenario == "anonymous":
            return "dcor.mpl.mpg.de"
        elif user_scenario == "custom":
            return self.lineEdit_server_custom.text()
        else:
            raise ValueError("Undefined!")

    def get_user_scenario(self):
        """Return string representing the user scenario (from first page)"""
        if self.radioButton_dcor.isChecked():
            return "dcor"
        elif self.radioButton_med.isChecked():
            return "medical"
        elif self.radioButton_play.isChecked():
            return "dcor-dev"
        elif self.radioButton_anonym.isChecked():
            return "anonymous"
        elif self.radioButton_custom.isChecked():
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
        if page == self.page_welcome:
            if user_scenario == "dcor":
                return page_dict[self.page_dcor]
            elif user_scenario == "medical":
                return page_dict[self.page_med]
            elif user_scenario == "dcor-dev":
                return -1
            elif user_scenario == "anonymous":
                return -1
            elif user_scenario == "custom":
                return page_dict[self.page_custom]
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
            self.lineEdit_access_token_path.setText(path)
