import pkg_resources
import sys
import uuid

from PyQt5 import uic, QtCore, QtWidgets

from ...api import APIKeyError, CKANAPI

from .import access_token


def get_dcor_dev_api_key():
    """Return a valid DCOR-dev user API key

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
    except APIKeyError:
        # create a new user
        rstr = str(uuid.uuid4())
        pwd = str(uuid.uuid4())[:8]
        usr = "dcoraid-{}".format(rstr[:5])
        user_dict = api.post(
            "user_create",
            data={"name": usr,
                  "fullname": "Player {}".format(rstr[:5]),
                  "email": "{}@dcor-dev.mpl.mpg.de".format(usr),
                  "password": pwd,
                  })
        api_key = user_dict["apikey"]
        # Let the user know that he has a password
        QtWidgets.QMessageBox.information(
            None, "You have a password",
            "DCOR-Aid generated a password for you which you could use "
            + "to view your private datasets online. If you would like "
            + "to test private datasets, please record this: "
            + "\n\n user: {}\n password: {}".format(usr, pwd)
            + "\n\nAlternatively, you could at any point set a valid "
            + "email address in the user preferences and request a "
            + "password reset link via the web interface.")
    return api_key


class SetupWizard(QtWidgets.QWizard):
    """DCOR-Aid setup wizard"""

    def __init__(self, *args, **kwargs):
        super(SetupWizard, self).__init__(None)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.wizard", "wizard.ui")
        uic.loadUi(path_ui, self)

        self.pushButton_path_access_token.clicked.connect(
            self.on_browse_access_token)
        self.button(QtWidgets.QWizard.FinishButton).clicked.connect(
            self._finalize)

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
                msg = "Changing the server or API key requires a restart of " \
                      + "DCOR-Aid. If you choose 'No', then the original " \
                      + "server and API key are NOT changed. Do you really " \
                      + "want to quit DCOR-Aid?"
                button_reply = QtWidgets.QMessageBox.question(
                    self,
                    'DCOR-Aid restart required',
                    msg,
                    QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                    QtWidgets.QMessageBox.No)
                if button_reply == QtWidgets.QMessageBox.Yes:
                    proceed = True
                else:
                    proceed = False
            else:
                QtWidgets.QMessageBox.information(
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
                    QtCore.QEventLoop.AllEvents, 300)
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
