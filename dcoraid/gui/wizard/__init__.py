import pkg_resources
import sys
import uuid

from dclab.rtdc_dataset.fmt_dcor import access_token
from PyQt5 import uic, QtCore, QtWidgets

from ...api import NoAPIKeyError, APINotFoundError, CKANAPI


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
    except (NoAPIKeyError, APINotFoundError):
        # create a new user
        rstr = str(uuid.uuid4())
        pwd = str(uuid.uuid4())[:8]
        usr = "dcoraid-{}".format(rstr[:5])
        api.post(
            "user_create",
            data={"name": usr,
                  "fullname": "Player {}".format(rstr[:5]),
                  "email": "{}@dcor-dev.mpl.mpg.de".format(usr),
                  "password": pwd,
                  })
        # Ask the user to create an access token via the web interface, since
        # CKAN does not support API token generation via API:
        # https://github.com/ckan/ckan/issues/7836
        api_dlg = APITokenRequestDCORDev(parent=None,
                                         user=usr,
                                         password=pwd)
        if api_dlg.exec():
            api_key = api_dlg.get_api_key()
        else:
            api_key = ""
    return api_key


class APITokenRequestDCORDev(QtWidgets.QDialog):
    def __init__(self, parent, user, password):
        super(APITokenRequestDCORDev, self).__init__(parent)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.wizard", "dcordevapi.ui")
        uic.loadUi(path_ui, self)
        self.label_user.setText(user)
        url = f"https://dcor-dev.mpl.mpg.de/user/{user}/api-tokens"
        self.label_url.setText(f"<a href='{url}'>{url}</a>")
        self.label_password.setText(password)

    def get_api_key(self):
        return self.lineEdit_token.text().strip()


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
