import pkg_resources
import uuid

from PyQt5 import uic, QtWidgets

from ...settings import SettingsFile
from ...api import APIKeyError, CKANAPI


def get_dcor_dev_api_key():
    """Return a valid DCOR-dev user API key

    If there are no credentials available in the settings file,
    then a new user is created on DCOR-dev.
    """
    settings = SettingsFile()
    api_key = settings.get_string("api key")
    api = CKANAPI(server="https://dcor-dev.mpl.mpg.de",
                  api_key=api_key)
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
            + "\n\n alternatively, you could at any point set a valid "
            + "email address in the user preferences.")
    return api_key


class SetupWizard(QtWidgets.QWizard):
    """DCOR-Aid setup wizard"""

    def __init__(self, *args, **kwargs):
        super(SetupWizard, self).__init__(None)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.wizard", "wizard.ui")
        uic.loadUi(path_ui, self)

        self.button(QtWidgets.QWizard.FinishButton).clicked.connect(
            self._finalize)

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

    def get_api_key(self):
        user_scenario = self.get_user_scenario()
        if user_scenario == "dcor":
            return self.lineEdit_apikey_dcor.text()
        elif user_scenario == "medical":
            return self.lineEdit_apikey_med.text()
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
            raise NotImplementedError("TODO")
        elif user_scenario == "dcor-dev":
            return "dcor-dev.mpl.mpg.de"
        elif user_scenario == "anonymous":
            return "dcor.mpl.mpg.de"
        elif user_scenario == "custom":
            return self.lineEdit_server_custom.text()
        else:
            raise ValueError("Undefined!")

    def nextId(self):
        """Determine the next page based on the current page data"""
        page = self.currentPage()
        page_dict = {}
        for ii in self.pageIds():
            page_dict[self.page(ii)] = ii
        if page == self.page_welcome:
            if self.radioButton_dcor.isChecked():
                return page_dict[self.page_dcor]
            elif self.radioButton_med.isChecked():
                return page_dict[self.page_med]
            elif self.radioButton_play.isChecked():
                return -1
            elif self.radioButton_custom.isChecked():
                return page_dict[self.page_custom]
            else:
                raise ValueError("No Rule!")
        else:
            return -1

    def _finalize(self):
        """Update settings"""
        settings = SettingsFile()
        if settings.get_string("api key"):
            buttonReply = QtWidgets.QMessageBox.question(
                self,
                'DCOR-Aid restart required',
                "Changing the server or API key requires a restart of "
                + "DCOR-Aid. If you choose 'No', then the original server "
                + "and API key are NOT changed. Do you really want to quit "
                + "DCOR-Aid?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.No)
            if buttonReply == QtWidgets.QMessageBox.Yes:
                proceed = True
            else:
                proceed = False
        else:
            proceed = True

        if proceed:
            settings.set_string("user scenario", self.get_user_scenario())
            settings.set_string("server", self.get_server())
            settings.set_string("api key", self.get_api_key())
