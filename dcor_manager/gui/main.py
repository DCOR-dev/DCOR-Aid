import os
import pkg_resources
import signal
import sys
import traceback as tb

import appdirs

from PyQt5 import uic, QtCore, QtGui, QtWidgets

from ..api import APIKeyError, CKANAPI
from ..dbmodel import APIModel
from .. import settings
from .._version import version as __version__
# from .widgets.wait_cursor import ShowWaitCursor, show_wait_cursor


# set Qt icon theme search path
QtGui.QIcon.setThemeSearchPaths([
    os.path.join(pkg_resources.resource_filename("dcor_manager", "img"),
                 "icon-theme")])
QtGui.QIcon.setThemeName(".")


class DCORManager(QtWidgets.QMainWindow):
    plots_changed = QtCore.pyqtSignal()

    def __init__(self):
        """Initialize DCOR_Manager

        If you pass the "--version" command line argument, the
        application will print the version after initialization
        and exit.
        """
        QtWidgets.QMainWindow.__init__(self)
        path_ui = pkg_resources.resource_filename(
            "dcor_manager.gui", "main.ui")
        uic.loadUi(path_ui, self)
        #: DCOR-Manager settings
        self.settings = settings.SettingsFile()
        # GUI
        self.setWindowTitle("DCOR-Manager {}".format(__version__))
        # Disable native menubar (e.g. on Mac)
        self.menubar.setNativeMenuBar(False)
        # File menu
        self.actionSet_API_key.triggered.connect(self.on_action_api_key)
        # Help menu
        self.actionSoftware.triggered.connect(self.on_action_software)
        self.actionAbout.triggered.connect(self.on_action_about)
        # if "--version" was specified, print the version and exit
        if "--version" in sys.argv:
            print(__version__)
            QtWidgets.QApplication.processEvents()
            sys.exit(0)
        # Display login status
        self.toolButton_user = QtWidgets.QToolButton()
        self.toolButton_user.setToolButtonStyle(
            QtCore.Qt.ToolButtonTextBesideIcon)
        self.toolButton_user.setAutoRaise(True)
        self.tabWidget.setCornerWidget(self.toolButton_user)
        self.toolButton_user.setText("Not logged in!")
        self.toolButton_user.clicked.connect(self.on_action_api_key)
        self.refresh_login_status()
        # Update private data tab
        self.refresh_private_data()
        # If a new dataset has been uploaded, refresh private data
        self.panel_upload.upload_finished.connect(self.refresh_private_data)

    def on_action_about(self):
        about_text = "GUI for managing data on DCOR."
        QtWidgets.QMessageBox.about(self,
                                    "DCOR-Manager {}".format(__version__),
                                    about_text)

    def on_action_api_key(self):
        """Open a dialog window for entering an API key"""
        api_key = self.settings.get_string("api key")
        text, okPressed = QtWidgets.QInputDialog.getText(
            self, "API key", "Please enter your API key:",
            QtWidgets.QLineEdit.Normal, api_key)
        if okPressed:
            api_key = "".join([ch for ch in text if ch in "0123456789abcdef-"])
            self.settings.set_string("api key", api_key)
        self.refresh_login_status()

    def on_action_software(self):
        libs = [appdirs,
                ]
        sw_text = "DCOR-Manager {}\n\n".format(__version__)
        sw_text += "Python {}\n\n".format(sys.version)
        sw_text += "Modules:\n"
        for lib in libs:
            sw_text += "- {} {}\n".format(lib.__name__, lib.__version__)
        sw_text += "- PyQt5 {}\n".format(QtCore.QT_VERSION_STR)
        sw_text += "\n Breeze icon theme by the KDE Community (LGPL)."
        if hasattr(sys, 'frozen'):
            sw_text += "\nThis executable has been created using PyInstaller."
        QtWidgets.QMessageBox.information(self,
                                          "Software",
                                          sw_text)

    @QtCore.pyqtSlot()
    def refresh_login_status(self):
        api_key = self.settings.get_string("api key")
        server = self.settings.get_string("server")
        api = CKANAPI(server=server, api_key=api_key)
        try:
            api.get("site_read")
        except BaseException:
            text = "No connection to '{}'.".format(server)
            tip = tb.format_exc()
        else:
            if not api_key:
                text = "No API key."
                tip = "Click here to enter your API key."
            else:
                try:
                    user_data = api.get_user_dict()
                except APIKeyError:
                    text = "API key not valid for '{}'.".format(server)
                    tip = "Click here to update your API key."
                else:
                    fullname = user_data["fullname"]
                    name = user_data["name"]
                    if not fullname:
                        fullname = name
                    text = "logged in as: {}".format(fullname)
                    tip = "user '{}'".format(name)
        self.toolButton_user.setText(text)
        self.toolButton_user.setToolTip(tip)

    def refresh_private_data(self):
        # TODO:
        # - what happens if the user changes the server? Ask to restart?
        try:
            am = APIModel(url=self.settings.get_string("server"),
                          api_key=self.settings.get_string("api key"))
            db_extract = am.get_user_datasets()
        except APIKeyError:
            pass
        else:
            self.user_filter_chain.set_db_extract(db_extract)


def excepthook(etype, value, trace):
    """
    Handler for all unhandled exceptions.

    :param `etype`: the exception type (`SyntaxError`,
        `ZeroDivisionError`, etc...);
    :type `etype`: `Exception`
    :param string `value`: the exception error message;
    :param string `trace`: the traceback header, if any (otherwise, it
        prints the standard Python header: ``Traceback (most recent
        call last)``.
    """
    vinfo = "Unhandled exception in DCOR-Manager version {}:\n".format(
        __version__)
    tmp = tb.format_exception(etype, value, trace)
    exception = "".join([vinfo]+tmp)

    errorbox = QtWidgets.QMessageBox()
    errorbox.setIcon(QtWidgets.QMessageBox.Critical)
    errorbox.addButton(QtWidgets.QPushButton('Close'),
                       QtWidgets.QMessageBox.YesRole)
    errorbox.addButton(QtWidgets.QPushButton(
        'Copy text && Close'), QtWidgets.QMessageBox.NoRole)
    errorbox.setText(exception)
    ret = errorbox.exec_()
    if ret == 1:
        cb = QtWidgets.QApplication.clipboard()
        cb.clear(mode=cb.Clipboard)
        cb.setText(exception)


# Make Ctr+C close the app
signal.signal(signal.SIGINT, signal.SIG_DFL)
# Display exception hook in separate dialog instead of crashing
sys.excepthook = excepthook
