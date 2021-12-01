import logging
import os
import pathlib
import pkg_resources
import signal
import sys
import time
import traceback as tb

import dclab
import requests
import requests_toolbelt

from PyQt5 import uic, QtCore, QtGui, QtWidgets

from ..api import APIOutdatedError, NoAPIKeyError
from ..common import ConnectionTimeoutErrors
from ..dbmodel import APIInterrogator, DBExtract
from .._version import version as __version__

from .api import get_ckan_api
from .preferences import PreferencesDialog
from .tools import run_async
from .wizard import SetupWizard

# set Qt icon theme search path
QtGui.QIcon.setThemeSearchPaths([
    os.path.join(pkg_resources.resource_filename("dcoraid", "img"),
                 "icon-theme")])
QtGui.QIcon.setThemeName(".")


class DCORAid(QtWidgets.QMainWindow):
    plots_changed = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Initialize DCOR_Manager

        If you pass the "--version" command line argument, the
        application will print the version after initialization
        and exit.
        """
        super(DCORAid, self).__init__(*args, **kwargs)

        # if "--version" was specified, print the version and exit
        if "--version" in sys.argv:
            print(__version__)
            QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents,
                                                 300)
            sys.exit(0)

        # Settings are stored in the .ini file format. Even though
        # `self.settings` may return integer/bool in the same session,
        # in the next session, it will reliably return strings. Lists
        # of strings (comma-separated) work nicely though.
        # Some promoted widgets need the below constants set in order
        # to access the settings upon initialization.
        QtCore.QCoreApplication.setOrganizationName("DCOR")
        QtCore.QCoreApplication.setOrganizationDomain("dcor.mpl.mpg.de")
        QtCore.QCoreApplication.setApplicationName("dcoraid")
        QtCore.QSettings.setDefaultFormat(QtCore.QSettings.IniFormat)

        # setup logging
        log_dir = pathlib.Path(
            QtCore.QStandardPaths.writableLocation(
                QtCore.QStandardPaths.AppLocalDataLocation)) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / time.strftime("dcoraid_%Y-%m-%d_%H-%M-%S.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s [%(threadName)s] in %(name)s "
                   + "%(funcName)s:%(lineno)d: %(message)s",
            filename=str(log_file))
        # keep ten logs
        log_entries = sorted(log_dir.glob("*.log"))
        log_entries.reverse()
        for pp in log_entries[10:]:
            pp.unlink()

        #: DCOR-Aid settings
        self.settings = QtCore.QSettings()
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui", "main.ui")
        uic.loadUi(path_ui, self)

        # GUI
        # Preferences dialog
        self.dlg_pref = PreferencesDialog()
        # Window title
        self.setWindowTitle(f"DCOR-Aid {__version__}")
        # Disable native menubar (e.g. on Mac)
        self.menubar.setNativeMenuBar(False)
        # File menu
        self.actionPreferences.triggered.connect(self.dlg_pref.show_server)
        self.actionSetupWizard.triggered.connect(self.on_wizard)
        # Help menu
        self.actionSoftware.triggered.connect(self.on_action_software)
        self.actionAbout.triggered.connect(self.on_action_about)

        # Display login status
        self.status_widget = StatusWidget(self)
        self.tabWidget.setCornerWidget(self.status_widget)
        self.status_widget.clicked.connect(self.dlg_pref.on_show_server)
        # Call refresh_login status regularly
        self.refresh_login_status()  # runs asynchronously
        if bool(int(self.settings.value("debug/without timers", "0"))):
            self.timer = None
        else:
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self.refresh_login_status)
            self.timer.start(300000)

        # Signals for user datasets (my data)
        self.pushButton_user_refresh.clicked.connect(
            self.on_refresh_private_data)

        # Signals for public data browser
        self.pushButton_public_search.clicked.connect(self.on_public_search)

        # Signal for requesting resource download
        self.public_filter_chain.download_resource.connect(
            self.panel_download.download_resource)
        self.user_filter_chain.download_resource.connect(
            self.panel_download.download_resource)

        QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents,
                                             300)

        # Run wizard if necessary
        if ((self.settings.value("user scenario", "") != "anonymous")
                and not self.settings.value("auth/api key", "")):
            # User has not done anything yet
            self.on_wizard()

        self.show()
        self.raise_()

    def close(self):
        if self.timer is not None:
            self.timer.stop()
        if self.panel_upload.widget_jobs.timer is not None:
            self.panel_upload.widget_jobs.timer.stop()
        self.panel_upload.jobs.daemon_compress.join()
        self.panel_upload.jobs.daemon_upload.join()
        self.panel_upload.jobs.daemon_verify.join()
        self.panel_upload.jobs.daemon_compress.terminate()
        self.panel_upload.jobs.daemon_upload.terminate()
        self.panel_upload.jobs.daemon_verify.terminate()
        QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents,
                                             3000)
        super(DCORAid, self).close()

    @QtCore.pyqtSlot()
    def on_action_about(self):
        dcor = "https://dcor.mpl.mpg.de"
        gh = "DCOR-dev/DCOR-Aid"
        rtd = "dc.readthedocs.io"
        about_text = "This is the client for the <a href='{}'>".format(dcor) \
            + "Deformability Cytometry Open Repository (DCOR)</a>.<br><br>" \
            + "Author: Paul Müller<br>" \
            + "GitHub: " \
            + "<a href='https://github.com/{gh}'>{gh}</a><br>".format(gh=gh) \
            + "Documentation: " \
            + "<a href='https://{rtd}'>{rtd}</a><br>".format(rtd=rtd)
        QtWidgets.QMessageBox.about(self,
                                    "DCOR-Aid {}".format(__version__),
                                    about_text)

    @QtCore.pyqtSlot()
    def on_action_software(self):
        libs = [dclab,
                requests,
                requests_toolbelt,
                ]
        sw_text = "DCOR-Aid {}\n\n".format(__version__)
        sw_text += "Python {}\n\n".format(sys.version)
        sw_text += "Modules:\n"
        for lib in libs:
            sw_text += "- {} {}\n".format(lib.__name__, lib.__version__)
        sw_text += "- PyQt5 {}\n".format(QtCore.QT_VERSION_STR)
        sw_text += "\n Breeze icon theme by the KDE Community (LGPL)."
        sw_text += "\n Font-Awesome icons by Fort Awesome (CC BY 4.0)."
        if hasattr(sys, 'frozen'):
            sw_text += "\nThis executable has been created using PyInstaller."
        QtWidgets.QMessageBox.information(self,
                                          "Software",
                                          sw_text)

    @QtCore.pyqtSlot()
    def on_public_search(self):
        self.tab_browse.setCursor(QtCore.Qt.WaitCursor)
        api = get_ckan_api(
            public=not self.checkBox_public_include_private.isChecked())
        try:
            ai = APIInterrogator(api=api)
            dbextract = ai.search_dataset(
                self.lineEdit_public_search.text(),
                limit=self.spinBox_public_rows.value())
            self.public_filter_chain.set_db_extract(dbextract)
        except ConnectionTimeoutErrors:
            QtWidgets.QMessageBox.critical(
                self,
                f"Failed to connect to {api.server}",
                tb.format_exc(limit=1))
        self.tab_browse.setCursor(QtCore.Qt.ArrowCursor)

    @QtCore.pyqtSlot()
    def on_refresh_private_data(self):
        self.tab_user.setCursor(QtCore.Qt.WaitCursor)
        api = get_ckan_api()
        data = DBExtract()
        if api.is_available() and api.api_key:
            try:
                ai = APIInterrogator(api=api)
                if self.checkBox_user_following.isChecked():
                    data += ai.get_datasets_user_following()
                if self.checkBox_user_owned.isChecked():
                    data += ai.get_datasets_user_owned()
                if self.checkBox_user_shared.isChecked():
                    data += ai.get_datasets_user_shared()
                self.user_filter_chain.set_db_extract(data)
            except ConnectionTimeoutErrors:
                QtWidgets.QMessageBox.critical(
                    self,
                    f"Failed to connect to {api.server}",
                    tb.format_exc(limit=1))
        self.tab_user.setCursor(QtCore.Qt.ArrowCursor)

    @QtCore.pyqtSlot()
    def on_wizard(self):
        self.wizard = SetupWizard(self)
        self.wizard.exec_()

    @run_async
    @QtCore.pyqtSlot()
    def refresh_login_status(self):
        api = get_ckan_api()
        if not api.api_key:
            text = "Anonymous"
            tip = "Click here to enter your API key."
            icon = "user"
        else:
            try:
                user_data = api.get_user_dict()
            except NoAPIKeyError:
                text = "Login failed"
                tip = "Click here to update your API key."
                icon = "user-times"
            except ConnectionTimeoutErrors:
                text = "No connection"
                tip = "Can you access {} via a browser?".format(api.server)
                icon = "hourglass"
            else:
                fullname = user_data["fullname"]
                name = user_data["name"]
                if not fullname:
                    fullname = name
                text = "{}".format(fullname)
                tip = "user '{}'".format(name)
                icon = "user-lock"
        self.status_widget.set_status(text=text,
                                      tooltip=tip,
                                      icon=icon,
                                      server=api.server)


class StatusWidget(QtWidgets.QWidget):
    clicked = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(StatusWidget, self).__init__(*args, **kwargs)
        self.layout = QtWidgets.QHBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        self.flabel = QtWidgets.QLabel(self)
        self.layout.addWidget(self.flabel)

        self.toolButton_user = QtWidgets.QToolButton()
        self.toolButton_user.setText("Initialization...")
        self.toolButton_user.setToolButtonStyle(
            QtCore.Qt.ToolButtonTextBesideIcon)
        self.toolButton_user.setAutoRaise(True)
        self.layout.addWidget(self.toolButton_user)
        self.toolButton_user.clicked.connect(self.clicked)

    @staticmethod
    def get_favicon(server):
        dldir = pathlib.Path(
            QtCore.QStandardPaths.writableLocation(
                QtCore.QStandardPaths.AppDataLocation)) / "favicons"

        dldir.mkdir(exist_ok=True, parents=True)
        favname = dldir / (server.split("://")[1] + "_favicon.ico")
        if not favname.exists():
            try:
                r = requests.get(server + "/favicon.ico", timeout=3.05)
                if r.ok:
                    with favname.open("wb") as fd:
                        fd.write(r.content)
                else:
                    raise ValueError("No favicon!")
                favicon = QtGui.QIcon(str(favname))
            except BaseException:
                favicon = QtGui.QIcon()
        else:
            favicon = QtGui.QIcon(str(favname))
        return favicon

    def set_status(self, text, tooltip, icon, server):
        favicon = self.get_favicon(server)
        self.flabel.setPixmap(favicon.pixmap(16, 16))
        self.flabel.setToolTip(server)
        self.toolButton_user.setText(text)
        self.toolButton_user.setToolTip(tooltip)
        self.toolButton_user.setIcon(QtGui.QIcon.fromTheme(icon))


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
    vinfo = "Unhandled exception in DCOR-Aid version {}:\n".format(
        __version__)
    tmp = tb.format_exception(etype, value, trace)
    exception = "".join([vinfo] + tmp)

    # log the exception
    logger = logging.getLogger(__name__)
    logger.error(exception)

    # Test connectivity
    if etype is APIOutdatedError:
        QtWidgets.QMessageBox.warning(
            None,
            "DCOR-Aid version outdated!",
            "Your version of DCOR-Aid is outdated. Please "
            + "update DCOR-Aid."
        )

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
