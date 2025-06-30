import atexit
from contextlib import ExitStack
import logging
from importlib import resources
import signal
import sys
import traceback as tb

import dclab
import requests
import requests_cache
import requests_toolbelt
import urllib3

from PyQt6 import uic, QtCore, QtGui, QtWidgets

from ..api import APIOutdatedError
from ..common import ConnectionTimeoutErrors
from ..dbmodel import APIInterrogator, DBExtract
from .._version import __version__

from .api import get_ckan_api
from .preferences import PreferencesDialog
from .status_widget import StatusWidget
from . import updater
from .wizard import SetupWizard

file_manager = ExitStack()
atexit.register(file_manager.close)

# set Qt icon theme search path
ref = resources.files('dcoraid.img') / 'icon-theme'
path = file_manager.enter_context(resources.as_file(ref))
QtGui.QIcon.setThemeSearchPaths([str(path)])
QtGui.QIcon.setThemeName(".")


class DCORAid(QtWidgets.QMainWindow):
    plots_changed = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Initialize DCOR-Aid

        If you pass the "--version" command line argument, the
        application will print the version after initialization
        and exit.
        """
        self._update_thread = None
        self._update_worker = None

        # Settings are stored in the .ini file format. Even though
        # `self.settings` may return integer/bool in the same session,
        # in the next session, it will reliably return strings. Lists
        # of strings (comma-separated) work nicely though.
        # Some promoted widgets need the below constants set in order
        # to access the settings upon initialization.
        QtCore.QCoreApplication.setOrganizationName("DCOR")
        QtCore.QCoreApplication.setOrganizationDomain("dcor.mpl.mpg.de")
        QtCore.QCoreApplication.setApplicationName("dcoraid")
        QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)

        super(DCORAid, self).__init__(*args, **kwargs)

        # if "--version" was specified, print the version and exit
        if "--version" in sys.argv:
            print(__version__)
            QtWidgets.QApplication.processEvents(
                QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)
            sys.exit(0)

        #: DCOR-Aid settings
        self.settings = QtCore.QSettings()
        ref_ui = resources.files("dcoraid.gui") / "main.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        # setup logging
        root_logger = logging.getLogger()
        root_logger.addHandler(self.panel_logs.log_handler)

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"DCOR-Aid {__version__}")

        # GUI
        # Preferences dialog
        self.dlg_pref = PreferencesDialog(self)
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
        self.status_widget = StatusWidget(self.tabWidget)
        self.tabWidget.setCornerWidget(self.status_widget)
        self.status_widget.clicked.connect(self.dlg_pref.on_show_server)

        # Signals for user datasets (my data)
        self.pushButton_user_refresh.clicked.connect(
            self.on_refresh_private_data)

        # Signal for requesting resource download
        self.panel_browse_public.request_download.connect(
            self.panel_download.download_resource)
        self.user_filter_chain.download_resource.connect(
            self.panel_download.download_resource)

        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)

        # Run wizard if necessary
        if ((self.settings.value("user scenario", "") != "anonymous")
                and not self.settings.value("auth/api key", "")):
            # User has not done anything yet
            self.on_wizard()

        # check for updates
        do_update = int(self.settings.value("check for updates", "1"))
        self.on_action_check_update(do_update)

        self.show()
        self.raise_()

        self.status_widget.request_status_update()

    def closeEvent(self, event):
        root_logger = logging.getLogger()
        while len(root_logger.handlers) > 0:
            h = root_logger.handlers[0]
            root_logger.removeHandler(h)
        self.panel_upload.prepare_quit()
        self.panel_download.prepare_quit()
        self.status_widget.prepare_quit()
        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)
        event.accept()

    @QtCore.pyqtSlot()
    def on_action_about(self):
        dcor = "https://dcor.mpl.mpg.de"
        gh = "DCOR-dev/DCOR-Aid"
        rtd = "dc.readthedocs.io"
        about_text = (
            f"This is the client for the <a href='{dcor}'>"
            f"Deformability Cytometry Open Repository (DCOR)</a>.<br><br>"
            f"Author: Paul Müller<br>"
            f"GitHub: "
            f"<a href='https://github.com/{gh}'>{gh}</a><br>"
            f"Documentation: "
            f"<a href='https://{rtd}'>{rtd}</a><br>")
        QtWidgets.QMessageBox.about(self,
                                    f"DCOR-Aid {__version__}",
                                    about_text)

    @QtCore.pyqtSlot(bool)
    def on_action_check_update(self, b):
        self.settings.setValue("check for updates", f"{int(b)}")
        if b and self._update_thread is None:
            self._update_thread = QtCore.QThread()
            self._update_worker = updater.UpdateWorker()
            self._update_worker.moveToThread(self._update_thread)
            self._update_worker.finished.connect(self._update_thread.quit)
            self._update_worker.data_ready.connect(
                self.on_action_check_update_finished)
            self._update_thread.start()

            ghrepo = "DCOR-dev/DCOR-Aid"

            QtCore.QMetaObject.invokeMethod(
                self._update_worker,
                'processUpdate',
                QtCore.Qt.ConnectionType.QueuedConnection,
                QtCore.Q_ARG(str, __version__),
                QtCore.Q_ARG(str, ghrepo),
            )

    def on_action_check_update_finished(self, mdict):
        # cleanup
        self._update_thread.quit()
        self._update_thread.wait()
        self._update_worker = None
        self._update_thread = None
        # display message box
        ver = mdict["version"]
        web = mdict["releases url"]
        dlb = mdict["binary url"]
        msg = QtWidgets.QMessageBox()
        msg.setWindowTitle(f"DCOR-Aid {ver} available!")
        msg.setTextFormat(QtCore.Qt.TextFormat.RichText)
        text = f"You can install DCOR-Aid {ver} "
        if dlb is not None:
            text += f'from a <a href="{dlb}">direct download</a>. '
        else:
            text += 'by running `pip install --upgrade dcoraid`. '
        text += f'Visit the <a href="{web}">official release page</a>!'
        msg.setText(text)
        msg.exec()

    @QtCore.pyqtSlot()
    def on_action_software(self):
        libs = [dclab,
                requests,
                requests_cache,
                requests_toolbelt,
                urllib3,
                ]
        sw_text = f"DCOR-Aid {__version__}\n\n"
        sw_text += f"Python {sys.version}\n\n"
        sw_text += "Modules:\n"
        for lib in libs:
            sw_text += f"- {lib.__name__} {lib.__version__}\n"
        sw_text += f"- PyQt6 {QtCore.QT_VERSION_STR}\n"
        sw_text += "\n Breeze icon theme by the KDE Community (LGPL)."
        sw_text += "\n Font-Awesome icons by Fort Awesome (CC BY 4.0)."
        if hasattr(sys, 'frozen'):
            sw_text += "\nThis executable has been created using PyInstaller."
        QtWidgets.QMessageBox.information(self,
                                          "Software",
                                          sw_text)

    @QtCore.pyqtSlot()
    def on_refresh_private_data(self):
        self.tab_user.setCursor(QtCore.Qt.CursorShape.WaitCursor)
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
                self.logger.error(tb.format_exc())
                QtWidgets.QMessageBox.critical(
                    self,
                    f"Failed to connect to {api.server}",
                    tb.format_exc(limit=1))
        self.tab_user.setCursor(QtCore.Qt.CursorShape.ArrowCursor)

    @QtCore.pyqtSlot()
    def on_wizard(self):
        self.wizard = SetupWizard(self)
        self.wizard.exec()


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
    vinfo = f"Unhandled exception in DCOR-Aid version {__version__}:\n"
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
    errorbox.setIcon(QtWidgets.QMessageBox.Icon.Critical)
    errorbox.addButton(QtWidgets.QPushButton('Close'),
                       QtWidgets.QMessageBox.ButtonRole.YesRole)
    errorbox.addButton(QtWidgets.QPushButton(
        'Copy text && Close'), QtWidgets.QMessageBox.ButtonRole.NoRole)
    errorbox.setText(exception)
    ret = errorbox.exec()
    if ret == 1:
        cb = QtWidgets.QApplication.clipboard()
        cb.clear(mode=cb.Clipboard)
        cb.setText(exception)


# Make Ctr+C close the app
signal.signal(signal.SIGINT, signal.SIG_DFL)
# Display exception hook in separate dialog instead of crashing
sys.excepthook = excepthook
