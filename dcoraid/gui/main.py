import atexit
import threading
import traceback
from contextlib import ExitStack
import logging
from importlib import resources
import signal
import sys
import time
import traceback as tb

import dclab
import requests
import requests_cache
import requests_toolbelt
import urllib3

from PyQt6 import uic, QtCore, QtGui, QtTest, QtWidgets

from ..api import APIOutdatedError
from ..dbmodel import CachedAPIInterrogator
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
    progress_update_event = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Initialize DCOR-Aid

        If you pass the "--version" command line argument, the
        application will print the version after initialization
        and exit.
        """
        self._update_thread = None
        self._update_worker = None

        self.database = None

        # Settings are stored in the .ini file format. Even though
        # `self.settings` may return integer/bool in the same session,
        # in the next session, it will reliably return strings. Lists
        # of strings (comma-separated) work nicely though.
        # Some promoted widgets need the below constants set in order
        # to access the settings upon initialization.
        QtCore.QCoreApplication.setOrganizationName("DCOR")
        QtCore.QCoreApplication.setOrganizationDomain("dc-cosmos.org")
        QtCore.QCoreApplication.setApplicationName("dcoraid")
        QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)

        super(DCORAid, self).__init__(*args, **kwargs)

        # if "--version" was specified, print the version and exit
        if "--version" in sys.argv:
            print(__version__)
            QtWidgets.QApplication.processEvents(
                QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)
            sys.exit(0)

        # Progressbar for database updates
        self._prog_update_db = None
        self._dbup_thread = None
        self._dbup_worker = None

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
        self.actionPreferences.triggered.connect(self.dlg_pref.show)
        self.actionSetupWizard.triggered.connect(self.on_wizard)
        # Help menu
        self.actionSoftware.triggered.connect(self.on_action_software)
        self.actionAbout.triggered.connect(self.on_action_about)

        # Display login status
        self.status_widget = StatusWidget(self.tabWidget)
        self.tabWidget.setCornerWidget(self.status_widget)
        self.status_widget.clicked.connect(self.dlg_pref.on_show_server)

        # Signal for requesting resource download
        self.panel_find_data.download_item.connect(
            self.panel_download.download_an_item)
        self.panel_my_data.download_item.connect(
            self.panel_download.download_an_item)

        # Signal for dataset upload or modification
        self.panel_upload.upload_finished.connect(self.on_dataset_changed)

        QtWidgets.QApplication.processEvents(
            QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)

        if self.settings.value("user scenario", "") == "anonymous":
            # disable tabs that an anonymous user cannot use
            self.tab_my_data.setEnabled(False)
            self.tab_maintain.setEnabled(False)
            self.tab_upload.setEnabled(False)

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

        # setup the metadata database
        try:
            self.database = CachedAPIInterrogator(
                api=get_ckan_api(),
                cache_location=QtCore.QStandardPaths.writableLocation(
                    QtCore.QStandardPaths.StandardLocation.CacheLocation)
                )
            self._last_asked_about_update = self.database.local_timestamp
            if not bool(int(self.settings.value(
                    "skip database update on startup", "0"))):
                self.check_update_database()
        except BaseException:
            self.logger.error(traceback.format_exc())
        else:
            self.panel_find_data.set_database(self.database)
            self.panel_my_data.set_database(self.database)
            self.panel_download.set_database(self.database)

        self.status_widget.request_status_update()

    @QtCore.pyqtSlot(bool)
    def check_update_database(self, reset=False, force=False):
        doit = False
        if not self.database:
            QtWidgets.QMessageBox.critical(
                self,
                "No connection",
                "Database could not be initialized. Please make sure "
                "your are connected to the network."
            )
        elif force:
            doit = True
            if (self.database.local_timestamp > time.time() - 10
                    and self._last_asked_about_update > time.time() - 1):
                # The user might be frantically hitting the update button,
                # because something is not showing up. Ask whether to reset
                # the database.
                button_reply = QtWidgets.QMessageBox.question(
                    self,
                    'User frustration detected',
                    "It seems like you are looking for a dataset that "
                    "is not here. If something has been modified by someone "
                    "different than you, it might help to reset the database. "
                    "Would you like to reset (instead of update) the "
                    "database? This will delete the local database and fetch "
                    "all metadata from the DCOR server.",
                    QtWidgets.QMessageBox.StandardButton.Yes
                    | QtWidgets.QMessageBox.StandardButton.No,
                    QtWidgets.QMessageBox.StandardButton.No)
                reset_db = (button_reply
                            == QtWidgets.QMessageBox.StandardButton.Yes)
                if reset_db:
                    self.database.reset_cache()
        else:
            if (self.database.local_timestamp < time.time() - 24*3600
                    and self._last_asked_about_update < time.time() - 3600):
                # Ask the user whether the cache should be updated
                button_reply = QtWidgets.QMessageBox.question(
                    self,
                    'Database outdated',
                    "The local database is outdated. Would you like to "
                    "refresh the database?",
                    QtWidgets.QMessageBox.StandardButton.Yes
                    | QtWidgets.QMessageBox.StandardButton.No,
                    QtWidgets.QMessageBox.StandardButton.Yes)
                doit = button_reply == QtWidgets.QMessageBox.StandardButton.Yes
        if doit:
            self._prog_update_db = QtWidgets.QProgressDialog(
                "Performing database update, please wait...\n" + " " * 200,
                "Abort",
                0,
                0,
                self
            )
            self._prog_update_db.setMinimumDuration(0)
            self._prog_update_db.show()

            # This is hell of a lot of boilerplate for just one progress bar.
            self._dbup_thread = QtCore.QThread()
            self._dbup_worker = UpdateDatabaseWorker(
                update=self.database.update, reset=reset)
            self._dbup_worker.moveToThread(self._dbup_thread)
            self._dbup_thread.started.connect(self._dbup_worker.run)
            self._dbup_worker.finished.connect(self._dbup_thread.quit)
            self._dbup_worker.finished.connect(self._dbup_worker.deleteLater)
            self._dbup_worker.progress.connect(self.on_progress_db_update)
            self._prog_update_db.canceled.connect(self._dbup_worker.cancelled)
            self._dbup_thread.start()

            while self._dbup_thread.isRunning():
                QtWidgets.QApplication.processEvents(
                    QtCore.QEventLoop.ProcessEventsFlag.AllEvents, 300)
                QtTest.QTest.qWait(500)

            self._prog_update_db.deleteLater()

        self._last_asked_about_update = time.time()

    @QtCore.pyqtSlot(QtCore.QEvent)
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

    @QtCore.pyqtSlot(dict)
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

    @QtCore.pyqtSlot(dict)
    def on_dataset_changed(self, ds_dict):
        self.database.update_dataset(ds_dict)

    @QtCore.pyqtSlot(dict)
    def on_progress_db_update(self, data):
        cdict = data["circle_current"]
        new_ds = data["datasets_new"]
        title = cdict.get("title")
        if not title:
            title = cdict.get("name")
        if len(title) > 50:
            title = title[:50] + "..."
        if self._prog_update_db is not None:
            self._prog_update_db.setLabelText(
                f"Fetching '{title}'\n"
                f"Imported {new_ds} datasets so far.")
            circle_ids = [c["id"] for c in data["circles"]]
            cur_index = circle_ids.index(cdict["id"])
            self._prog_update_db.setMaximum(len(circle_ids))
            self._prog_update_db.setValue(cur_index + 1)
        else:
            self.logger.error("Progress dialog not defined")

    @QtCore.pyqtSlot()
    def on_wizard(self):
        self.wizard = SetupWizard(self)
        self.wizard.exec()


class UpdateDatabaseWorker(QtCore.QObject):
    finished = QtCore.pyqtSignal()
    progress = QtCore.pyqtSignal(dict)

    def __init__(self, update, reset, *args, **kwargs):
        self.update = update
        self.reset = reset
        self.abort_event = threading.Event()
        super(UpdateDatabaseWorker, self).__init__(*args, **kwargs)

    @QtCore.pyqtSlot()
    def cancelled(self):
        self.abort_event.set()

    def run(self):
        """Long-running task."""
        self.update(reset=self.reset,
                    abort_event=self.abort_event,
                    callback=self.progress.emit)
        self.finished.emit()


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
    exc_long = "".join([vinfo] + tb.format_exception(etype, value, trace))
    exc_short = "".join([vinfo] + tb.format_exception(
        etype, value, trace, limit=3))

    # log the exception
    logger = logging.getLogger(__name__)
    logger.error(exc_long)

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
    copy_button = QtWidgets.QPushButton('Copy message to clipboard and close')
    copy_button.clicked.connect(lambda: copy_text_to_clipboard(exc_long))
    errorbox.addButton(QtWidgets.QPushButton('Close'),
                       QtWidgets.QMessageBox.ButtonRole.YesRole)
    errorbox.addButton(copy_button, QtWidgets.QMessageBox.ButtonRole.NoRole)
    errorbox.setDetailedText(exc_long)
    errorbox.setText(exc_short)
    errorbox.exec()


def copy_text_to_clipboard(text):
    cb = QtWidgets.QApplication.clipboard()
    cb.clear()
    cb.setText(text)


# Make Ctr+C close the app
signal.signal(signal.SIGINT, signal.SIG_DFL)
# Display exception hook in separate dialog instead of crashing
sys.excepthook = excepthook
