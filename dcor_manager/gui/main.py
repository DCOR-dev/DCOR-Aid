import os
import pathlib
import pkg_resources
import signal
import sys
import traceback
import webbrowser

import appdirs
import dclab
import h5py
import numpy
import scipy

from PyQt5 import uic, QtCore, QtGui, QtWidgets
import pyqtgraph as pg

from . import widgets

from .. import settings
from .._version import version as __version__
from .widgets.wait_cursor import ShowWaitCursor, show_wait_cursor


# global plotting configuration parameters
pg.setConfigOption("background", "w")
pg.setConfigOption("foreground", "k")
pg.setConfigOption("antialias", True)
pg.setConfigOption("imageAxisOrder", "row-major")


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
        path_ui = pkg_resources.resource_filename("dcor_manager.gui", "main.ui")
        uic.loadUi(path_ui, self)
        #: DCOR-Manager settings
        self.settings = settings.SettingsFile()
        # Register user-defined DCOR API Key in case the user wants to
        # open a session with private data.
        api_key = self.settings.get_string("dcor api key")
        dclab.rtdc_dataset.fmt_dcor.APIHandler.add_api_key(api_key)
        # GUI
        self.setWindowTitle("DCOR-Manager {}".format(__version__))
        # Disable native menubar (e.g. on Mac)
        self.menubar.setNativeMenuBar(False)
        # Help menu
        self.actionSoftware.triggered.connect(self.on_action_software)
        # Initialize
        self.showMaximized()
        # if "--version" was specified, print the version and exit
        if "--version" in sys.argv:
            print(__version__)
            QtWidgets.QApplication.processEvents()
            sys.exit(0)

    def on_action_about(self):
        about_text = "GUI for managing data on DCOR."
        QtWidgets.QMessageBox.about(self,
                                    "DCOR-Manager {}".format(__version__),
                                    about_text)

    def on_action_save(self):
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, 'Save session', '', 'Shape-Out 2 session (*.so2)')
        if path:
            if not path.endswith(".so2"):
                path += ".so2"
            session.save_session(path, self.pipeline)

    def on_action_software(self):
        libs = [appdirs,
                dclab,
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
    tmp = traceback.format_exception(etype, value, trace)
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
