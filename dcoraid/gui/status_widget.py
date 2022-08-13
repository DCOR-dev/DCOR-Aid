import logging
import pathlib
import requests
import traceback

from PyQt5 import QtCore, QtGui, QtWidgets

from ..common import ConnectionTimeoutErrors

from .api import get_ckan_api, setup_certificate_file


class StatusWidget(QtWidgets.QWidget):
    clicked = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(StatusWidget, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
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
        self.toolButton_user.clicked.connect(self.request_status_update)

        # Automated updates of login status
        # thread pool
        self.thread_pool = QtCore.QThreadPool()
        self.thread_pool.setMaxThreadCount(2)
        # worker
        self.status_worker = StatusWidetUpdateWorker()
        self.status_worker.setAutoDelete(False)
        self.status_worker.signal.state_signal.connect(self.set_status)
        # initial refresh
        self.request_status_update()

        settings = QtCore.QSettings()
        if bool(int(settings.value("debug/without timers", "0"))):
            self.timer = None
        else:
            # refresh login status every 5 min
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self.request_status_update)
            self.timer.start(300000)

    @staticmethod
    def get_favicon(server):
        dldir = pathlib.Path(
            QtCore.QStandardPaths.writableLocation(
                QtCore.QStandardPaths.AppDataLocation)) / "favicons"

        dldir.mkdir(exist_ok=True, parents=True)
        favname = dldir / (server.split("://")[1] + "_favicon.ico")
        if not favname.exists():
            try:
                r = requests.get(server + "/favicon.ico",
                                 verify=setup_certificate_file(),
                                 timeout=3.05)
                if r.ok:
                    with favname.open("wb") as fd:
                        fd.write(r.content)
                else:
                    raise ValueError("No favicon!")
                favicon = QtGui.QIcon(str(favname))
            except BaseException:
                logger = logging.getLogger(__name__)
                logger.error(traceback.format_exc())
                favicon = QtGui.QIcon()
        else:
            favicon = QtGui.QIcon(str(favname))
        return favicon

    @QtCore.pyqtSlot()
    def request_status_update(self):
        self.thread_pool.start(self.status_worker)

    @QtCore.pyqtSlot(str, str, str, str)
    def set_status(self, text, tooltip, icon, server):
        favicon = self.get_favicon(server)
        self.flabel.setPixmap(favicon.pixmap(16, 16))
        self.flabel.setToolTip(server)
        self.toolButton_user.setText(text)
        self.toolButton_user.setToolTip(tooltip)
        self.toolButton_user.setIcon(QtGui.QIcon.fromTheme(icon))

    def stop_timers(self):
        if self.timer is not None:
            self.timer.stop()


class StatusWidetUpdateWorker(QtCore.QRunnable):
    """Worker for updating the current API situation
    """
    def __init__(self):
        super(StatusWidetUpdateWorker, self).__init__()
        self.signal = StatusWidetUpdateWorkerSignals()
        self.logger = logging.getLogger(__name__)

    @QtCore.pyqtSlot()
    def run(self):
        """Determine the API situation"""
        api = get_ckan_api()

        if not api.is_available():
            text = "No connection"
            tip = f"Can you access {api.server} via a browser?"
            icon = "hourglass"
        elif not api.is_available(with_correct_version=True):
            text = "Server out of date"
            tip = "Please downgrade DCOR-Aid"
            icon = "ban"
        elif not api.api_key:
            text = "Anonymous"
            tip = "Click here to enter your API key."
            icon = "user"
        elif not api.is_available(with_api_key=True):
            text = "API token incorrect"
            tip = "Click here to update your API key."
            icon = "user-times"
        else:
            try:
                user_data = api.get_user_dict()
            except ConnectionTimeoutErrors:
                self.logger.error(traceback.format_exc())
                text = "Connection timeout"
                tip = f"Can you access {api.server} via a browser?"
                icon = "hourglass"
            else:
                fullname = user_data["fullname"]
                name = user_data["name"]
                if not fullname:
                    fullname = name
                text = "{}".format(fullname)
                tip = "user '{}'".format(name)
                icon = "user-lock"
        self.logger.info(text)
        self.signal.state_signal.emit(text, tip, icon, api.server)


class StatusWidetUpdateWorkerSignals(QtCore.QObject):
    state_signal = QtCore.pyqtSignal(str, str, str, str)
