import subprocess
from collections import deque
from importlib import resources
import logging
import os
import pathlib
import sys
import time
import traceback

from PyQt5 import uic, QtCore, QtWidgets

from ..._version import version


levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


class StringSignalLogHandler(logging.Handler):
    new_message = QtCore.pyqtSignal(str)

    def __init__(self, signal, *args, **kwargs):
        super(StringSignalLogHandler, self).__init__(*args, **kwargs)
        self.signal = signal
        self.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(processName)s/%(threadName)s "
            + "in %(name)s: %(message)s",
            datefmt='%H:%M:%S'))

    def emit(self, record):
        self.signal.emit(self.format(record))


class WidgetLog(QtWidgets.QWidget):
    """Logging"""
    new_message = QtCore.pyqtSignal(str)

    def __init__(self, *args, **kwargs):
        super(WidgetLog, self).__init__(*args, **kwargs)
        ref_ui = resources.files("dcoraid.gui.logs") / "widget_log.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)
        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            self.comboBox_level.addItem(level, levels[level])

        # Logging output path
        log_dir = pathlib.Path(
            QtCore.QStandardPaths.writableLocation(
                QtCore.QStandardPaths.StandardLocation.TempLocation)
            ) / "DCOR-Aid-Logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Remove logs if there are more than 10
        if len(logs := sorted(log_dir.glob("*.log"))) > 10:
            for _ in range(len(logs) - 10):
                try:
                    logs.pop(-1).unlink()
                except BaseException:
                    print(traceback.format_exc())

        self.log_path = log_dir / time.strftime(
            "dcoraid_%Y-%m-%d_%H.%M.%S.log", time.localtime())
        self.log_fd = self.log_path.open("w", encoding="utf-8")

        # Set logging level to INFO for normal operations
        is_dev_version = version.count("post")
        self.comboBox_level.setCurrentIndex(0 if is_dev_version else 1)

        self.new_message.connect(self.add_colored_item)
        self.lineEdit_filter.textChanged.connect(self.on_filter_changed)
        self.comboBox_level.currentIndexChanged.connect(
            self.on_filter_changed)
        self.toolButton_dir.clicked.connect(self.on_log_dir_open)
        self.log_handler = StringSignalLogHandler(self.new_message)
        self.full_log = deque(maxlen=5000)

    @QtCore.pyqtSlot(str)
    def add_colored_item(self, msg, append_global=True):
        if append_global:
            self.full_log.append(msg)
            try:
                self.log_fd.write(f"{msg}\n")
                self.log_fd.flush()
            except BaseException:
                print(traceback.format_exc())

        if self.check_filter(msg):
            mlev = self.get_level(msg)
            if mlev >= logging.ERROR:
                style = "style='color:#A60000'"
            elif mlev >= logging.WARNING:
                style = "style='color:#7C4B00'"
            else:
                style = ""

            html = f"<div {style}>{msg}</div>"

            self.textEdit.append(html)

    def get_level(self, msg):
        try:
            mlev = levels[msg.split()[1]]
        except BaseException:
            mlev = logging.INFO
        return mlev

    def check_filter(self, msg):
        """Return True when `msg` should be displayed"""
        filt_str = self.lineEdit_filter.text().strip()
        curlev = self.comboBox_level.currentData()
        mlev = self.get_level(msg)

        if mlev < curlev:
            return False  # logging level too low
        elif not filt_str:
            return True  # no filters applied
        else:
            return filt_str.lower() in msg.lower()  # filter

    @QtCore.pyqtSlot()
    def on_filter_changed(self):
        self.textEdit.clear()
        for msg in self.full_log:
            self.add_colored_item(msg, append_global=False)

    @QtCore.pyqtSlot()
    def on_log_dir_open(self):
        path = str(self.log_path.parent)
        if os.name == "nt":
            os.startfile(os.path.normpath(path))
        elif sys.platform == "darwin":
            subprocess.Popen(["open", "-R", path])
        else:
            subprocess.Popen(["xdg-open", path])
