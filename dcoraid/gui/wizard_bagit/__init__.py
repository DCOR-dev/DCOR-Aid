from importlib import resources
import pathlib
import threading
import traceback

from PyQt6 import uic, QtCore, QtTest, QtWidgets
from PyQt6.QtWidgets import QWizard

from ..api import get_ckan_api
from ...bagit import bag_circle

from .. import settings


class BagItWizard(QtWidgets.QWizard):
    """DCOR-Aid setup wizard"""

    def __init__(self, *args, **kwargs):
        super().__init__(None)
        ref_ui = resources.files(
            "dcoraid.gui.wizard_bagit") / "wizard_bagit.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        self.settings = QtCore.QSettings()

        self.bagit_thread = None
        self.bagit_worker = None

        self.api = get_ckan_api()

        self.init_lock = threading.Lock()
        self._init_done = False

        self.event_bagit_abort = threading.Event()

        # progressbar
        self.progressBar.setMaximum(100_000)

        # user info
        self.label_user.setText(f"{self.api.user_name}")

        # fetch the circles that the user is a member of
        circles = self.api.get("organization_list_for_user",
                               permission="read")
        for ci in circles:
            self.comboBox_circles.addItem(
                ci["title"] if ci["title"] else ci["name"], ci["name"])
        self.on_circle_changed()

        # set default download location
        self.lineEdit_path.setText(str(self.download_path))
        self.lineEdit_path_2.setText(str(self.download_path))

        # disable going back from parameter page
        self.page_02_prepare.setCommitPage(True)

        # signals
        # circle selection
        self.comboBox_circles.currentIndexChanged.connect(
            self.on_circle_changed)
        # cancel wizard
        self.button(QWizard.WizardButton.CancelButton).clicked.connect(
            self.on_cancel)
        # browse button
        self.pushButton_browse.clicked.connect(self.on_browse)

    @property
    def download_path(self):
        path = settings.get_dir("bagit", self.settings)
        path = pathlib.Path(path).resolve()
        return path

    @download_path.setter
    def download_path(self, value):
        settings.set_dir("bagit", value, self.settings)
        self.lineEdit_path.setText(str(value))
        self.lineEdit_path_2.setText(str(value))

    def nextId(self):
        if self.currentPage() == self.page_03_bagit:
            self.start_bagit()
            page_dict = {}
            for ii in self.pageIds():
                page_dict[self.page(ii)] = ii

            # force user to stay on that page
            return page_dict[self.page_03_bagit]
        elif self.currentPage() in [self.page_04_finish, self.page_05_fail]:
            return -1
        else:
            return super().nextId()

    @QtCore.pyqtSlot(bool)
    def on_bagit_finished(self, success):
        page_dict = {}
        for ii in self.pageIds():
            page_dict[self.page(ii)] = ii

        if success:
            self.setCurrentId(page_dict[self.page_04_finish])
        else:
            self.setCurrentId(page_dict[self.page_05_fail])

    @QtCore.pyqtSlot()
    def on_browse(self):
        path = QtWidgets.QFileDialog.getExistingDirectory(
            parent=self,
            caption='Archive location',
            directory=str(self.download_path),
            )
        if path:
            self.download_path = path

    @QtCore.pyqtSlot()
    def on_cancel(self):
        self.event_bagit_abort.set()
        if self.bagit_thread is not None:
            while self.bagit_thread.isRunning():
                QtTest.QTest.qWait(200)

    @QtCore.pyqtSlot()
    def on_circle_changed(self):
        current_circle = self.comboBox_circles.currentData()
        self.label_circle.setText(current_circle)

    @QtCore.pyqtSlot(float)
    def on_progress(self, value):
        self.progressBar.setValue(int(value*100_000))

    def start_bagit(self):
        # prevent multiple calls to this method
        with self.init_lock:
            if self._init_done:
                return
            else:
                self._init_done = True

        # initialize the worker and connect all signals
        self.bagit_thread = QtCore.QThread()
        circle_name = self.comboBox_circles.currentData()
        bagit_kwargs = {
            "api": self.api,
            "circle_name": circle_name,
            "target_path": self.download_path / circle_name,
            "abort_event": self.event_bagit_abort,
        }
        self.bagit_worker = BagItWorker(bagit_kwargs=bagit_kwargs)
        self.bagit_worker.moveToThread(self.bagit_thread)
        self.bagit_thread.started.connect(
            self.bagit_worker.run)
        self.bagit_worker.finished.connect(
            self.bagit_thread.quit)
        self.bagit_worker.finished.connect(
            self.bagit_worker.deleteLater)
        self.bagit_worker.finished.connect(
            self.on_bagit_finished)
        self.bagit_worker.progress.connect(
            self.on_progress)
        self.bagit_worker.traceback.connect(
            self.plainTextEdit_traceback.setPlainText)
        self.bagit_thread.start()

    def validateCurrentPage(self):
        page = self.currentPage()
        if page == self.page_02_prepare:
            return (bool(self.download_path.is_dir())
                    and bool(self.comboBox_circles.currentData()))
        else:
            return super().validateCurrentPage()


class BagItWorker(QtCore.QThread):
    finished = QtCore.pyqtSignal(bool)
    progress = QtCore.pyqtSignal(float)
    traceback = QtCore.pyqtSignal(str)

    def __init__(self, bagit_kwargs, *args, **kwargs):
        self.bagit_kwargs = bagit_kwargs
        super().__init__(*args, **kwargs)

    def run(self):
        try:
            bag_circle(callback=self.progress.emit, **self.bagit_kwargs)
        except BaseException:
            print(traceback.format_exc())
            self.traceback.emit(traceback.format_exc())
            self.finished.emit(False)
        else:
            self.finished.emit(True)
