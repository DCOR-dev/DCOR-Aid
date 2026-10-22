import os
import platform
import subprocess
import webbrowser

from PyQt6 import QtCore, QtWidgets

from .widget_actions_download_ui import Ui_Form


class TableCellActionsDownload(QtWidgets.QWidget):
    delete_job = QtCore.pyqtSignal(str)

    def __init__(self, job, *args, **kwargs):
        """Actions in a table cell

        Used for the "Running Downloads" table in the "Downloads" tab.
        """
        super().__init__(*args, **kwargs)
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        self.job = job
        # signals and slots
        self.ui.tb_delete.clicked.connect(self.on_delete)
        self.ui.tb_error.clicked.connect(self.on_error)
        self.ui.tb_retry.clicked.connect(self.on_retry)
        self.ui.tb_view.clicked.connect(self.on_view)
        self.ui.tb_folder.clicked.connect(self.on_folder)

        for tbact in [self.ui.tb_delete,
                      self.ui.tb_error,
                      self.ui.tb_retry,
                      self.ui.tb_view,
                      self.ui.tb_folder,
                      ]:
            row_height = tbact.geometry().height()
            tbact.setFixedSize(row_height - 2, row_height - 2)

    @QtCore.pyqtSlot()
    def on_delete(self):
        self.delete_job.emit(self.job.job_id)

    @QtCore.pyqtSlot()
    def on_error(self):
        msg = QtWidgets.QMessageBox()
        msg.setIcon(QtWidgets.QMessageBox.Icon.Critical)
        msg.setText("There was an error during data transfer. If this happens "
                    + "often or with a particular type of dataset, please "
                    + "<a href='https://github.com/DCOR-dev/DCOR-Aid/issues'>"
                    + "create an issue online</a>. Click on the button below "
                      "to see details required for fixing the problem.")
        msg.setWindowTitle(f"Job {self.job.job_id[:5]} error")
        msg.setDetailedText(self.job.traceback)
        msg.exec()

    @QtCore.pyqtSlot()
    def on_folder(self):
        dl_path = self.job.path
        if not dl_path.is_dir():
            dl_path = dl_path.parent
        open_file(dl_path)

    @QtCore.pyqtSlot()
    def on_retry(self):
        self.job.retry_download()

    @QtCore.pyqtSlot()
    def on_view(self):
        ds_dict = self.job.get_dataset_dict()
        url = (f"{self.job.api.server}"
               f"/dataset/{ds_dict['id']}"
               f"/resource/{self.job.resource_id}")
        webbrowser.open(url)

    def refresh_visibility(self, job):
        """Show or hide the different toolbuttons depending on the job state"""
        self.job = job
        state = job.state

        if state in ["abort", "error"]:
            self.ui.tb_retry.show()
        else:
            self.ui.tb_retry.hide()

        if state == "error":
            self.ui.tb_error.show()
        else:
            self.ui.tb_error.hide()


def open_file(path):
    if platform.system() == "Windows":
        os.startfile(path)
    elif platform.system() == "Darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])
