import webbrowser

from PyQt6 import QtCore, QtWidgets

from .widget_actions_upload_ui import Ui_Form


class TableCellActionsUpload(QtWidgets.QWidget):
    abort_job = QtCore.pyqtSignal(str)
    delete_job = QtCore.pyqtSignal(str)

    def __init__(self, job, *args, **kwargs):
        """Actions in a table cell

        Used for the "Running Uploads" table in the "Uploads" tab.
        """
        super().__init__(*args, **kwargs)
        self.ui = Ui_Form()
        self.ui.setupUi(self)

        self.job = job
        # signals and slots
        self.ui.tb_abort.clicked.connect(self.on_abort)
        self.ui.tb_delete.clicked.connect(self.on_delete)
        self.ui.tb_error.clicked.connect(self.on_error)
        self.ui.tb_retry.clicked.connect(self.on_retry)
        self.ui.tb_view.clicked.connect(self.on_view)

        for tbact in [self.ui.tb_abort,
                      self.ui.tb_delete,
                      self.ui.tb_error,
                      self.ui.tb_retry,
                      self.ui.tb_view,
                      ]:
            row_height = tbact.geometry().height()
            tbact.setFixedSize(row_height - 2, row_height - 2)

    @QtCore.pyqtSlot()
    def on_abort(self):
        self.abort_job.emit(self.job.dataset_id)

    @QtCore.pyqtSlot()
    def on_delete(self):
        self.delete_job.emit(self.job.dataset_id)

    @QtCore.pyqtSlot()
    def on_error(self):
        msg = QtWidgets.QMessageBox()
        msg.setIcon(QtWidgets.QMessageBox.Icon.Critical)
        msg.setText("There was an error during data transfer. If this happens "
                    + "often or with a particular type of dataset, please "
                    + "<a href='https://github.com/DCOR-dev/DCOR-Aid/issues'>"
                    + "create an issue online</a>. Click on the button below "
                      "to see details required for fixing the problem.")
        msg.setWindowTitle(f"Job {self.job.dataset_id[:5]} error")
        msg.setDetailedText(self.job.traceback)
        msg.exec()

    @QtCore.pyqtSlot()
    def on_retry(self):
        self.job.retry_upload()

    @QtCore.pyqtSlot()
    def on_view(self):
        url = self.job.get_dataset_url()
        webbrowser.open(url)

    def refresh_visibility(self, job):
        """Show or hide the different toolbuttons depending on the job state"""
        self.job = job
        state = job.state

        if state in ["online", "verify", "finalize", "done"]:
            self.ui.tb_view.show()
        else:
            self.ui.tb_view.hide()

        if state in ["abort", "error"]:
            self.ui.tb_retry.show()
        else:
            self.ui.tb_retry.hide()

        if state == "error":
            self.ui.tb_error.show()
        else:
            self.ui.tb_error.hide()

        if state in ["compress", "transfer"]:
            self.ui.tb_abort.show()
        else:
            self.ui.tb_abort.hide()

        if state not in ["compress", "transfer"]:
            self.ui.tb_delete.show()
        else:
            self.ui.tb_delete.hide()
