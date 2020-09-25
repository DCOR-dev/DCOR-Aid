import pkg_resources
import webbrowser

from PyQt5 import uic, QtCore, QtWidgets


class TableCellActions(QtWidgets.QWidget):
    upload_finished = QtCore.pyqtSignal()

    def __init__(self, job, *args, **kwargs):
        """Actions in a table cell"""
        super(TableCellActions, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "widget_tablecell_actions.ui")
        uic.loadUi(path_ui, self)
        self.job = job
        # signals and slots
        self.tb_error.clicked.connect(self.on_error)
        self.tb_retry.clicked.connect(self.on_retry)
        self.tb_view.clicked.connect(self.on_view)

    @QtCore.pyqtSlot()
    def on_error(self):
        msg = QtWidgets.QMessageBox()
        msg.setIcon(QtWidgets.QMessageBox.Critical)
        msg.setText("There was an error during data transfer. If this happens "
                    + "often or with a particular type of dataset, please "
                    + "<a href='https://github.com/DCOR-dev/DCOR-Aid/issues'>"
                    + "create an issue online</a>.")
        msg.setWindowTitle("Job {} error".format(self.job.dataset_id[:5]))
        msg.setDetailedText(self.job.traceback)
        msg.exec_()

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

        if state in ["finalize", "done"]:
            self.tb_view.show()
        else:
            self.tb_view.hide()

        if state == "error":
            self.tb_error.show()
            self.tb_retry.show()
        else:
            self.tb_error.hide()
            self.tb_retry.hide()

        if state == "transfer":
            self.tb_abort.show()
        else:
            self.tb_abort.hide()
