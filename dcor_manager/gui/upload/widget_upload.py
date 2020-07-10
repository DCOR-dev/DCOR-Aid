import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets

from ...upload import UploadJobList
from ...settings import SettingsFile

from .dlg_upload import UploadDialog


class UploadWidget(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        """Manage running uploads
        """
        super(UploadWidget, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcor_manager.gui.upload", "widget_upload.ui")
        uic.loadUi(path_ui, self)

        self.toolButton_new_upload.clicked.connect(self.on_draft_upload)
        self._upload_dialogs = []

        # Underlying upload class
        settings = SettingsFile()
        self.jobs = UploadJobList(server=settings.get_string("server"),
                                  api_key=settings.get_string("api key"))
        self.widget_jobs.set_job_list(self.jobs)

    @QtCore.pyqtSlot()
    def on_draft_upload(self):
        dlg = UploadDialog(self)
        dlg.finished.connect(self.on_run_upload)
        self._upload_dialogs.append(dlg)
        dlg.show()

    @QtCore.pyqtSlot(object)
    def on_run_upload(self, upload_dialog):
        files = upload_dialog.get_file_list()
        dataset_id = upload_dialog.dataset_id
        # add the entry to the job list
        self.jobs.add_job(dataset_id, files)


class UploadTableWidget(QtWidgets.QTableWidget):
    pass
    # TODO: visualize uploads

    def set_job_list(self, jobs):
        # This is the actual initialization
        self.jobs = jobs
