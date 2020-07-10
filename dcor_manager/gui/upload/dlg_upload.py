import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets


class UploadDialog(QtWidgets.QMainWindow):
    finished = QtCore.pyqtSignal(object)
    instance_counter = 1

    def __init__(self, parent=None):
        """Create a new window for setting up a file upload
        """
        super(UploadDialog, self).__init__(parent)
        path_ui = pkg_resources.resource_filename(
            "dcor_manager.gui.upload", "dlg_upload.ui")
        uic.loadUi(path_ui, self)

        # Keep identifier
        self.identifier = self.instance_counter
        UploadDialog.instance_counter += 1

        #: This will be overridden with the actual value assigned
        #: by DCOR. It is important for later uploading the resources.
        self.dataset_id = None

        self.setWindowTitle("DCOR Upload {}".format(self.identifier))

    @QtCore.pyqtSlot()
    def on_proceed(self):
        """User is done and clicked the proceed button

        This will first trigger a creation of the draft dataset
        on DCOR. Then, the job is enqueued in the parent
        """
        # Get the metadata dictionary

        # Try to create the dataset and display any issues with the metadata

        # Remember the dataset identifier
        self.dataset_id = "my string"

        # signal that we are clear to proceed

        self.finished.emit(self)

    def assemble_metadata(self):
        """Get all the metadata from the form"""
        return {}

    def get_file_list(self):
        """Return the paths of the files to be uploaded"""
        return []
