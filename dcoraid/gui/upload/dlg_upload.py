import pathlib
import pkg_resources

from PyQt5 import uic, QtCore, QtGui, QtWidgets

from ...api import CKANAPI
from ...upload import create_dataset
from ...settings import SettingsFile
from functools import lru_cache


class UploadDialog(QtWidgets.QMainWindow):
    finished = QtCore.pyqtSignal(object)
    instance_counter = 1

    def __init__(self, parent=None):
        """Create a new window for setting up a file upload
        """
        super(UploadDialog, self).__init__(parent)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "dlg_upload.ui")
        uic.loadUi(path_ui, self)

        # Keep identifier
        self.identifier = self.instance_counter
        UploadDialog.instance_counter += 1

        #: This will be overridden with the actual value assigned
        #: by DCOR. It is important for later uploading the resources.
        self.dataset_id = None

        self.setWindowTitle("DCOR Upload {}".format(self.identifier))

        # Set license choices
        licenses = [
            ("CC0-1.0", "Creative Commons Public Domain Dedication"),
            ("CC-BY-4.0", "Creative Commons Attribution 4.0"),
            ("CC-BY-SA-4.0", "Creative Commons Attribution Share-Alike 4.0"),
            ("CC-BY-NC-4.0", "Creative Commons Attribution-NonCommercial 4.0"),
        ]
        for key, title in licenses:
            self.comboBox_license.addItem(title, key)

        # Set circle choices
        circles = UploadDialog.get_user_circle_dicts()
        for ci in circles:
            self.comboBox_circles.addItem(ci["title"], ci["name"])

        # Set visibility choices
        self.comboBox_vis.addItem("Public", "public")
        self.comboBox_vis.addItem("Private", "private")

        # Shortcut for testing
        self.shortcut = QtWidgets.QShortcut(
            QtGui.QKeySequence("Ctrl+Alt+Shift+T"), self)
        self.shortcut.activated.connect(self._autofill_for_testing)

        # signal slots
        self.toolButton_add.clicked.connect(self.on_add_resources)
        self.toolButton_rem.clicked.connect(self.on_rem_resources)
        self.pushButton_proceed.clicked.connect(self.on_proceed)

    def _autofill_for_testing(self, **kwargs):
        self.lineEdit_title.setText(kwargs.get("title", "Dataset Title"))
        self.lineEdit_authors.setText(kwargs.get("authors", "John Doe"))
        self.lineEdit_doi.setText(kwargs.get("doi", ""))
        self.lineEdit_references.setText(kwargs.get("references", ""))
        self.plainTextEdit_notes.setPlainText(
            kwargs.get("notes", "A description"))
        self.lineEdit_tags.setText(kwargs.get("tags", "HL60, GFP"))
        licen = "CC-BY-SA-4.0"
        self.comboBox_license.setCurrentIndex(
            self.comboBox_license.findData(licen))
        self.comboBox_circles.setCurrentIndex(0)
        self.comboBox_vis.setCurrentIndex(1)
        relpath = "../../../tests/data/calibration_beads_47.rtdc"
        path = pathlib.Path(__file__).resolve().parent / relpath
        if path.exists():
            self.listWidget_resources.addItem(str(path.resolve()))

    @classmethod
    @lru_cache(maxsize=1)
    def get_user_circle_dicts(cls):
        settings = SettingsFile()
        api = CKANAPI(server=settings.get_string("server"),
                      api_key=settings.get_string("api key"))
        circles = api.get("organization_list_for_user",
                          permission="create_dataset")
        return circles

    @QtCore.pyqtSlot()
    def on_add_resources(self):
        """Ask the user to specify files to add"""
        files, _ = QtWidgets.QFileDialog.getOpenFileNames(
            self, "Resources to upload", ".", "")
        for ff in files:
            self.listWidget_resources.addItem(ff)

    @QtCore.pyqtSlot()
    def on_rem_resources(self):
        """Remove the selected resources"""
        sel = self.listWidget_resources.selectedItems()
        for item in sel:
            row = self.listWidget_resources.row(item)
            self.listWidget_resources.takeItem(row)

    @QtCore.pyqtSlot()
    def on_proceed(self):
        """User is done and clicked the proceed button

        This will first trigger a creation of the draft dataset
        on DCOR. Then, the job is enqueued in the parent
        """
        self.setHidden(True)
        # Initiate API
        settings = SettingsFile()
        # Try to create the dataset and display any issues with the metadata
        data = create_dataset(dataset_dict=self.assemble_metadata(),
                              server=settings.get_string("server"),
                              api_key=settings.get_string("api key")
                              )
        # Remember the dataset identifier
        self.dataset_dict = data
        self.dataset_id = data["id"]
        # signal that we are clear to proceed
        self.finished.emit(self)
        self.close()

    def assemble_metadata(self):
        """Get all the metadata from the form"""
        tags = []
        for tt in self.lineEdit_tags.text().replace(" ", "").split(","):
            tags.append({"name": tt})

        dataset_dict = {
            "title": self.lineEdit_title.text(),
            "authors": self.lineEdit_authors.text(),
            "doi": self.lineEdit_doi.text(),
            "references": self.lineEdit_references.text(),
            "tags": tags,
            "notes": self.plainTextEdit_notes.toPlainText(),
            "license_id": self.comboBox_license.currentData(),
            "visibility": self.comboBox_vis.currentData(),
            "owner_org": self.comboBox_circles.currentData(),
        }
        return dataset_dict

    def get_file_list(self):
        """Return the paths of the files to be uploaded"""
        files = []
        for ii in range(self.listWidget_resources.count()):
            files.append(
                pathlib.Path(self.listWidget_resources.item(ii).text()))
        return files
