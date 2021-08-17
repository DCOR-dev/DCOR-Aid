import pathlib
import pkg_resources
import traceback as tb

from PyQt5 import uic, QtCore, QtGui, QtWidgets

from ..tools import ShowWaitCursor
from ...upload import create_dataset

from ..api import get_ckan_api

from . import circle_mgr
from .resources_model import ResourcesModel
from .resource_schema_preset import PersistentResourceSchemaPresets


class NoCircleSelectedError(ValueError):
    """Used to tell UploadWidget that the user did not select a circle"""
    pass


class UploadDialog(QtWidgets.QDialog):
    finished = QtCore.pyqtSignal(object)
    instance_counter = 1

    def __init__(self, parent=None):
        """Create a new window for setting up a file upload
        """
        super(UploadDialog, self).__init__(parent)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "dlg_upload.ui")
        uic.loadUi(path_ui, self)

        # Dialog box buttons
        self.pushButton_proceed = self.buttonBox.button(
            QtWidgets.QDialogButtonBox.Apply)
        self.pushButton_proceed.setText("Proceed with upload / Enqueue job")
        self.pushButton_cancel = self.buttonBox.button(
            QtWidgets.QDialogButtonBox.Cancel)

        # Keep identifier
        self.identifier = self.instance_counter
        UploadDialog.instance_counter += 1

        #: This will be overridden with the actual value assigned
        #: by DCOR. It is important for later uploading the resources.
        self.dataset_id = None

        self.setWindowTitle("DCOR Upload {}".format(self.identifier))

        # Initialize api
        self.api = get_ckan_api()

        # Set circle choices
        circles = self.get_user_circle_dicts()
        for ci in circles:
            self.comboBox_circles.addItem(
                ci["title"] if ci["title"] else ci["name"], ci["name"])

        with ShowWaitCursor():
            # Set license choices
            licenses = self.api.get_license_list()
            for lic in licenses:
                if lic["domain_data"]:  # just a identifier to exclude "none"
                    self.comboBox_license.addItem(
                        "{} ({})".format(lic["title"], lic["id"]), lic["id"])

            # Set supplementary resource schema
            rss = self.api.get_supplementary_resource_schema()
            self.widget_schema.populate_schema(rss)

        # Set visibility choices
        settings = QtCore.QSettings()
        if settings.value("user scenario", "") == "medical":
            # only allow private datasets
            self.comboBox_vis.addItem("Private", "private")
        else:
            self.comboBox_vis.addItem("Public", "public")
            self.comboBox_vis.addItem("Private", "private")

        # Shortcut for testing
        self.shortcut = QtWidgets.QShortcut(
            QtGui.QKeySequence("Ctrl+Alt+Shift+E"), self)
        self.shortcut.activated.connect(self._autofill_for_testing)

        # Setup resources view
        self.rvmodel = ResourcesModel()
        self.listView_resources.setModel(self.rvmodel)
        self.selModel = self.listView_resources.selectionModel()

        # Effectively hide resource schema options initially
        self.on_selection_changed()

        # Presets for user convenience
        self.presets = PersistentResourceSchemaPresets()
        self.comboBox_preset.addItems(sorted(self.presets.keys()))

        # Signals and slots
        # general buttons
        self.toolButton_add.clicked.connect(self.on_add_resources)
        self.toolButton_rem.clicked.connect(self.on_rem_resources)
        self.pushButton_proceed.clicked.connect(self.on_proceed)
        # resource-related signals
        self.lineEdit_res_filename.textChanged.connect(
            self.on_update_resources_model)
        self.widget_schema.schema_changed.connect(
            self.on_update_resources_model)
        self.selModel.selectionChanged.connect(self.on_selection_changed)
        # do not allow to proceed without a title
        self.lineEdit_authors.textChanged.connect(self.on_authors_edited)
        self.on_authors_edited("")  # initial state
        # if the user changes the preset combobox text, offer him
        # to save it as a preset
        self.comboBox_preset.editTextChanged.connect(
            self.on_preset_text_edited)
        # also offer the user to save the preset if the schema changed
        self.widget_schema.schema_changed.connect(
            self.on_preset_text_edited)
        self.on_preset_text_edited()  # empty string means no preset
        # store/load a preset
        self.toolButton_preset_load.clicked.connect(self.on_preset_load)
        self.toolButton_preset_store.clicked.connect(self.on_preset_store)

        # Do not allow drag and drop to line edit of combobox
        self.comboBox_preset.lineEdit().setAcceptDrops(False)

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
        self.comboBox_vis.setCurrentIndex(0)
        relpath = "../../../tests/data/calibration_beads_47.rtdc"
        path = pathlib.Path(__file__).resolve().parent / relpath
        if path.exists():
            self.on_add_resources([str(path.resolve())])

    def dragEnterEvent(self, e):
        """Whether files are accepted"""
        if e.mimeData().hasUrls():
            e.accept()
        else:
            e.ignore()

    def dropEvent(self, e):
        """Add dropped files to view"""
        urls = e.mimeData().urls()
        pathlist = []
        for ff in urls:
            pp = pathlib.Path(ff.toLocalFile())
            if pp.is_dir():
                pathlist += list(pp.rglob("*.rtdc"))
            else:
                pathlist.append(pp)
        if pathlist:
            self.on_add_resources(files=sorted(pathlist))

    def get_user_circle_dicts(self):
        with ShowWaitCursor():
            circles = circle_mgr.get_user_circle_dicts()
        if not circles:
            cdict = circle_mgr.ask_for_new_circle(self)
            if cdict:
                circles.append(cdict)
            else:
                # abort here
                raise NoCircleSelectedError("User did not specify a circle!")
        return circles

    def assemble_metadata(self):
        """Get all the metadata from the form"""
        # Dataset
        tags = []
        for tt in self.lineEdit_tags.text().replace(" ", "").split(","):
            if tt:
                tags.append({"name": tt})

        dataset_dict = {
            "title": self.lineEdit_title.text(),
            "authors": self.lineEdit_authors.text(),
            "doi": self.lineEdit_doi.text(),
            "references": self.lineEdit_references.text(),
            "tags": tags,
            "notes": self.plainTextEdit_notes.toPlainText(),
            "license_id": self.comboBox_license.currentData(),
            "private": self.comboBox_vis.currentData() == "private",
            "owner_org": self.comboBox_circles.currentData(),
        }
        return dataset_dict

    @QtCore.pyqtSlot(str)
    def on_authors_edited(self, newtext):
        """Enable proceed button if authors text is set"""
        if newtext:
            enabled = True
            tooltip = "Proceed with upload"
        else:
            enabled = False
            tooltip = "Set 'Authors' to proceed with upload"
        self.pushButton_proceed.setEnabled(enabled)
        self.pushButton_proceed.setToolTip(tooltip)

    @QtCore.pyqtSlot()
    def on_add_resources(self, files=None):
        """Ask the user to specify files to add"""
        if files is None:
            suffixes = self.api.get_supported_resource_suffixes()
            files, _ = QtWidgets.QFileDialog.getOpenFileNames(
                self, "Resources to upload", ".",
                "Supported file types ({})".format(
                    " ".join(["*{}".format(s) for s in suffixes])))
        files = [str(ff) for ff in files]  # make sure files are strings
        self.rvmodel.add_resources(files)
        if not self.listView_resources.selectedIndexes():
            # Select the first item
            ix = self.rvmodel.index(0, 0)
            sm = self.listView_resources.selectionModel()
            sm.select(ix, QtCore.QItemSelectionModel.Select)

    @QtCore.pyqtSlot()
    def on_preset_load(self):
        """Load the preset with the current name"""
        curtext = self.comboBox_preset.currentText()

        # Load from presets (must come before UI logic)
        self.widget_schema.set_schema(self.presets[curtext])

        # UI logic
        self.toolButton_preset_store.setEnabled(False)
        self.toolButton_preset_load.setEnabled(False)

    @QtCore.pyqtSlot()
    def on_preset_store(self):
        """Store the current preset"""
        curtext = self.comboBox_preset.currentText()

        # Store preset
        preset = self.widget_schema.get_current_schema()
        self.presets[curtext] = preset
        self.on_update_resources_model()

        # UI logic
        items = []
        for ii in range(self.comboBox_preset.count()):
            items.append(self.comboBox_preset.itemText(ii))
        items = sorted(set(items + [curtext]))
        self.comboBox_preset.blockSignals(True)
        self.comboBox_preset.clear()
        self.comboBox_preset.addItems(items)
        self.comboBox_preset.setCurrentIndex(items.index(curtext))
        self.comboBox_preset.blockSignals(False)
        self.toolButton_preset_store.setEnabled(False)
        self.toolButton_preset_load.setEnabled(False)

    @QtCore.pyqtSlot()
    def on_preset_text_edited(self):
        """The preset combobox text was edited by the user

        Give or take a away from the user the option to load or save
        a preset.
        """
        # UI logic only
        curtext = self.comboBox_preset.currentText()
        if curtext:
            self.toolButton_preset_store.setEnabled(True)
        else:
            self.toolButton_preset_store.setEnabled(False)
        if curtext in self.presets:
            self.toolButton_preset_load.setEnabled(True)
        else:
            self.toolButton_preset_load.setEnabled(False)

    @QtCore.pyqtSlot()
    def on_proceed(self):
        """User is done and clicked the proceed button

        This will first trigger a creation of the draft dataset
        on DCOR. Then, the job is enqueued in the parent
        """
        # We should only proceed if we have resources
        if self.rvmodel.rowCount() == 0:
            QtWidgets.QMessageBox.critical(self, "No resources selected",
                                           "Please add at least one resource.")
            return
        # Checking for duplicate resources is the responsibility of
        # DCOR-Aid, because we are skipping existing resources in
        # dcoraid.upload.job.UploadJob.task_upload_resources.
        if not self.rvmodel.filenames_are_unique():
            QtWidgets.QMessageBox.critical(
                self,
                "Resource names not unique",
                "Please make sure that all resources have a unique file name. "
                + "Resources with identical file names are not supported by "
                + "DCOR.")
            return
        if not self.rvmodel.filenames_were_edited():
            choice = QtWidgets.QMessageBox.question(
                self,
                "Only raw file names",
                "You did not rename any of the resource file names. Was this "
                + "intentional? Using generic file names is discouraged. "
                + "Please select 'No' if you would like to change things. "
                + "Select 'Yes' to proceed without changes - be aware that "
                + "you will not be able to change anything later on.")
            if choice != QtWidgets.QMessageBox.Yes:
                return
        if not self.rvmodel.supplements_were_edited():
            choice = QtWidgets.QMessageBox.question(
                self,
                "No supplementary resource metadata",
                "You did not specify any supplementary resource metadata. "
                + "Would you like to proceed anyway? The editing options "
                + "appear when you click on a resource. Please select 'No' "
                + "if you would like to go back (recommended). Select 'Yes' "
                + "to proceed without changes (no future changes possible).")
            if choice != QtWidgets.QMessageBox.Yes:
                return

        # Try to create the dataset and display any issues with the metadata
        try:
            data = create_dataset(dataset_dict=self.assemble_metadata(),
                                  api=self.api.copy())
        except BaseException:
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Critical)
            msg.setText("It was not possible to create a dataset draft. "
                        "If this is not a connection problem, please consider "
                        "creating an <a href='"
                        "https://github.com/DCOR-dev/DCOR-Aid/issues"
                        "'>issue on GitHub</a>.")
            msg.setWindowTitle("Dataset creation failed")
            msg.setDetailedText(tb.format_exc())
            msg.exec_()
            return
        self.setHidden(True)
        # Remember the dataset identifier
        self.dataset_dict = data
        self.dataset_id = data["id"]
        # signal that we are clear to proceed
        self.finished.emit(self)
        self.close()

    @QtCore.pyqtSlot()
    def on_rem_resources(self):
        """Remove the selected resources"""
        sel = self.listView_resources.selectedIndexes()
        self.rvmodel.rem_resources(sel)
        self.listView_resources.clearSelection()

    @QtCore.pyqtSlot()
    def on_selection_changed(self):
        """User changed the ListView selection; Refresh side panel"""
        sel = self.listView_resources.selectedIndexes()
        seltype = self.rvmodel.get_indexes_types(sel)
        # Resource options
        if len(sel) == 1:  # a single resource; show resource options
            self.groupBox_res_info.show()
            # populate
            path, data = self.rvmodel.get_data_for_index(sel[0])
            self.lineEdit_res_filename.blockSignals(True)
            self.lineEdit_res_filename.setText(data["file"]["filename"])
            self.lineEdit_res_filename.blockSignals(False)
            self.lineEdit_res_path.setText(path)
        else:  # hide resource options
            self.groupBox_res_info.hide()
        # Supplement options
        if seltype in ["dc", "mixed"]:
            self.widget_supplement.show()
            # populate
            common = self.rvmodel.get_common_supplements_from_indexes(sel)
            self.widget_schema.set_schema(common)
        else:
            self.widget_supplement.hide()

        # also reset the preset combo box string
        self.comboBox_preset.setCurrentText("")

    @QtCore.pyqtSlot()
    def on_update_resources_model(self):
        """Assemble metadata dictionary and update self.rvmodel"""
        data_dict = {}
        sel = self.listView_resources.selectedIndexes()
        if len(sel) == 0:  # nothing to do
            return
        elif len(sel) == 1:  # update file name
            path = pathlib.Path(self.rvmodel.get_data_for_index(sel[0])[0])
            suffix = path.suffix
            # prevent users from changing the suffix
            fn = self.lineEdit_res_filename.text()
            if not fn.endswith(suffix):
                fn += suffix
                self.lineEdit_res_filename.blockSignals(True)
                self.lineEdit_res_filename.setText(fn)
                self.lineEdit_res_filename.blockSignals(False)
            if fn != path.name:  # only update filename if user changed it
                data_dict["file"] = {"filename": fn}
        # collect supplementary resource data
        schema = self.widget_schema.get_current_schema()
        if schema:  # only update supplement if user made changes
            data_dict["supplement"] = schema
        self.rvmodel.update_resources(sel, data_dict)
