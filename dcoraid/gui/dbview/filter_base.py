import copy
from importlib import resources

from PyQt6 import QtCore, QtGui, QtWidgets, uic


class FilterBase(QtWidgets.QWidget):
    #: The user selection has changed
    selection_changed = QtCore.pyqtSignal(list)
    #: Download something (which, id_or_name, condensed)
    #: - `which` is e.g. "resource", "dataset", "collection", circle
    #: - `id_or_name` is the identifier or name of the thing
    #: - `condensed` determines whether to download condensed resources only
    download_item = QtCore.pyqtSignal(str, str, bool)
    #: Share something (collection, dataset) with another user.
    #: - `which` is "dataset" or "collection"
    #: - `id_or_name` is the identifier or name of the thing
    share_item = QtCore.pyqtSignal(str, str)
    #: Signal used for editing an item
    edit_item = QtCore.pyqtSignal(str, str)
    #: Signal for removing an item from a list
    remove_item = QtCore.pyqtSignal(str, str)

    def __init__(self, *args, **kwargs):
        """Filter view widget with title, edit, checkbox, and table
        """
        super(FilterBase, self).__init__(*args, **kwargs)
        # to be populated by subclasses
        self.active_actions = []
        ref_ui = resources.files("dcoraid.gui.dbview") / "filter_base.ui"
        with resources.as_file(ref_ui) as path_ui:
            uic.loadUi(path_ui, self)

        #: List of entries in the current list
        self.entries = []

        # resize first column
        header = self.tableWidget.horizontalHeader()
        header.setSectionResizeMode(0,
                                    QtWidgets.QHeaderView.ResizeMode.Stretch)

        # trigger user selection change signal
        self.tableWidget.itemSelectionChanged.connect(self.on_entry_selected)

        # TODO: enable quick-filters via lineEdit
        self.lineEdit.setVisible(False)

        # Disable custom tool button
        self.pushButton_custom.setVisible(False)

        # default drag&drop behavior is "off"
        self.tableWidget.setDropIndicatorShown(False)  # don't show indicator
        self.tableWidget.setDragEnabled(False)  # disable drag
        self.tableWidget.setDragDropOverwriteMode(False)  # don't overwrite
        self.tableWidget.setDragDropMode(
            # drag & drop disabled
            QtWidgets.QAbstractItemView.DragDropMode.NoDragDrop)
        self.tableWidget.setDefaultDropAction(
            QtCore.Qt.DropAction.IgnoreAction)  # no drop by default
        self.tableWidget.horizontalHeader().setSectionResizeMode(
            0, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.tableWidget.horizontalHeader().setSectionResizeMode(
            1, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)

    def get_entry_actions(self, row, entry):
        """This is defined in the subclasses (Circle, Collection, etc)"""
        return []

    def get_entry_identifiers(self, selected=False, which="id"):
        """Return the identifiers of the current tableWidget entries"""
        if selected:
            identifiers = []
            for ii, entry in enumerate(self.entries):
                if self.tableWidget.item(ii, 0).isSelected():
                    identifiers.append(entry[which])
        else:
            identifiers = [ee[which] for ee in self.entries]
        return identifiers

    @QtCore.pyqtSlot()
    def on_entry_selected(self):
        ids = self.get_entry_identifiers(selected=True)
        self.selection_changed.emit(ids)

    def set_entries(self, entries):
        """Set the current tableWidget entries

        """
        if not isinstance(entries, list):
            raise ValueError(f"`entries` must be list, got '{entries}'!")
        self.tableWidget.clear()
        self.entries = copy.deepcopy(entries)
        self.tableWidget.blockSignals(True)
        self.tableWidget.setRowCount(len(self.entries))
        for row, entry in enumerate(self.entries):
            self.set_entry(row, entry)
        self.tableWidget.blockSignals(False)

    def set_entry(self, row, entry):
        """Set table Widget entry at index `row`"""
        # text (1st column)
        self.set_entry_label(row, entry)

        # tool buttons (2nd column)
        widact = QtWidgets.QWidget(self)
        horz_layout = QtWidgets.QHBoxLayout(widact)
        horz_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight)
        horz_layout.setSpacing(2)
        horz_layout.setContentsMargins(0, 0, 2, 0)

        for action in self.get_entry_actions(row, entry):
            if action["name"] in self.active_actions:
                tbact = QtWidgets.QToolButton(widact)
                icon = QtGui.QIcon.fromTheme(action["icon"])
                tbact.setIcon(icon)
                tbact.setToolTip(action["tooltip"])
                tbact.clicked.connect(action["function"])
                horz_layout.addWidget(tbact)
                row_height = tbact.geometry().height()
                tbact.setFixedSize(row_height - 2, row_height - 2)

        self.tableWidget.setCellWidget(row, 1, widact)

        return widact

    def set_entry_label(self, row, entry):
        """Set table Widget entry text at index `row`

        Parameters
        ----------
        row: int
            row where to put the entry
        entry: dict
            CKAN entry dictionary
        """
        item = QtWidgets.QTableWidgetItem()
        item.setText(entry.get("title") or entry["name"])
        item.setToolTip(entry.get("title") or entry["name"])
        item.setStatusTip(entry.get("title") or entry["name"])
        self.tableWidget.setItem(row, 0, item)
