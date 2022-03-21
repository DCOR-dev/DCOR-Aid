import copy
import pkg_resources

from PyQt5 import QtCore, QtGui, QtWidgets, uic


class FilterBase(QtWidgets.QWidget):
    #: The user selection has changed
    selection_changed = QtCore.pyqtSignal(list)
    download_resource = QtCore.pyqtSignal(str, bool)

    def __init__(self, *args, **kwargs):
        """Filter view widget with title, edit, checkbox, and table
        """
        super(FilterBase, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename("dcoraid.gui.dbview",
                                                  "filter_base.ui")
        uic.loadUi(path_ui, self)

        #: List of entries in the current list
        self.entries = []

        # resize first column
        header = self.tableWidget.horizontalHeader()
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)

        # trigger user selection change signal
        self.tableWidget.itemSelectionChanged.connect(self.on_entry_selected)

        # TODO: enable quick-filters via lineEdit
        self.lineEdit.setVisible(False)

        # Disable custom tool button
        self.toolButton_custom.setVisible(False)

        # default drag&drop behavior is "off"
        self.tableWidget.setDropIndicatorShown(False)  # don't show indicator
        self.tableWidget.setDragEnabled(False)  # disable drag
        self.tableWidget.setDragDropOverwriteMode(False)  # don't overwrite
        self.tableWidget.setDragDropMode(
            QtWidgets.QAbstractItemView.NoDragDrop)  # drag & drop disabled
        self.tableWidget.setDefaultDropAction(
            QtCore.Qt.IgnoreAction)  # no drop by default

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
        horz_layout.setContentsMargins(2, 0, 2, 0)

        spacer = QtWidgets.QSpacerItem(0, 0,
                                       QtWidgets.QSizePolicy.Expanding,
                                       QtWidgets.QSizePolicy.Minimum)
        horz_layout.addItem(spacer)

        for action in self.get_entry_actions(row, entry):
            tbact = QtWidgets.QToolButton(widact)
            icon = QtGui.QIcon.fromTheme(action["icon"])
            tbact.setIcon(icon)
            tbact.setToolTip(action["tooltip"])
            tbact.clicked.connect(action["function"])
            horz_layout.addWidget(tbact)
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
        self.tableWidget.setItem(row, 0, item)
