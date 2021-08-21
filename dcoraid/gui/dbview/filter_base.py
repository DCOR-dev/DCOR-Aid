import copy
import pkg_resources

from PyQt5 import QtCore, QtWidgets, uic


class FilterBase(QtWidgets.QWidget):
    #: The user selection has changed
    selection_changed = QtCore.pyqtSignal(list)

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

    def get_entry_actions(self, row, entry):
        """This is defined in the subclasses (Circle, Collection, etc)"""
        return []

    def get_entry_identifiers(self, selected=False):
        """Return the identifiers of the current tableWidget entries"""
        if selected:
            identifiers = []
            for ii, entry in enumerate(self.entries):
                if self.tableWidget.item(ii, 0).isSelected():
                    identifiers.append(entry["name"])
        else:
            identifiers = [ee["name"] for ee in self.entries]
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
        """Set table Widget entry at index `row`

        This function should call `set_entry_text`
        """
        self.set_entry_text(row, entry.get("title") or entry["name"])
        for action in self.get_entry_actions(row, entry):
            pass

    def set_entry_text(self, row, text):
        """Set table Widget entry text at index `row`

        Parameters
        ----------
        row: int
            The row where to put the entry
        text: str
            the text to display on the left
        """
        item = QtWidgets.QTableWidgetItem()
        item.setText(text)
        self.tableWidget.setItem(row, 0, item)
