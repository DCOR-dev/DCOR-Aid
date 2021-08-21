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

    def get_entry_identifiers(self, selected=False):
        """Return the identifiers of the current tableWidget entries"""
        if selected:
            identifiers = []
            for ii, entry in enumerate(self.entries):
                if self.tableWidget.item(ii, 0).isSelected():
                    identifiers.append(entry["identifier"])
        else:
            identifiers = [ee["identifier"] for ee in self.entries]
        return identifiers

    @QtCore.pyqtSlot()
    def on_entry_selected(self):
        ids = self.get_entry_identifiers(selected=True)
        self.selection_changed.emit(ids)

    def set_entries(self, entries):
        """Set the current tableWidget entries

        Parameters
        ----------
        entries: list of dict
            List of entries. Each entry is a dictionary with the keys:

            - "identifier": identifier of the entry
            - "text": text of the entry
            - "tools": list of dictionaries for the tool buttons displayed
               on the right of each row. Each of the dictionaries contains
               the keys:
               - action: callable function that is executed
               - icon: icon to be displayed
               - tooltip: tooltip to be displayed
        """
        if not isinstance(entries, list):
            raise ValueError(f"`entries` must be list, got '{entries}'!")
        self.tableWidget.clear()
        self.entries = copy.deepcopy(entries)
        self.tableWidget.blockSignals(True)
        self.tableWidget.setRowCount(len(self.entries))
        for ii, entry in enumerate(self.entries):
            item = QtWidgets.QTableWidgetItem()
            item.setText(self.entries[ii]["text"])
            self.tableWidget.setItem(ii, 0, item)
        self.tableWidget.blockSignals(False)
