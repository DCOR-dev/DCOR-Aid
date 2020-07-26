from collections import OrderedDict
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
        # trigger user selection change signal
        self.listWidget.itemSelectionChanged.connect(self.on_item_selected)

    def get_item_keys(self, selected=False):
        """Return the keys of the dictionary items"""
        if selected:
            keys = []
            for ii, key in enumerate(self.item_dict):
                if self.listWidget.item(ii).isSelected():
                    keys.append(key)
        else:
            keys = list(self.item_dict.keys())
        return keys

    @QtCore.pyqtSlot()
    def on_item_selected(self):
        keys = self.get_item_keys(selected=True)
        self.selection_changed.emit(keys)

    def select_item_keys(self, keys):
        """Select entries in `keys`, all other entries are deselected"""
        for ii, key in enumerate(self.item_dict):
            if key in keys:
                self.listWidget.item(ii).setSelected(True)
            else:
                self.listWidget.item(ii).setSelected(False)

    def set_items(self, item_dict):
        """Set the current list items

        Parameters
        ----------
        item_dict: dict
            A dictionary {"key": "displayed text", ...} of the
            items to be displayed.
        """
        if not isinstance(item_dict, dict):
            raise ValueError(
                "`item_dict` must be dict, got '{}'!".fromat(item_dict))
        self.item_dict = OrderedDict(item_dict)
        self.listWidget.clear()
        for key in self.item_dict:
            self.listWidget.addItem(self.item_dict[key])
