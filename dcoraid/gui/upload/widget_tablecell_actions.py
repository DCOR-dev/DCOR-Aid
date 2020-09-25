import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets


class TableCellActions(QtWidgets.QWidget):
    upload_finished = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Actions in a table cell"""
        super(TableCellActions, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "widget_tablecell_actions.ui")
        uic.loadUi(path_ui, self)
