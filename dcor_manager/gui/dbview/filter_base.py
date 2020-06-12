import pkg_resources

from PyQt5 import uic, QtWidgets


class FilterBase(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        """Filter view widget with title, edit, checkbox, and table
        """
        super(FilterBase, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename("dcor_manager.gui.dbview",
                                                  "filter_base.ui")
        uic.loadUi(path_ui, self)
