import pkg_resources

from PyQt5 import uic, QtWidgets


class FilterChain(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        """Filter chain widget with multiple filter views
        """
        super(FilterChain, self).__init__(*args, **kwargs)
        QtWidgets.QMainWindow.__init__(self)
        path_ui = pkg_resources.resource_filename("dcor_manager.gui.dbview",
                                                  "filter_chain.ui")
        uic.loadUi(path_ui, self)
