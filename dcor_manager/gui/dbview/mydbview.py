import pkg_resources

from PyQt5 import uic, QtWidgets


class MyDBView(QtWidgets.QWidget):
    def __init__(self, *args, **kwargs):
        """Filter view widget with title, edit, checkbox, and table
        """
        super(MyDBView, self).__init__(*args, **kwargs)
        QtWidgets.QMainWindow.__init__(self)
        path_ui = pkg_resources.resource_filename("dcor_manager.gui.dbview",
                                                  "mydbview.ui")
        uic.loadUi(path_ui, self)
