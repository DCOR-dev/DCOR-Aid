from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import Qt


class DragTableWidget(QtWidgets.QTableWidget):
    def mouseMoveEvent(self, e):
        if e.buttons() != Qt.LeftButton:
            return

        urls = []
        for item in self.selectedItems():
            data = item.data(Qt.UserRole + 1)
            urls.append(QtCore.QUrl(data))

        mime_data = QtCore.QMimeData()
        mime_data.setUrls(urls)

        drag = QtGui.QDrag(self)
        drag.setMimeData(mime_data)
        drag.setHotSpot(e.pos() - self.rect().topLeft())

        # This magic is somehow required to get drag working.
        dropAction = drag.exec_(Qt.CopyAction)  # noqa: F841
