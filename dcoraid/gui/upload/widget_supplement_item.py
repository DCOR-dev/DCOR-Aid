import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets


class RSSItem(QtWidgets.QWidget):
    value_changed = QtCore.pyqtSignal()

    def __init__(self, rss_dict, *args, **kwargs):
        """Represents an item in the supplementary resource schema"""
        super(RSSItem, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "widget_supplement_item.ui")
        uic.loadUi(path_ui, self)
        self.rss_dict = rss_dict
        self.apply_schema()

        # signals
        self.checkBox.clicked.connect(self.on_value_changed)
        self.spinBox.valueChanged.connect(self.on_value_changed)
        self.doubleSpinBox.valueChanged.connect(self.on_value_changed)
        self.lineEdit.textChanged.connect(self.on_value_changed)
        self.plainTextEdit.textChanged.connect(self.on_value_changed)
        self.comboBox.currentTextChanged.connect(self.on_value_changed)
        self.radioButton_yes.clicked.connect(self.on_value_changed)
        self.radioButton_no.clicked.connect(self.on_value_changed)

    def apply_schema(self):
        """Initialize the item schema according to self.rss_dict"""
        rss_dict = self.rss_dict
        self.checkBox.setText(rss_dict["name"])
        self.checkBox.setToolTip(rss_dict.get("hint", None))

        widget = self.get_data_widget()
        self.show_only_data_widget()
        if "choices" in rss_dict:  # combobox
            assert widget is self.comboBox
            self.comboBox.insertItems(0, rss_dict["choices"])
            editable = "choices fixed" not in rss_dict.get("options", [])
            self.comboBox.setEditable(editable)
        else:
            example = rss_dict.get("example", None)
            if example:
                widget.setToolTip("e.g. {}".format(example))
        if "unit" in rss_dict:
            self.doubleSpinBox.setSuffix(" " + rss_dict["unit"])

    def check(self, b):
        """Check the check box"""
        self.checkBox.setChecked(b)

    def get_value(self):
        """Return the value of the current data widget"""
        _, value = self.get_data_widget(retval=True)
        return value

    def set_value(self, value):
        """Set the widget value with the appropriate function"""
        widget = self.get_data_widget()
        if widget is self.lineEdit:
            self.lineEdit.setText(value)
        elif widget is self.comboBox:
            self.comboBox.setCurrentText(value)
        elif widget is self.spinBox or widget is self.doubleSpinBox:
            widget.setValue(value)
        elif widget is self.plainTextEdit:
            widget.setPlainText(value)
        elif widget is self.widget_bool:
            if value:
                self.radioButton_yes.setChecked(True)
            else:
                self.radioButton_no.setChecked(True)

    def get_data_widget(self, retval=False):
        """Return the widget the holds the data according to self.rss_dict"""
        self.blockSignals(True)
        rss_dict = self.rss_dict
        if "choices" in rss_dict:  # combobox
            widget = self.comboBox
            value = self.comboBox.currentText()
        else:
            itemtype = rss_dict.get("type", "string")
            if itemtype == "string":
                if "text" in rss_dict.get("options", []):
                    widget = self.plainTextEdit
                    value = self.plainTextEdit.toPlainText()
                else:
                    widget = self.lineEdit
                    value = self.lineEdit.text()
            elif itemtype == "list":
                widget = self.lineEdit
                value = self.lineEdit.text()
            elif itemtype == "float":
                widget = self.doubleSpinBox
                value = self.doubleSpinBox.value()
            elif itemtype == "integer":
                widget = self.spinBox
                value = self.spinBox.value()
            elif itemtype == "boolean":
                widget = self.widget_bool
                value = self.radioButton_yes.isChecked()
            else:
                raise ValueError("No rule to process item {}".format(rss_dict))
        self.blockSignals(False)
        if retval:
            return widget, value
        else:
            return widget

    @QtCore.pyqtSlot()
    def on_value_changed(self):
        """Activate checkbox and emit value_changed signal"""
        if self.sender() == self.checkBox and not self.checkBox.isChecked():
            # Do not check the checkBox again if the user unchecks it
            pass
        else:
            self.checkBox.setChecked(True)
        self.value_changed.emit()

    def show_only_data_widget(self):
        """Convenience function that hides all but the data widget"""
        widget = self.get_data_widget()
        for ww in [self.comboBox,
                   self.doubleSpinBox,
                   self.lineEdit,
                   self.plainTextEdit,
                   self.spinBox,
                   self.widget_bool]:
            if ww is not widget:
                ww.hide()
