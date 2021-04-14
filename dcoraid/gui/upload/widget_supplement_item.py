import pkg_resources

from PyQt5 import uic, QtCore, QtWidgets


class TitleItem(QtWidgets.QWidget):
    def __init__(self, requires, title, *args, **kwargs):
        """A schema widget for the section titles

        Parameters
        ----------
        requires: dict
            Requirements dictionary
        title: str
            Section title
        """
        super(TitleItem, self).__init__(*args, **kwargs)

        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)

        self.label = QtWidgets.QLabel("<b>{}</b>".format(title))
        self.verticalLayout.addWidget(self.label)

        self.requires = requires

    @QtCore.pyqtSlot(str, str, object, bool)
    def on_schema_key_toggled(self, section, key, value, enabled):
        """Hide or show this widget based on self.requires"""
        if not self.requires:
            self.setVisible(True)
        elif section in self.requires and key in self.requires[section]:
            if value in self.requires[section][key]:
                self.setVisible(enabled)
            else:
                self.setVisible(False)


class RSSItemBase(QtWidgets.QWidget):
    value_changed = QtCore.pyqtSignal(str, str, object, bool)

    def __init__(self, rss_dict, section, *args, **kwargs):
        super(RSSItemBase, self).__init__(*args, **kwargs)
        self.rss_dict = rss_dict
        self.section = section
        self.key = rss_dict["key"]
        self.requires = rss_dict.get("requires", {})

    def emit_value(self):
        """Emit `value_changed` with current data"""
        self.value_changed.emit(self.section, self.key, self.get_value(),
                                self.checkBox.isChecked())

    @QtCore.pyqtSlot()
    def on_value_changed(self):
        """Activate checkbox and call `emit_value`"""
        if self.sender() == self.checkBox and not self.checkBox.isChecked():
            # Do not check the checkBox again if the user unchecks it
            pass
        else:
            self.check(True)
        self.emit_value()

    @QtCore.pyqtSlot(str, str, object, bool)
    def on_schema_key_toggled(self, section, key, value, enabled):
        """Hide or show this widget based on self.rss_dict["requires"]"""
        if not self.requires:
            visible = True
        elif section in self.requires and key in self.requires[section]:
            if value in self.requires[section][key]:
                visible = enabled
            else:
                visible = False
        else:
            visible = self.isVisible()
        self.setVisible(visible)
        if not visible:
            # uncheck the widget if it is hidden
            self.checkBox.setChecked(False)


class RSSTagsItem(RSSItemBase):
    def __init__(self, *args, **kwargs):
        """Represents an item in the supplementary resource schema"""
        super(RSSTagsItem, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "widget_supplement_tags.ui")
        uic.loadUi(path_ui, self)
        self.tableWidget.setRowCount(3)
        self.on_assert_row_count()

        # signals
        self.tableWidget.itemChanged.connect(self.on_value_changed)
        self.tableWidget.itemChanged.connect(self.on_assert_row_count)

    def check(self, b):
        """Check the check box"""
        self.checkBox.setChecked(b)

    def get_compounds(self):
        """Get the compounds labels (1st column)"""
        return " ".join([it[0] for it in self.get_value() if it]).strip()

    def get_labels(self):
        """Get the fluorescence labels (2nd column)"""
        return " ".join([it[1] for it in self.get_value() if it]).strip()

    def get_tags(self):
        """Return the tags (antibodies and fluorphores connected with dash)"""
        tags = []
        for it in self.get_value():
            if it[0] and it[1]:
                tags.append("{}-{}".format(*it))
        return " ".join(tags).strip()

    def get_value(self):
        """Return the items of the QTableWidget

        This is not used by DCOR, but only internally by DCOR-Aid.
        """
        self.on_assert_row_count()
        value = []
        for ii in range(self.tableWidget.rowCount()):
            it0 = self.tableWidget.item(ii, 0).text().strip()
            it1 = self.tableWidget.item(ii, 1).text().strip()
            if it0 or it1:
                value.append((it0, it1))
        return value

    def set_value(self, value):
        """Set the value"""
        self.on_assert_row_count()
        self.tableWidget.setRowCount(len(value))
        for ii, it in enumerate(value):
            self.tableWidget.item(ii, 0).setText(it[0])
            self.tableWidget.item(ii, 1).setText(it[1])

    @QtCore.pyqtSlot()
    def on_assert_row_count(self):
        """Increase row count by one if last item has data"""
        nrow = self.tableWidget.rowCount()
        it0 = self.tableWidget.item(nrow-1, 0)
        it1 = self.tableWidget.item(nrow-1, 1)
        if (it0 and it0.text().strip() or it1 and it1.text().strip()):
            nrow += 1
            self.tableWidget.setRowCount(nrow)
        # populate empty items
        for ii in range(nrow):
            for jj in [0, 1]:
                it = self.tableWidget.item(ii, jj)
                if it is None:
                    self.tableWidget.setItem(ii, jj,
                                             QtWidgets.QTableWidgetItem())


class RSSItem(RSSItemBase):
    def __init__(self, *args, **kwargs):
        """Represents an item in the supplementary resource schema"""
        super(RSSItem, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "widget_supplement_item.ui")
        uic.loadUi(path_ui, self)
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
            if editable:
                self.comboBox.setToolTip("You may also type your own choice.")
        else:
            example = rss_dict.get("example", None)
            if example and hasattr(widget, "setPlaceholderText"):
                widget.setPlaceholderText("e.g. {}".format(example))
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
            elif itemtype == "date":
                widget = self.lineEdit
                value = self.lineEdit.text()
            else:
                raise ValueError("No rule to process item {}".format(rss_dict))
        self.blockSignals(False)
        if retval:
            return widget, value
        else:
            return widget

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
