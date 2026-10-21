from PyQt6 import QtCore, QtWidgets

from .widget_supplement_item_ui import Ui_Form as Ui_WidgetSupplementItem
from .widget_supplement_tags_ui import Ui_Form as Ui_WidgetSupplementTags


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
        super().__init__(*args, **kwargs)

        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)

        self.label = QtWidgets.QLabel(f"<b>{title}</b>")
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
        super().__init__(*args, **kwargs)
        self.rss_dict = rss_dict
        self.section = section
        self.key = rss_dict["key"]
        self.requires = rss_dict.get("requires", {})

    def emit_value(self):
        """Emit `value_changed` with current data"""
        self.value_changed.emit(self.section, self.key, self.get_value(),
                                self.ui.checkBox.isChecked())

    @QtCore.pyqtSlot()
    def on_value_changed(self):
        """Activate checkbox and call `emit_value`"""
        if (self.sender() == self.ui.checkBox
                and not self.ui.checkBox.isChecked()):
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
            self.ui.checkBox.setChecked(False)


class RSSTagsItem(RSSItemBase):
    def __init__(self, *args, **kwargs):
        """Represents an item in the supplementary resource schema"""
        super().__init__(*args, **kwargs)
        self.ui = Ui_WidgetSupplementTags()
        self.ui.setupUi(self)

        self.ui.tableWidget.setRowCount(3)
        self.on_assert_row_count()

        # signals
        self.ui.tableWidget.itemChanged.connect(self.on_value_changed)
        self.ui.tableWidget.itemChanged.connect(self.on_assert_row_count)

    def check(self, b):
        """Check the check box"""
        self.ui.checkBox.setChecked(b)

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
        for ii in range(self.ui.tableWidget.rowCount()):
            it0 = self.ui.tableWidget.item(ii, 0).text().strip()
            it1 = self.ui.tableWidget.item(ii, 1).text().strip()
            if it0 or it1:
                value.append((it0, it1))
        return value

    def set_value(self, value):
        """Set the value"""
        self.on_assert_row_count()
        self.ui.tableWidget.setRowCount(len(value))
        for ii, it in enumerate(value):
            self.ui.tableWidget.item(ii, 0).setText(it[0])
            self.ui.tableWidget.item(ii, 1).setText(it[1])

    @QtCore.pyqtSlot()
    def on_assert_row_count(self):
        """Increase row count by one if last item has data"""
        nrow = self.ui.tableWidget.rowCount()
        it0 = self.ui.tableWidget.item(nrow-1, 0)
        it1 = self.ui.tableWidget.item(nrow-1, 1)
        if (it0 and it0.text().strip() or it1 and it1.text().strip()):
            nrow += 1
            self.ui.tableWidget.setRowCount(nrow)
        # populate empty items
        for ii in range(nrow):
            for jj in [0, 1]:
                it = self.ui.tableWidget.item(ii, jj)
                if it is None:
                    self.ui.tableWidget.setItem(ii, jj,
                                                QtWidgets.QTableWidgetItem())


class RSSItem(RSSItemBase):
    def __init__(self, *args, **kwargs):
        """Represents an item in the supplementary resource schema"""
        super().__init__(*args, **kwargs)
        self.ui = Ui_WidgetSupplementItem()
        self.ui.setupUi(self)

        self.apply_schema()

        # signals
        self.ui.checkBox.clicked.connect(self.on_value_changed)
        self.ui.spinBox.valueChanged.connect(self.on_value_changed)
        self.ui.doubleSpinBox.valueChanged.connect(self.on_value_changed)
        self.ui.lineEdit.textEdited.connect(self.on_value_changed)
        self.ui.plainTextEdit.textChanged.connect(self.on_value_changed)
        self.ui.comboBox.currentTextChanged.connect(self.on_value_changed)
        self.ui.radioButton_yes.clicked.connect(self.on_value_changed)
        self.ui.radioButton_no.clicked.connect(self.on_value_changed)

    def apply_schema(self):
        """Initialize the item schema according to self.rss_dict"""
        rss_dict = self.rss_dict
        self.ui.checkBox.setText(rss_dict["name"])
        self.ui.checkBox.setToolTip(rss_dict.get("hint", None))

        widget = self.get_data_widget()
        self.show_only_data_widget()
        if "choices" in rss_dict:  # combobox
            assert widget is self.ui.comboBox
            self.ui.comboBox.insertItems(0, rss_dict["choices"])
            editable = "choices fixed" not in rss_dict.get("options", [])
            self.ui.comboBox.setEditable(editable)
            if editable:
                self.ui.comboBox.setToolTip(
                    "You may also type your own choice.")
        else:
            example = rss_dict.get("example", None)
            if example and hasattr(widget, "setPlaceholderText"):
                widget.setPlaceholderText(f"e.g. {example}")
        if "unit" in rss_dict:
            self.ui.doubleSpinBox.setSuffix(" " + rss_dict["unit"])

    def check(self, b):
        """Check the check box"""
        self.ui.checkBox.setChecked(b)

    def get_value(self):
        """Return the value of the current data widget"""
        _, value = self.get_data_widget(retval=True)
        return value

    def set_value(self, value):
        """Set the widget value with the appropriate function"""
        widget = self.get_data_widget()
        if widget is self.ui.lineEdit:
            self.ui.lineEdit.setText(value)
        elif widget is self.ui.comboBox:
            self.ui.comboBox.setCurrentText(value)
        elif widget is self.ui.spinBox or widget is self.ui.doubleSpinBox:
            widget.setValue(value)
        elif widget is self.ui.plainTextEdit:
            widget.setPlainText(value)
        elif widget is self.ui.widget_bool:
            if value:
                self.ui.radioButton_yes.setChecked(True)
            else:
                self.ui.radioButton_no.setChecked(True)

    def get_data_widget(self, retval=False):
        """Return the widget the holds the data according to self.rss_dict"""
        self.blockSignals(True)
        rss_dict = self.rss_dict
        if "choices" in rss_dict:  # combobox
            widget = self.ui.comboBox
            value = self.ui.comboBox.currentText()
        else:
            itemtype = rss_dict.get("type", "string")
            if itemtype == "string":
                if "text" in rss_dict.get("options", []):
                    widget = self.ui.plainTextEdit
                    value = self.ui.plainTextEdit.toPlainText()
                else:
                    widget = self.ui.lineEdit
                    value = self.ui.lineEdit.text()
            elif itemtype == "list":
                widget = self.ui.lineEdit
                value = self.ui.lineEdit.text()
            elif itemtype == "float":
                widget = self.ui.doubleSpinBox
                value = self.ui.doubleSpinBox.value()
            elif itemtype == "integer":
                widget = self.ui.spinBox
                value = self.ui.spinBox.value()
            elif itemtype == "boolean":
                widget = self.ui.widget_bool
                value = self.ui.radioButton_yes.isChecked()
            elif itemtype == "date":
                widget = self.ui.lineEdit
                value = self.ui.lineEdit.text()
            else:
                raise ValueError(f"No rule to process item {rss_dict}")
        self.blockSignals(False)
        if retval:
            return widget, value
        else:
            return widget

    def show_only_data_widget(self):
        """Convenience function that hides all but the data widget"""
        widget = self.get_data_widget()
        for ww in [self.ui.comboBox,
                   self.ui.doubleSpinBox,
                   self.ui.lineEdit,
                   self.ui.plainTextEdit,
                   self.ui.spinBox,
                   self.ui.widget_bool]:
            if ww is not widget:
                ww.hide()
