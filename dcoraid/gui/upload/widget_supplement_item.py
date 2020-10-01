import pkg_resources

from PyQt5 import uic, QtWidgets


class RSSItem(QtWidgets.QWidget):
    def __init__(self, rss_dict, *args, **kwargs):
        """Represents an item in the supplementary resource schema"""
        super(RSSItem, self).__init__(*args, **kwargs)
        path_ui = pkg_resources.resource_filename(
            "dcoraid.gui.upload", "widget_supplement_item.ui")
        uic.loadUi(path_ui, self)
        self.apply_schema(rss_dict)

    def apply_schema(self, rss_dict):
        self.checkBox.setText(rss_dict["name"])
        self.checkBox.setToolTip(rss_dict.get("hint", None))

        if "choices" in rss_dict:  # combobox
            self.show_only_edit_widget(self.comboBox)
            self.comboBox.insertItems(0, rss_dict["choices"])
            editable = "choices fixed" not in rss_dict.get("options", [])
            self.comboBox.setEditable(editable)
        else:
            itemtype = rss_dict.get("type", "string")
            if itemtype == "string":
                if "text" in rss_dict.get("options", []):
                    widget = self.plainTextEdit
                else:
                    widget = self.lineEdit
            elif itemtype == "list":
                widget = self.lineEdit
            elif itemtype == "float":
                widget = self.doubleSpinBox
            elif itemtype == "integer":
                widget = self.spinBox
            else:
                raise ValueError("No rule to process item {}".format(rss_dict))
            self.show_only_edit_widget(widget)
            example = rss_dict.get("example", None)
            if example:
                widget.setToolTip("e.g. {}".format(example))

    def show_only_edit_widget(self, widget):
        for ww in [self.comboBox,
                   self.doubleSpinBox,
                   self.lineEdit,
                   self.plainTextEdit,
                   self.spinBox]:
            if ww is not widget:
                ww.hide()
