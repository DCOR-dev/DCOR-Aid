from PyQt5 import QtCore, QtWidgets

from .widget_supplement_item import RSSItem


class SchemaWidget(QtWidgets.QWidget):
    schema_changed = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Widget containing groups of """
        super(SchemaWidget, self).__init__(*args, **kwargs)
        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.schema_widgets = {}

    def get_current_schema(self):
        schema = {}
        for sec in self.schema_widgets:
            for widget in self.schema_widgets[sec]:
                if widget.checkBox.isChecked():
                    if sec not in schema:
                        schema[sec] = {}
                    key = widget.rss_dict["key"]
                    schema[sec][key] = widget.get_value()
        return schema

    def populate_schema(self, schema_dict):
        for sec in schema_dict:
            widget_list = []
            label = QtWidgets.QLabel(sec.capitalize())
            self.verticalLayout.addWidget(label)
            for item in schema_dict[sec]["items"]:
                wrss = RSSItem(item, self)
                self.verticalLayout.addWidget(wrss)
                widget_list.append(wrss)
                wrss.value_changed.connect(self.schema_changed)
            self.schema_widgets[sec] = widget_list

    def set_schema(self, schema_dict):
        self.blockSignals(True)
        for sec in self.schema_widgets:
            for wrss in self.schema_widgets[sec]:
                key = wrss.rss_dict["key"]
                match = sec in schema_dict and key in schema_dict[sec]
                wrss.check(match)
                if match:
                    wrss.set_value(schema_dict[sec][key])
        self.blockSignals(False)
