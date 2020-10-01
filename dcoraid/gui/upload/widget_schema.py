from PyQt5 import QtWidgets

from .widget_supplement_item import RSSItem


class SchemaWidget(QtWidgets.QWidget):

    def __init__(self, *args, **kwargs):
        """Widget containing groups of """
        super(SchemaWidget, self).__init__(*args, **kwargs)
        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.schema_widgets = {}

    def populate_schema(self, schema_dict):
        for section in schema_dict:
            widget_list = []
            label = QtWidgets.QLabel(section.capitalize())
            self.verticalLayout.addWidget(label)
            for item in schema_dict[section]["items"]:
                wrss = RSSItem(item, self)
                self.verticalLayout.addWidget(wrss)
                widget_list.append(wrss)
            self.schema_widgets[section] = widget_list
