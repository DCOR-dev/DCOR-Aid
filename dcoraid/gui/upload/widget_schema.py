from PyQt5 import QtCore, QtWidgets

from .widget_supplement_item import RSSItem, RSSTagsItem


#: These items displayed differently to the user and may populate other keys
MAGIC_ITEMS = {
    "identifiers": {
        "tags": {
            "widget": RSSTagsItem,  # use this instead of RSSItem
            "setters": [
                {"section": "identifiers",
                 "key": "compounds",
                 "func": "get_compounds"
                 },
                {"section": "identifiers",
                 "key": "labels",
                 "func": "get_labels"
                 },
                {"section": "identifiers",
                 "key": "tags",
                 "func": "get_tags"
                 },
            ]
        }
    }
}

#: These items are defined by magical items (or are just ignored)
HIDDEN_ITEMS = {
    "identifiers": ["antibodies", "fluorophores"]
}


class SchemaWidget(QtWidgets.QWidget):
    schema_changed = QtCore.pyqtSignal()

    def __init__(self, *args, **kwargs):
        """Widget containing groups of """
        super(SchemaWidget, self).__init__(*args, **kwargs)
        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.schema_widgets = {}

    def get_current_schema(self):
        """Return the current schema which will can stored in the model

        The schema also contains magic items (see :const:`MAGIC_ITEMS`)
        which are super-items that contain information about more than
        one item.
        """
        schema = {}
        for sec in self.schema_widgets:
            for widget in self.schema_widgets[sec]:
                if widget.checkBox.isChecked():
                    if sec not in schema:
                        schema[sec] = {}
                    key = widget.rss_dict["key"]
                    # We extract all relevant information from magic items
                    if sec in MAGIC_ITEMS and key in MAGIC_ITEMS[sec]:
                        schema[sec]["MAGIC_{}".format(
                            key)] = widget.get_value()
                        for sr in MAGIC_ITEMS[sec][key]["setters"]:
                            fc = getattr(widget, sr["func"])
                            schema[sr["section"]][sr["key"]] = fc()
                    else:
                        schema[sec][key] = widget.get_value()
        return schema

    def populate_schema(self, schema_dict):
        """Create all widgets corresponding to the schema

        Omits any :const:`HIDDEN_ITEMS` and creates special widgets
        for :const:`MAGIC_ITEMS`.
        """
        for sec in schema_dict:
            widget_list = []
            label = QtWidgets.QLabel(sec.capitalize())
            self.verticalLayout.addWidget(label)
            for item in schema_dict[sec]["items"]:
                key = item["key"]
                if sec in MAGIC_ITEMS and key in MAGIC_ITEMS[sec]:
                    # is a special widget (e.g. tags)
                    wrss = MAGIC_ITEMS[sec][key]["widget"](item, self)
                elif sec in HIDDEN_ITEMS and key in HIDDEN_ITEMS[sec]:
                    # should not be displayed
                    continue
                else:
                    wrss = RSSItem(item, self)
                self.verticalLayout.addWidget(wrss)
                widget_list.append(wrss)
                wrss.value_changed.connect(self.schema_changed)
            self.schema_widgets[sec] = widget_list

    def set_schema(self, schema_dict):
        """Apply schema to all widgets

        If there are any special :const:`MAGIC_ITEMS` widgets,
        then the `schema_dict` should contain the corresponding
        `MAGIC_key` key (which is used to set the value of the
        special widget).
        """
        self.blockSignals(True)
        for sec in self.schema_widgets:
            for wrss in self.schema_widgets[sec]:
                key = wrss.rss_dict["key"]
                match = sec in schema_dict and key in schema_dict[sec]
                wrss.check(match)
                if match:
                    # Note that hidden items don't show up here since
                    # we iterate over the widgets.
                    if sec in MAGIC_ITEMS and key in MAGIC_ITEMS[sec]:
                        value = schema_dict[sec]["MAGIC_{}".format(key)]
                    else:
                        value = schema_dict[sec][key]
                    wrss.set_value(value)
        self.blockSignals(False)
