from PyQt5 import QtCore, QtWidgets

from .widget_supplement_item import RSSItem, RSSTagsItem, TitleItem


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
        """Return the current schema dict for storage in the model

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

    @QtCore.pyqtSlot(str, str, object, bool)
    def on_hide_show_items(self, section, key, value, enabled):
        """Send a value_changed signal to all widgets

        ...so they can decide whether they are visible or not.
        """
        for sec in self.schema_widgets:
            for item in self.schema_widgets[sec]:
                item.on_schema_key_toggled(section, key, value, enabled)
        for label in self.schema_title_widgets:
            label.on_schema_key_toggled(section, key, value, enabled)

    def populate_schema(self, schema_dict):
        """Create all widgets corresponding to the schema

        Omits any :const:`HIDDEN_ITEMS` and creates special widgets
        for :const:`MAGIC_ITEMS`.
        """
        self.schema_title_widgets = []
        for sec in schema_dict:
            widget_list = []
            label = TitleItem(schema_dict[sec].get("requires", {}),
                              schema_dict[sec]["name"])
            self.schema_title_widgets.append(label)
            self.verticalLayout.addWidget(label)
            for item in schema_dict[sec]["items"]:
                key = item["key"]
                # Update item requirements with section requirements
                if "requires" not in item:
                    item["requires"] = schema_dict[sec].get("requires", {})
                if sec in MAGIC_ITEMS and key in MAGIC_ITEMS[sec]:
                    # is a special widget (e.g. tags)
                    wrss = MAGIC_ITEMS[sec][key]["widget"](item,
                                                           section=sec,
                                                           parent=self)
                elif sec in HIDDEN_ITEMS and key in HIDDEN_ITEMS[sec]:
                    # should not be displayed
                    continue
                else:
                    wrss = RSSItem(item, section=sec, parent=self)
                self.verticalLayout.addWidget(wrss)
                widget_list.append(wrss)
                wrss.value_changed.connect(self.schema_changed)
                wrss.value_changed.connect(self.on_hide_show_items)
            self.schema_widgets[sec] = widget_list
        # Finally add a stretch spacer in case there are not enough
        # items.
        spacer_item = QtWidgets.QSpacerItem(20, 0,
                                            QtWidgets.QSizePolicy.Minimum,
                                            QtWidgets.QSizePolicy.Expanding)
        self.verticalLayout.addItem(spacer_item)

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
        self.update_visible_widgets()

    def update_visible_widgets(self):
        """Cause all schema data widgets to emit their values

        This effectively updates the visibility of all schema
        widgets (including titles).
        """
        # Force all updates
        for sec in self.schema_widgets:
            for widget in self.schema_widgets[sec]:
                widget.emit_value()
