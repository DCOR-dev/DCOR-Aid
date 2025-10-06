from functools import partial
import webbrowser

from PyQt6 import QtWidgets
from PyQt6.QtCore import Qt

from ...common import is_dc_resource_dict

from ..api import get_ckan_api

from . import filter_base


class FilterCircles(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterCircles, self).__init__(*args, **kwargs)
        self.active_actions += ["view-online"]

        self.label.setText("Circles")
        self.lineEdit.setPlaceholderText("filter names...")
        self.checkBox.setVisible(False)
        self.label_info.setVisible(False)

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/organization/{entry['name']}"
        actions = [
            {"name": "view-online",
             "icon": "eye",
             "tooltip": f"View circle {entry['name']} online",
             "function": partial(webbrowser.open, url)}
        ]
        return actions


class FilterCollections(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterCollections, self).__init__(*args, **kwargs)
        self.active_actions += [
            "download", "download-condensed", "view-online"]
        self.label.setText("Collections")
        self.lineEdit.setPlaceholderText("filter names...")
        self.checkBox.setVisible(False)
        self.label_info.setVisible(False)

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/group/{entry['name']}"
        actions = [
            {"name": "download",
             "icon": "angle-down",
             "tooltip": f"Download collection {entry['name']}",
             "function": partial(self.download_item.emit,
                                 "collection", entry["name"], False)},
            {"name": "download-condensed",
             "icon": "angles-down",
             "tooltip": f"Download condensed collection {entry['name']}",
             "function": partial(self.download_item.emit,
                                 "collection", entry["name"], True)},
            {"name": "share",
             "icon": "share-nodes",
             "tooltip": f"Share collection '{entry['name']}' with a user",
             "function": partial(self.share_item.emit,
                                 "collection", entry["name"])},
            {"name": "edit",
             "icon": "pencil",
             "tooltip": f"Modify collection '{entry['name']}'",
             "function": partial(self.edit_item.emit,
                                 "collection", entry["name"])},
            {"name": "view-online",
             "icon": "eye",
             "tooltip": f"View collection {entry['name']} online",
             "function": partial(webbrowser.open, url)}
        ]
        return actions


class FilterDatasets(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterDatasets, self).__init__(*args, **kwargs)
        self.active_actions += [
            "download", "download-condensed", "view-online"]
        self.label.setText("Datasets")
        self.lineEdit.setPlaceholderText("filter titles...")
        self.checkBox.setVisible(False)
        self.label_info.setVisible(False)

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/dataset/{entry['name']}"
        actions = [
            {"name": "download",
             "icon": "angle-down",
             "tooltip": f"Download dataset {entry['name']}",
             "function": partial(self.download_item.emit,
                                 "dataset", entry["id"], False)},
            {"name": "download-condensed",
             "icon": "angles-down",
             "tooltip": f"Download condensed dataset {entry['name']}",
             "function": partial(self.download_item.emit,
                                 "dataset", entry["id"], True)},
            {"name": "share",
             "icon": "share-nodes",
             "tooltip": f"Share dataset '{entry['name']}' with a user",
             "function": partial(self.share_item.emit,
                                 "dataset", entry["name"])},
            {"name": "remove-from-collection",
             "icon": "trash-can-arrow-up",
             "tooltip": f"Remove dataset '{entry['name']}' from collection",
             "function": partial(self.remove_item.emit,
                                 "dataset", entry["id"])},
            {"name": "view-online",
             "icon": "eye",
             "tooltip": f"View dataset {entry['name']} online",
             "function": partial(webbrowser.open, url)},
        ]
        return actions


class FilterResources(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterResources, self).__init__(*args, **kwargs)
        self.active_actions += [
            "download", "download-condensed", "view-online"]
        self.label.setText("Resources")
        self.lineEdit.setPlaceholderText("filter file names...")
        self.checkBox.setVisible(True)
        self.checkBox.setText(".rtdc only")
        self.checkBox.setChecked(True)
        self.tableWidget.setDragEnabled(True)
        self.tableWidget.setDragDropMode(
            QtWidgets.QAbstractItemView.DragDropMode.DragOnly)
        self.label_info.setText(
            "<i>Hint: You can drag and drop your selection "
            "from the resources list to DCscope!</i>")

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/dataset/{entry['package_id']}/" \
              + f"resource/{entry['id']}"
        actions = [
            {"name": "download",
             "icon": "angle-down",
             "tooltip": f"Download resource {entry['name']}",
             "function": partial(self.download_item.emit,
                                 "resource", entry["id"], False)},
            {"name": "view-online",
             "icon": "eye",
             "tooltip": f"View resource {entry['name']} online",
             "function": partial(webbrowser.open, url)},
        ]
        if is_dc_resource_dict(entry):
            # only show condensed-download-button for .rtdc files
            actions.insert(
                1,
                {"name": "download-condensed",
                 "icon": "angles-down",
                 "tooltip": f"Download condensed resource {entry['name']}",
                 "function": partial(self.download_item.emit,
                                     "resource", entry["id"], True)},
            )
        return actions

    def set_entry_label(self, row, entry):
        super(FilterResources, self).set_entry_label(row, entry)
        # set additional data link
        item = self.tableWidget.item(row, 0)
        api = get_ckan_api()
        dcor_url = f"{api.server}/api/3/action/dcserv?id={entry['id']}"
        item.setData(Qt.ItemDataRole.UserRole + 1, dcor_url)
