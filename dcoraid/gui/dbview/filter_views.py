from functools import partial
import webbrowser

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import Qt

from ..api import get_ckan_api
from ..tools import ShowWaitCursor

from . import filter_base


class FilterCircles(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterCircles, self).__init__(*args, **kwargs)
        self.label.setText("Circles")
        self.lineEdit.setPlaceholderText("filter names...")
        self.checkBox.setVisible(False)
        self.label_info.setVisible(False)

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/organization/{entry['name']}"
        actions = [
            {"icon": "eye",
             "tooltip": f"view circle {entry['name']} online",
             "function": partial(webbrowser.open, url)}
        ]
        return actions


class FilterCollections(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterCollections, self).__init__(*args, **kwargs)
        self.label.setText("Collections")
        self.lineEdit.setPlaceholderText("filter names...")
        self.checkBox.setVisible(False)
        self.label_info.setVisible(False)

    def download_collection(self, collection_name, condensed=False):
        """Emit signals to download all resources of a collection

        Parameters
        ----------
        collection_name: str
            Name of the collection
        condensed: bool
            Whether to download condensed resources instead
        """
        with ShowWaitCursor():
            api = get_ckan_api()
            search_dict = api.get("package_search",
                                  fq=f"+groups:{collection_name}",
                                  include_private=True,
                                  rows=1000)
            num_datasets = search_dict["count"]
            if num_datasets >= 1000:
                raise NotImplementedError(
                    # We have to increase ckan.search.rows_max = 1000
                    # or (better) use the `start` parameter until we
                    # hit a number < 1000.
                    f"There are too many datasets in '{collection_name}'!")
            for ii, ds_dict in enumerate(search_dict["results"]):
                for res_dict in ds_dict.get("resources", []):
                    self.download_resource.emit(res_dict["id"], condensed)
                    QtWidgets.QApplication.processEvents(
                        QtCore.QEventLoop.AllEvents,
                        300)

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/group/{entry['name']}"
        actions = [
            {"icon": "angle-down",
             "tooltip": f"download collection {entry['name']}",
             "function": partial(self.download_collection, entry["name"])},
            {"icon": "angles-down",
             "tooltip": f"download condensed collection {entry['name']}",
             "function": partial(self.download_collection, entry["name"],
                                 condensed=True)},
            {"icon": "eye",
             "tooltip": f"view collection {entry['name']} online",
             "function": partial(webbrowser.open, url)}
        ]
        return actions


class FilterDatasets(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterDatasets, self).__init__(*args, **kwargs)
        self.label.setText("Datasets")
        self.lineEdit.setPlaceholderText("filter titles...")
        self.checkBox.setVisible(False)
        self.label_info.setVisible(False)

    def download_dataset(self, dataset_id, condensed=False):
        """Emit signals to download all resources of a dataset

        Parameters
        ----------
        dataset_id: str
            dataset ID
        condensed: bool
            Whether to download condensed resources instead
        """
        api = get_ckan_api()
        ds_dict = api.get("package_show", id=dataset_id)
        for res_dict in ds_dict.get("resources", []):
            self.download_resource.emit(res_dict["id"], condensed)
            QtWidgets.QApplication.processEvents(
                QtCore.QEventLoop.AllEvents,
                300)

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/dataset/{entry['name']}"
        actions = [
            {"icon": "angle-down",
             "tooltip": f"download dataset {entry['name']}",
             "function": partial(self.download_dataset, entry["id"])},
            {"icon": "angles-down",
             "tooltip": f"download condensed dataset {entry['name']}",
             "function": partial(self.download_dataset, entry["id"],
                                 condensed=True)},
            {"icon": "eye",
             "tooltip": f"view dataset {entry['name']} online",
             "function": partial(webbrowser.open, url)},
        ]
        return actions


class FilterResources(filter_base.FilterBase):
    def __init__(self, *args, **kwargs):
        super(FilterResources, self).__init__(*args, **kwargs)
        self.label.setText("Resources")
        self.lineEdit.setPlaceholderText("filter file names...")
        self.checkBox.setVisible(True)
        self.checkBox.setText(".rtdc only")
        self.checkBox.setChecked(True)
        self.tableWidget.setDragEnabled(True)
        self.tableWidget.setDragDropMode(
            QtWidgets.QAbstractItemView.DragOnly)
        self.label_info.setText("<i>Tip: You can drag and drop your selection "
                                "from the resources list to Shape-Out!</i>")

    def get_entry_actions(self, row, entry):
        api = get_ckan_api()
        url = f"{api.server}/dataset/{entry['package_id']}/" \
              + f"resource/{entry['id']}"
        actions = [
            {"icon": "angle-down",
             "tooltip": f"download resource {entry['name']}",
             "function": partial(self.download_resource.emit,
                                 entry["id"], False)},
            {"icon": "eye",
             "tooltip": f"view resource {entry['name']} online",
             "function": partial(webbrowser.open, url)},
        ]
        if entry["mimetype"] == "RT-DC":
            # only show condensed-download-button for .rtdc files
            actions.insert(
                1,
                {"icon": "angles-down",
                 "tooltip": f"download condensed resource {entry['name']}",
                 "function": partial(self.download_resource.emit,
                                     entry["id"], True)},)
        return actions

    def set_entry_label(self, row, entry):
        super(FilterResources, self).set_entry_label(row, entry)
        # set additional data link
        item = self.tableWidget.item(row, 0)
        api = get_ckan_api()
        dcor_url = f"{api.server}/api/3/action/dcserv?id={entry['id']}"
        item.setData(Qt.UserRole + 1, dcor_url)
