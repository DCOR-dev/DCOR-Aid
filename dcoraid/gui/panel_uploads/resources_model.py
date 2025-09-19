import collections
import re
from collections import OrderedDict
import copy
from functools import lru_cache
import pathlib

from PyQt6 import QtCore, QtGui

from ...common import is_dc_file
from ...upload import job


class ResourcesModel(QtCore.QAbstractListModel):
    """Handle resources and their metadata selected in the UI

    This is the "Model" component of the QListView.
    """

    def __init__(self, *args, **kwargs):
        super(ResourcesModel, self).__init__(*args, **kwargs)
        self.resources = OrderedDict()

    def add_resources(self, rslist):
        """Add resources to the current model

        Note that resource suffixes are not checked for validity
        using the `supported_resource_suffixes` API command.
        This should be done in the UI file dialogs.
        """
        for ff in rslist:
            if ff not in self.resources:  # avoid adding the same file twice
                # initialize file metadata
                data = {"file": {}}
                path = pathlib.Path(ff)
                fname = path.name
                if re.match("M[0-9]*_data.rtdc", fname):
                    # We have M0001_data.rtdc. Get the file name from the
                    # directory above so the user does not have to manually
                    # rename everything
                    fname = f"{path.parent.name}_{fname}"
                data["file"]["filename"] = job.valid_resource_name(fname)
                data["file"]["filename_init"] = data["file"]["filename"]
                self.resources[ff] = data
                self.layoutChanged.emit()

    def data(self, index, role=QtCore.Qt.ItemDataRole.DisplayRole):
        """Return data for 'View'"""
        if role == QtCore.Qt.ItemDataRole.DisplayRole:
            _, data = self.get_data_for_index(index)
            return data["file"]["filename"]

        if role == QtCore.Qt.ItemDataRole.DecorationRole:
            if self.index_has_edits(index):
                return get_icon("tag")
            else:
                return get_icon("slash")

    def filenames_are_unique(self):
        """Test whether the final file names are unique"""
        data = self.get_all_data()
        names = [data[dd]["file"]["filename"] for dd in data]
        return len(names) == len(list(set(names)))

    def filenames_were_edited(self):
        """Return number of filenames that have been edited"""
        data = self.get_all_data()
        counter = 0
        for key in data:
            if pathlib.Path(key).name != data[key]["file"]["filename"]:
                counter += 1
        return counter

    def index_is_dc(self, index):
        """Does the given index instance belong to an RT-DC file?"""
        path = pathlib.Path(self.get_file_list()[index.row()])
        return is_dc_file(path, test_open=False)

    def index_has_edits(self, index):
        """Is there a modification of the list entry of this index instance?"""
        filen = self.get_file_list()[index.row()]
        return bool(self.resources[filen])

    def supplements_were_edited(self):
        """Return number of resource supplements that have been edited"""
        data = self.get_all_data()
        counter = 0
        for key in data:
            if data[key]["supplement"]:
                counter += 1
        return counter

    def get_all_data(self, magic_keys=True):
        """Return dictionary with complete information for all resources"""
        data = collections.OrderedDict()
        for ii, path in enumerate(self.resources.keys()):
            data[path] = self.get_data_for_row(ii, magic_keys=magic_keys)[1]
        return data

    def get_common_supplements_from_indexes(self, indexes):
        """Return the supplementary items common to indexes"""
        if indexes:
            common = self.get_data_for_index(indexes[0])[1]["supplement"]
            for idx in indexes:
                supi = self.get_data_for_index(idx)[1]["supplement"]
                for sec in list(common.keys()):
                    if sec not in supi:
                        common.pop(sec)
                    else:
                        for key in list(common[sec].keys()):
                            if (key not in supi[sec]
                                    or supi[sec][key] != common[sec][key]):
                                common[sec].pop(key)
        else:
            common = {}
        return common

    def get_data_for_index(self, index):
        """Return the complete resource dictionary for this index"""
        return self.get_data_for_row(index.row())

    def get_data_for_row(self, row, magic_keys=True):
        """Return the complete information dictionary for this row index"""
        rfile = self.get_file_list()[row]
        data = copy.deepcopy(self.resources[rfile])
        if "supplement" not in data:
            data["supplement"] = {}
        if not magic_keys:
            for sec in list(data["supplement"].keys()):
                for key in list(data["supplement"][sec].keys()):
                    if key.startswith("MAGIC_"):
                        data["supplement"][sec].pop(key)
        return rfile, data

    def get_file_list(self):
        """Return the list of paths in the model"""
        return [item for item in self.resources]

    def get_filename_from_index(self, index):
        """Return the full path that belongs to a given index instance"""
        row = index.row()
        return self.get_file_list()[row]

    def get_indexes_types(self, indexes):
        """Return which types of files are selected

        Returns
        -------
        ftype: str
            None if selection is None, "dc" if selection is
            exclusively DC data, "mixed", if DC data and other
            resources are selected, "nodc" if no DC resources
            are selected.
        """
        dc_count = sum([self.index_is_dc(idx) for idx in indexes])
        if len(indexes) == 0:
            return None
        elif dc_count == len(indexes):
            return "dc"
        elif dc_count == 0:
            return "nodc"
        else:
            return "mixed"

    def rem_resources(self, indexes):
        """Remove resources in this list of index instances"""
        ids = [idx.row() for idx in indexes]
        for ii, pp in enumerate(self.get_file_list()):
            if ii in ids:
                self.resources.pop(pp)
        self.layoutChanged.emit()

    def reset_filenames(self, indexes):
        """Reset the file names of the resources in list of index instances"""
        resource_keys = list(self.resources.keys())
        ids = [idx.row() for idx in indexes]
        for index in ids:
            key = resource_keys[index]
            self.resources[key]["file"]["filename"] = \
                self.resources[key]["file"]["filename_init"]
        self.layoutChanged.emit()

    def rowCount(self, index=None):
        """Return number of resources"""
        return len(self.resources)

    @QtCore.pyqtSlot(list, dict)
    def update_resources(self, indexes, data_dict):
        """Update resources defined by index list with `data_dict`"""
        if len(indexes) == 0:  # nothing to do
            return
        elif len(indexes) == 1 and "file" in data_dict:
            # update the name
            pp = self.get_filename_from_index(indexes[0])
            self.resources[pp]["file"].update(data_dict["file"])

        if "supplement" in data_dict:
            for idx in indexes:
                if self.index_is_dc(idx):
                    # update supplementary parameters
                    pp = self.get_filename_from_index(idx)
                    self.resources[pp].update(
                        {"supplement": data_dict["supplement"]})

        self.layoutChanged.emit()


@lru_cache(maxsize=32)
def get_icon(name):
    return QtGui.QIcon.fromTheme(name).pixmap(16, 16)
