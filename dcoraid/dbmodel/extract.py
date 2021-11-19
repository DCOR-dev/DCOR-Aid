import numbers
from functools import lru_cache


class DBExtract:
    def __init__(self, datasets=None):
        """User-convenient access to dataset search results

        Parameters
        ----------
        datasets: list
            List of CKAN package dictionaries
        """
        self._circles = None
        self._collections = None
        self._dataset_name_index = None

        self.registry = {}
        self.registry_id = {}
        self.datasets = []
        if datasets:
            self.add_datasets(datasets)

    def __add__(self, other):
        return DBExtract(self.datasets + other.datasets)

    def __iadd__(self, other):
        self.add_datasets(other.datasets)
        return self

    def __contains__(self, item):
        """Check whether dataset is in this DBExtract

        Parameters
        ----------
        item: dict or str
            The dataset dictionary or the name or the id of the dataset
        """
        if isinstance(item, dict):
            id_name = item["id"]
        else:
            id_name = item
        if len(id_name) == 36:  # minor optimization
            # we probably have a UUID
            return id_name in self.registry_id or id_name in self.registry
        else:
            return id_name in self.registry

    def __getitem__(self, idx_or_name):
        if isinstance(idx_or_name, numbers.Integral):
            return self.datasets[idx_or_name]
        else:
            return self.get_dataset_dict(idx_or_name)

    def __len__(self):
        return len(self.datasets)

    def __iter__(self):
        return iter(self.datasets)

    def add_datasets(self, datasets):
        for dd in datasets:
            name = dd["name"]
            if name not in self.registry:  # datasets must only be added once
                self.registry[name] = dd
                self.datasets.append(dd)

    @property
    @lru_cache(maxsize=1)
    def circles(self):
        if not self._circles:
            circ_list = []
            circ_names = []
            for dd in self.datasets:
                name = dd["organization"]["name"]
                if name not in circ_names:
                    circ_list.append(dd["organization"])
                    circ_names.append(name)
            self._circles = sorted(
                circ_list, key=lambda x: x.get("title") or x["name"])
        return self._circles

    @property
    @lru_cache(maxsize=1)
    def collections(self):
        if not self._collections:
            coll_list = []
            coll_names = []
            for dd in self.datasets:
                for gg in dd["groups"]:
                    name = gg["name"]
                    if name not in coll_names:
                        coll_list.append(gg)
                        coll_names.append(name)
            self._collections = sorted(
                coll_list, key=lambda x: x.get("title") or x["name"])
        return self._collections

    def get_dataset_dict(self, dataset_name):
        return self.registry[dataset_name]
