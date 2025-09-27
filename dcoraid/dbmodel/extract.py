import numbers

from ..common import weak_lru_cache


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

    def __iadd__(self, other: "DBExtract | list"):
        """Add datasets from DBExtract instance or from list to self"""
        if isinstance(other, DBExtract):
            to_add = other.datasets
        else:
            to_add = other
        self.add_datasets(to_add)
        return self

    def __contains__(self, item):
        """Check whether dataset is in this DBExtract

        Parameters
        ----------
        item: dict or str
            The dataset dictionary or the name or the id of the dataset
        """
        if isinstance(item, dict):
            name_or_id = item["id"]
        else:
            name_or_id = item
        return name_or_id in self.registry_id or name_or_id in self.registry

    def __getitem__(self, idx_or_name_or_id):
        if isinstance(idx_or_name_or_id, numbers.Integral):
            return self.datasets[idx_or_name_or_id]
        else:
            return self.get_dataset_dict(idx_or_name_or_id)

    def __iter__(self):
        return iter(self.datasets)

    def __len__(self):
        return len(self.datasets)

    def __repr__(self):
        return f"<DBExtract of size {len(self)} at {hex(id(self))}>"

    def add_datasets(self, datasets: list[dict]):
        for dd in datasets:
            if dd not in self:  # dataset must only be added once
                self.registry_id[dd["id"]] = dd
                self.registry[dd["name"]] = dd
                self.datasets.append(dd)

    @property
    @weak_lru_cache(maxsize=1)
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
    @weak_lru_cache(maxsize=1)
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

    def get_dataset_dict(self, name_or_id):
        if name_or_id in self.registry:
            return self.registry[name_or_id]
        elif name_or_id in self.registry_id:
            return self.registry_id[name_or_id]
        else:
            raise KeyError(f"Could not find name or id '{name_or_id}")
