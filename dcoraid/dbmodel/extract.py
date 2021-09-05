from functools import lru_cache


class DBExtract:
    def __init__(self, datasets):
        """User-convenient access to dataset search results

        Parameters
        ----------
        datasets: list
            List of CKAN package dictionaries
        """
        self._circles = None
        self._collections = None
        self._dataset_name_index = None
        self.datasets = datasets

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

    def _generate_index(self):
        if self._dataset_name_index is None:
            index = {}
            for ii, dd in enumerate(self.datasets):
                index[dd["name"]] = ii
            self._dataset_name_index = index

    def get_dataset_dict(self, dataset_name):
        self._generate_index()
        return self.datasets[self._dataset_name_index[dataset_name]]
