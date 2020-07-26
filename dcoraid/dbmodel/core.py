import abc
from functools import lru_cache

from .util import ttl_cache


class DBExtract():
    def __init__(self, datasets, circles=None, collections=None):
        """User-convenient access to dataset search results

        Parameters
        ----------
        datasets: list
            List of CKAN package dictionaries
        """
        self._circles = circles
        self._collections = collections
        self._dataset_name_index = None
        self.datasets = datasets

    @property
    @lru_cache(maxsize=1)
    def circles(self):
        if self._circles is None:
            cl = []
            for dd in self.datasets:
                name = dd["organization"]["name"]
                if name not in cl:
                    cl.append(name)
            self._circles = sorted(cl)
        return self._circles

    @property
    @lru_cache(maxsize=1)
    def collections(self):
        if self._collections is None:
            ct = []
            for dd in self.datasets:
                for gg in dd["groups"]:
                    name = gg["name"]
                    if gg not in ct:
                        ct.append(name)
            self._collections = sorted(ct)
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


class DBModel():
    def __init__(self, interrogator, mode="public", *args, **kwargs):
        """User-convenient interface for DCOR/CKAN DBInterrogator

        Parameters
        ----------
        interrogator: DBInterrogator
            Class interacting with the database.
        mode: str
            This can be either "public" (only public datasets are
            considered) or "user" (datasets are limited to those owned
            by or shared with the user, including private datasets).
        """
        self.db = interrogator
        self.user_data = self.db.get_user_data()
        if mode not in ["public", "user"]:
            raise ValueError(
                "`mode` must be 'user', or 'public'; got '{}'!".format(mode))
        self.mode = mode
        self.search_query = {}
        self.set_filter()  # initiate search_query

    @property
    def is_up_to_date(self):
        return self.db.is_up_to_date()

    def get_circles(self):
        """Return the list of DCOR Circles"""
        return self.db.get_circles(mode=self.mode)

    def get_collections(self):
        """Return the list of DCOR Collections"""
        return self.db.get_collections(mode=self.mode)

    def get_user_datasets(self):
        """Return DBExtract with data owned by or shared with the user"""
        owned = self.db.get_datasets_user_owned()
        shared = self.db.get_datasets_user_shared()
        following = self.db.get_datasets_user_following()
        return DBExtract(owned+shared+following)

    def get_users(self):
        """Return the list of DCOR users"""
        return self.db.get_users()

    def search_dataset(self, query):
        """Search for a string in the database"""
        data = self.db.search_dataset(
            query,
            mode=self.mode,
            circles=self.search_query["circles"],
            collections=self.search_query["collections"],
        )
        extract = DBExtract(data,
                            circles=self.search_query["circles"],
                            collections=self.search_query["collections"],
                            )
        return extract

    def set_filter(self, circles=[], collections=[]):
        """Set a search query filter"""
        db_circles = self.db.get_circles(mode=self.mode)
        db_collections = self.db.get_collections(mode=self.mode)

        # If no circles or collections are specified, then
        # there would be no search results. For user
        # convenience, we set the complete lists instead.
        if not circles:
            circles = db_circles
        if not collections:
            collections = db_collections

        # Sanity checks
        for ci in circles:
            if ci not in db_circles:
                raise KeyError(
                    "Circle '{}' does not exist in '{}'!".format(ci, self.db))
        for co in collections:
            if co not in db_collections:
                raise KeyError(
                    "Collection '{}' does not exist in '{}'!".format(co,
                                                                     self.db))

        self.search_query["circles"] = circles
        self.search_query["collections"] = collections


class DBInterrogator(abc.ABC):
    def __init__(self, *args, **kwargs):
        self.search_query = {}

    @property
    def is_up_to_date(self):
        "Checks whether the local database copy is up-to-date"
        if self.local_version_score != self.remote_version_score:
            uptodate = False
        elif self.local_timestamp != self.remote_timestamp:
            uptodate = False
        else:
            uptodate = True
        return uptodate

    @abc.abstractmethod
    def get_circles(self, mode="public"):
        """Return the list of DCOR Circles"""
        pass

    @abc.abstractmethod
    def get_collections(self, mode="public"):
        """Return the list of DCOR Collections"""
        pass

    @abc.abstractmethod
    def get_datasets_user_following(self):
        """Return all datasets the user is following"""
        pass

    @abc.abstractmethod
    def get_datasets_user_owned(self):
        """Return all datasets owned by the user"""
        pass

    @abc.abstractmethod
    def get_datasets_user_shared(self):
        """Return all datasets shared with the user"""
        pass

    @abc.abstractmethod
    def get_user_data(self):
        """Return the current user data dictionary"""
        pass

    @abc.abstractmethod
    def get_users(self):
        """Return the list of DCOR users"""
        pass

    @abc.abstractmethod
    def search_dataset(self, circles, collections, mode="public"):
        pass

    @property
    @abc.abstractmethod
    def local_version_score(self):
        """Local database version"""

    @property
    @abc.abstractmethod
    def local_timestamp(self):
        """Local database date in seconds since epoch"""

    @property
    @ttl_cache(seconds=5)
    def remote_version_score(self):
        """Remote database version"""
        return 0

    @property
    @ttl_cache(seconds=5)
    def remote_timestamp(self):
        """Remote database date in seconds since epoch"""
        return 0
