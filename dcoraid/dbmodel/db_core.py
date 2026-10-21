from __future__ import annotations

import abc

from .extract import DBExtract


class DBInterrogator(abc.ABC):
    def __init__(self, user_data):
        if user_data:
            mandatory = ["id", "name", "number_created_packages"]
            missing = [key for key in mandatory if key not in user_data]
            if missing:
                raise ValueError("The following keys are missing in "
                                 f"`user_data`: {missing}")
        self.user_data = user_data
        self.search_query = {}

    @property
    def is_up_to_date(self):
        """Checks whether the local database copy is up-to-date"""
        if (self.local_version_score != self.remote_version_score
                or self.local_timestamp != self.remote_timestamp):
            uptodate = False
        else:
            uptodate = True
        return uptodate

    def close(self):
        """Make sure any open file handles or connections are closed"""

    @abc.abstractmethod
    def get_circles(self) -> list[dict]:
        """Return the list of DCOR Circle dictionaries"""

    @abc.abstractmethod
    def get_circles_user(self) -> list[dict]:
        """Return the list of DCOR Circle dictionaries the user is member of
        """

    @abc.abstractmethod
    def get_collections(self) -> list[dict]:
        """Return the list of DCOR Collection dictionaries"""

    @abc.abstractmethod
    def get_collections_user(self) -> list[dict]:
        """Return the list of DCOR Collection dicts the user is member of
        """

    def get_datasets_user(self):
        """Return DBExtract with data owned by or shared with the user"""
        owned = self.get_datasets_user_owned()
        shared = self.get_datasets_user_shared()
        following = self.get_datasets_user_following()
        # these are all instances of DBExtract
        return owned + shared + following

    @abc.abstractmethod
    def get_datasets_user_following(self) -> DBExtract:
        """Return all datasets the user is following"""

    @abc.abstractmethod
    def get_datasets_user_owned(self) -> DBExtract:
        """Return all datasets owned by the user"""

    @abc.abstractmethod
    def get_datasets_user_shared(self) -> DBExtract:
        """Return all datasets shared with the user"""

    @abc.abstractmethod
    def get_users(self) -> list[str] | tuple[list[str], list[str]]:
        """Return the list of DCOR users"""

    @abc.abstractmethod
    def search_dataset(self, query, limit=100) -> DBExtract:
        """Free text search for a dataset in the database"""

    @property
    @abc.abstractmethod
    def local_version_score(self) -> int:
        """Local database version"""

    @property
    @abc.abstractmethod
    def local_timestamp(self) -> int | float:
        """Local database date in seconds since epoch"""

    @property
    def remote_version_score(self) -> int:
        """Remote database version"""
        return 0

    @property
    def remote_timestamp(self) -> int | float:
        """Remote database date in seconds since epoch"""
        return 0

    @abc.abstractmethod
    def update(self, reset=False):
        """Update the local database copy"""
