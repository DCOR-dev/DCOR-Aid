import abc

from ..common import ttl_cache


class DBInterrogator(abc.ABC):
    def __init__(self, mode, user_data):
        if mode == "user":
            mandatory = ["id", "name", "number_created_packages"]
            missing = [key for key in mandatory if key not in user_data]
            if missing:
                raise ValueError("The following keys are missing in "
                                 f"`user_data` for `mode=='user'`: {missing}")
        self.user_data = user_data
        self.mode = mode
        self.search_query = {}

    @property
    def is_up_to_date(self):
        """Checks whether the local database copy is up-to-date"""
        if self.local_version_score != self.remote_version_score:
            uptodate = False
        elif self.local_timestamp != self.remote_timestamp:
            uptodate = False
        else:
            uptodate = True
        return uptodate

    @abc.abstractmethod
    def get_circles(self):
        """Return the list of DCOR Circles"""
        pass

    @abc.abstractmethod
    def get_collections(self):
        """Return the list of DCOR Collections"""
        pass

    def get_datasets_user(self):
        """Return DBExtract with data owned by or shared with the user"""
        if self.mode != "user":
            raise ValueError("Cannot get user datasets if mode is 'public'!")
        owned = self.get_datasets_user_owned()
        shared = self.get_datasets_user_shared()
        following = self.get_datasets_user_following()
        # these are all instances of DBExtract
        return owned + shared + following

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
    def get_users(self):
        """Return the list of DCOR users"""
        pass

    @abc.abstractmethod
    def search_dataset(self, query, circles=None, collections=None):
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
