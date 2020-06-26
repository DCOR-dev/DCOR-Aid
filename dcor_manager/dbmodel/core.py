import abc
from .util import ttl_cache


class DBModel(abc.ABC):
    def __init__(self, dcor_url, *args, **kwargs):
        """Abstract base class for accessing a DCOR/CKAN database"""

    @property
    def is_up_to_date(self):
        if self.local_version_score != self.remote_version_score:
            uptodate = False
        elif self.local_timestamp != self.remote_timestamp:
            uptodate = False
        else:
            uptodate = True
        return uptodate

    @abc.abstracmethod
    @property
    def local_version_score(self):
        """Local database version"""

    @abc.abstracmethod
    @property
    def local_timestamp(self):
        """Local database date in seconds since epoch"""

    @ttl_cache(seconds=5)
    @property
    def remote_version_score(self):
        """Remote database version"""
        return 0

    @ttl_cache(seconds=5)
    @property
    def remote_timestamp(self):
        """Remote database date in seconds since epoch"""
        return 0

    def get_circles(self):
        pass

    def set_filter(self):
        """Set a filter"""
        pass

    def rebuild_db(self):
        """This only affects local databases"""
