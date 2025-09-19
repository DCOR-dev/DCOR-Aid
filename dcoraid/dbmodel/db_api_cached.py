import logging
import pathlib
import time

from .db_core import DBInterrogator
from .db_api import APIInterrogator
from .meta_cache import MetaCache
from .extract import DBExtract


logger = logging.getLogger(__name__)


class CachedAPIInterrogator(DBInterrogator):
    def __init__(self, api, cache_location):
        self.api = api.copy()
        self.ai = APIInterrogator(api)
        self.cache_location = (
                pathlib.Path(cache_location)
                / api.hostname
                / (api.user_name or "anonymous")
        )
        self.cache_location.mkdir(parents=True, exist_ok=True)

        self._cache_fleeting = {
            "circles": [],
            "collections": [],
        }

        if api.user_id:
            mode = "user"
            user_data = api.get_user_dict()
        else:
            mode = "public"
            user_data = None

        self._mc = MetaCache(self.cache_location)
        self._mc_timestamp_path = self.cache_location / "cache_timestamp"
        self._mc_timestamp_path.touch()
        self._mc_version_path = self.cache_location / "cache_version"
        self._mc_version_path.touch()

        super(CachedAPIInterrogator, self).__init__(mode=mode,
                                                    user_data=user_data)

    @property
    def local_timestamp(self):
        return float(self._mc_timestamp_path.read_text().strip() or "0")

    @local_timestamp.setter
    def local_timestamp(self, timestamp):
        self._mc_timestamp_path.write_text(str(timestamp))

    @property
    def local_version_score(self):
        return int(self._mc_version_path.read_text().strip() or "0")

    def close(self):
        self._mc.close()

    def get_circles(self, refresh=False):
        """Return the list of DCOR Circle names
        """
        clist = self._cache_fleeting["circles"]
        if not clist or refresh:
            clist.clear()
            clist += self.ai.get_circles()
        return clist

    def get_collections(self, refresh=False):
        """Return the list of DCOR Collection names"""
        clist = self._cache_fleeting["collections"]
        if not clist or refresh:
            clist.clear()
            clist += self.ai.get_collections()
        return clist

    def get_datasets_user_following(self):
        """Return datasets the user is following"""
        # TODO: Use datasets in self._mc
        return self.ai.get_datasets_user_following()

    def get_datasets_user_owned(self):
        """Return datasets the user created"""
        # TODO: Use datasets in self._mc
        return self.ai.get_datasets_user_owned()

    def get_datasets_user_shared(self):
        """Return datasets shared with the user"""
        # TODO: Use datasets in self._mc
        return self.ai.get_datasets_user_shared()

    def get_users(self):
        """Return the list of DCOR users"""
        return self.ai.get_users()

    def search_dataset(self, query="", limit=100):
        """Search datasets via the CKAN API

        Parameters
        ----------
        query: str
            search query
        limit: int
            limit number of search results; Set to 0 to get all results
        """
        return DBExtract(self._mc.search(query, limit))

    def update(self, reset=False, progress_callback=None):
        """Update the local metadata cache based on the last local timestamp"""
        if self.remote_version_score != self.local_version_score:
            reset = True

        if reset:
            self.local_timestamp = 0
            self._mc.reset()

        circles = self.get_circles(refresh=True)
        self.get_collections(refresh=True)

        new_timestamp = time.time()
        for cc in circles:
            logger.info(f"Updating metadata cache for circle '{cc}'")
            for ds_dict in self.ai.search_dataset_via_api(
                    circles=[cc],
                    since_time=self.local_timestamp,
            ):
                self._mc.upsert_dataset(ds_dict)
        self.local_timestamp = new_timestamp
