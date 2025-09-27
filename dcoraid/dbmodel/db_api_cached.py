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
            user_data = api.get_user_dict()
        else:
            user_data = None

        self._mc = MetaCache(directory=self.cache_location,
                             user_id=self.api.user_id,
                             )
        self._mc_timestamp_path = self.cache_location / "cache_timestamp"
        self._mc_timestamp_path.touch()
        self._mc_version_path = self.cache_location / "cache_version"
        self._mc_version_path.touch()

        super(CachedAPIInterrogator, self).__init__(user_data=user_data)

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

    def get_circles(self):
        """Return the list of DCOR Circle names
        """
        # TODO: Use self._mc to extract circles
        clist = self._cache_fleeting.setdefault("circles", [])
        if not clist:
            clist.clear()
            clist += self.ai.get_circles()
        return clist

    def get_collections(self):
        """Return the list of DCOR Collection names"""
        # TODO: Use self._mc to extract collections
        clist = self._cache_fleeting.setdefault("collections", [])
        if not clist:
            clist.clear()
            clist += self.ai.get_collections()
        return clist

    def get_dataset_dict(self, ds_id):
        return self._mc[ds_id]

    def get_datasets_user_following(self) -> DBExtract:
        """Return datasets the user is following"""
        # TODO: Use datasets in self._mc
        ckey = "get_datasets_user_following"
        if self._cache_fleeting.get(ckey) is None:
            self._cache_fleeting[ckey] = self.ai.get_datasets_user_following()
        return self._cache_fleeting[ckey]

    def get_datasets_user_owned(self) -> DBExtract:
        """Return datasets the user created"""
        own_list = self._mc.datasets_user_owned
        ds_list = self._mc.datasets
        owned = [ds for (ds, byuser) in zip(ds_list, own_list) if byuser]
        return DBExtract(owned)

    def get_datasets_user_shared(self) -> DBExtract:
        """Return datasets shared with the user"""
        # TODO: Use datasets in self._mc
        ckey = "get_datasets_user_shared"
        if self._cache_fleeting.get(ckey) is None:
            self._cache_fleeting[ckey] = self.ai.get_datasets_user_shared()
        return self._cache_fleeting[ckey]

    def get_users(self):
        """Return the list of DCOR users"""
        return self.ai.get_users()

    def reset_cache(self):
        self._mc_timestamp_path.unlink()
        self._mc_version_path.unlink()
        self._mc_timestamp_path.touch()
        self._mc_version_path.touch()
        self._mc.reset()

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

    def update(self, reset=False, abort_event=None, callback=None):
        """Update the local metadata cache based on the last local timestamp"""
        # Clear the fleeting cache.
        self._cache_fleeting.clear()
        if self.remote_version_score != self.local_version_score:
            reset = True

        if reset:
            self.local_timestamp = 0
            self._mc.reset()

        # Call these methods now so they reflect the current database state.
        circles = self.get_circles()
        collections = self.get_collections()

        new_timestamp = time.time()

        datasets_new = 0

        for cdict in circles:
            if callback is not None:
                callback({
                    "circles": circles,
                    "collections": collections,
                    "datasets_new": datasets_new,
                    "circle_current": cdict,
                })
            logger.info(f"Fetching dataset from circle {cdict['name']}")
            dbe = self.ai.search_dataset_via_api(
                since_time=self.local_timestamp,
                circles=[cdict["name"]],
                limit=0,
                ret_db_extract=False,
            )
            datasets_new += len(dbe)
            if abort_event and abort_event.is_set():
                break
            self._mc.upsert_many(dbe, org_id=cdict["id"])
            logger.info(
                f"Loaded {len(dbe)} datasets from circle {cdict['name']}")
        else:
            # Only update the local timestamp if we actually did
            # update the local database.
            self.local_timestamp = new_timestamp

    def update_dataset(self, ds_dict):
        """Update a single dataset in the database without calling `update`"""
        self._cache_fleeting.clear()
        self._mc.upsert_dataset(ds_dict)
