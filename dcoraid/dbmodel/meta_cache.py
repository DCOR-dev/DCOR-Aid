from itertools import chain
import logging
import pathlib
import threading
import traceback
from typing import Any

import numpy as np

from .meta_cache_sqlite import SQLiteKeyJSONDatabase


logger = logging.getLogger(__name__)


BLOB_KEYS = [
    # dataset
    "authors",
    "creator_user_id",
    "doi",
    "id",
    "name",
    "notes",
    "title",
    # resource
    "resources",
    "dc:experiment:date",
    "dc:experiment:sample",
    "dc:setup:chip region",
    "dc:setup:identifier",
    "dc:setup:module composition",
    "description",
    # "id",  # duplicate
    # "name",  # duplicate
    "organization",
    # tags
    "tags",
    "display_name",
    # groups
    "groups",
    ]


class MetaCache:
    """Cache dataset dictionaries (metadata)

    The implementation uses an SQLite database which is loaded upon
    init and edited whenever data changes. For compute-intensive
    tasks (searching), the metadata are loaded into memory.

    Datasets are sorted according to "metadata_created", descending.
    """
    def __init__(self,
                 directory: str | pathlib.Path,
                 user_id: str = None,
                 org_ids: list[str] = None,
                 ) -> None:
        """
        Scan *directory* for ``org_*.db`` files, load all of them
        and fill the numpy backing store.

        Parameters
        ----------
        directory: str | pathlib.Path
            Path to the folder that will hold the ``org_<org_id>.db`` files.
            The folder is created automatically if it does not exist.
        user_id: str
            The ID of the user operating the database.
        org_ids: list[str]
            List of organization IDs that should be taken into consideration.
            If set to None (default), all databases in the `directory`
            are loaded.
        """
        self.base_dir = pathlib.Path(directory).expanduser().resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.user_id = user_id

        # The organization registry is a dictionary with organization IDs and
        # a list of dataset IDs as values.
        self._registry_org = {}

        #: Dictionary of databases for persistent storage
        self._databases = {}

        #: List of dataset dictionaries
        self.datasets = []

        #: List of dataset IDs, mapping database index to dataset ID
        self._dataset_ids = []

        #: Dict of dataset ID indices, mapping dataset ID to database index
        self._dataset_index_dict = {}

        #: List of booleans indicating whether dataset was created by the user
        self.datasets_user_owned = []

        # Search blob array
        self._srt_blobs = None

        self._lock = threading.Lock()

        with self._lock:
            self._initialize(org_ids)

    def _initialize(self, org_ids=None):
        # List of all dataset dictionaries (only used during init)
        datasets = []
        # List of search data (only used during init)
        rows: list[tuple] = []

        # Initialize the databases
        if org_ids is None:
            db_paths = list(self.base_dir.glob("org_*.db"))
        else:
            db_paths = [self.base_dir / f"org_{c}.db" for c in org_ids]

        for cp in db_paths:
            cid = cp.stem.split("_", 1)[-1]
            try:
                self._databases[cid] = SQLiteKeyJSONDatabase(cp)
            except BaseException:
                logger.error(
                    f"Recreating broken DB '{cp}': {traceback.format_exc()}")
                cp.unlink()
                self._databases[cid] = SQLiteKeyJSONDatabase(cp)

        # populate registry, dataset list, and search array data
        # initial blob size for dataset search
        blob_size = 256
        for db in self._databases.values():
            for ds_dict in db:
                ds_id: str = ds_dict.get("id", "")
                m_created: str = ds_dict.get("metadata_created", "")

                # Build a double-space‑separated string of **only values**
                value_blob = _create_blob_for_search(ds_dict)
                blob_size = max(blob_size, len(value_blob))

                rows.append((ds_id, m_created, value_blob))
                datasets.append(ds_dict)
                self._registry_org.setdefault(ds_dict["owner_org"],
                                              []).append(ds_dict["id"])

        # Convert the Python list of tuples into a NumPy 2‑D object array
        # TODO: If there are performance issues, we can create these arrays
        #       in chunks of e.g. 10000 in the above for-loop.
        data_dtype = [
            # "4b1f7c53-9d7f-2c53-aeb6-eaa8ecf10ca9"
            ("id", "<U36"),
            # 2025-09-18T08:09:52.947634
            ("created", "<U26"),
            # initialize with maximum size
            ("blob", f"<U{blob_size}")
        ]
        if rows:
            data = np.array(rows, dtype=data_dtype)   # shape (n_records, 3)
        else:
            # initialize emtpy cache
            data = np.empty(0, dtype=data_dtype)

        # Sort the dataset according to creation date, descending
        sort_idx = np.argsort(data["created"])[::-1]
        #: Blobs for searching, sorted by creation date descending
        self._srt_blobs = data[sort_idx]

        #: list of datasets, sorted by creation date descending
        self.datasets = [datasets[ii] for ii in sort_idx]

        #: List of booleans indicating whether dataset was created by the user
        self.datasets_user_owned = [
            ds_dict["creator_user_id"] == self.user_id
            for ds_dict in self.datasets
        ]

        #: List of dataset IDs
        self._dataset_ids = [ds_dict["id"] for ds_dict in self.datasets]

        #: Dict of dataset ID indices
        self._dataset_index_dict = {
            ds_id: ii for (ii, ds_id) in enumerate(self._dataset_ids)}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getitem__(self, ds_id):
        """Return the dataset dictionary given its ID"""
        return self.datasets[self._dataset_index_dict[ds_id]]

    def close(self):
        for db in self._databases.values():
            db.close()

    def reset(self):
        """Reset the entire database"""
        with self._lock:
            self.close()
            self._databases.clear()
            self.datasets.clear()
            self.datasets_user_owned.clear()
            self._dataset_ids.clear()
            self._dataset_index_dict.clear()
            self._registry_org.clear()
            for path in self.base_dir.glob("org_*.db"):
                path.unlink()
            self._initialize()

    def search(self,
               query: str,
               limit: int = None
               ) -> list[dict[str, Any]]:
        """
        Free‑text search across **all** cached datasets.

        The search is case‑insensitive and matches if the query appears
        anywhere in the JSON blob created from the database.

        Parameters
        ----------
        query: str
            Text to look for.
        limit: int | None, optional
            Maximum number of results to return. ``None`` (default) returns all
            matches.

        Returns
        -------
        List[dict]
            A list of the matching dataset dictionaries
        """
        norm_query = query.strip().lower()
        if not norm_query:
            return []  # empty query -> no results

        # `np.strings.find` works element‑wise on an array of strings
        # and returns the index of the first occurrence (or -1 if not found).
        # `self._srt_blobs[:, 2]` is the column that holds the lower‑cased
        # blobs.
        match_mask = np.strings.find(self._srt_blobs["blob"], norm_query) != -1

        if limit is not None:
            # Remove all items that are above a threshold.
            # TODO: apply the limit already during search by chunking?
            idx_lim = np.where(np.cumsum(match_mask) > limit)[0]
            if idx_lim.size:
                match_mask[idx_lim.min():] = False

        idx_result = np.where(match_mask)[0]

        return [self.datasets[idx] for idx in idx_result]

    def upsert_dataset(self, ds_dict: dict[str, Any]) -> None:
        """Insert a new dataset or update an existing one

        If the dataset `id` is not present in the cache, it is
        appended to the persistent SQLite database and to the in‑memory
        structures.

        If a dataset with the same `id` already exists, the record is
        replaced everywhere.

        Parameters
        ----------
        ds_dict : dict
            A CKAN‑style dataset dictionary.
        """
        ds_id = ds_dict.get("id")
        org_id = ds_dict.get("owner_org")

        # Is this dataset new?
        if ds_id not in self._registry_org.setdefault(org_id, []):
            # We have a new dataset
            with self._lock:
                self._upsert_dataset_insert(ds_dict)
        else:
            # We have an existing dataset
            if self._databases[org_id][ds_id] != ds_dict:
                # We have tp update the dataset
                with self._lock:
                    self._upsert_dataset_update(ds_dict)

    def _upsert_dataset_insert(self, ds_dict):
        """Insert a new dataset

        The search array will be rebuilt fully, because the order of
        datasets will change.
        """
        ds_id = ds_dict["id"]
        org_id = ds_dict["owner_org"]
        m_created = ds_dict["metadata_created"]

        blob = _create_blob_for_search(ds_dict)
        if len(blob) > int(self._srt_blobs.dtype["blob"].str[2:]):
            # Increase the search blob size.
            new_dtype = [("id", "<U36"),
                         ("created", "<U26"),
                         ("blob", f"<U{len(blob) + 10}")
                         ]
        else:
            new_dtype = self._srt_blobs.dtype

        # registry
        self._registry_org.setdefault(org_id, []).append(ds_id)

        # search array
        dates = np.array(self._srt_blobs["created"], copy=True)
        new_size = dates.size + 1
        new_idx = np.searchsorted(dates, m_created)
        # we can set refcheck to False, since we created a copy above
        dates.resize(new_size, refcheck=False)
        new_blobs = np.empty(new_size, dtype=new_dtype)
        new_blobs[:new_idx] = self._srt_blobs[:new_idx]
        new_blobs[new_idx] = (ds_id, m_created, blob)
        new_blobs[new_idx + 1:] = self._srt_blobs[new_idx:]
        self._srt_blobs = new_blobs

        # datasets
        self.datasets.insert(new_idx, ds_dict)

        # persistent database
        if org_id not in self._databases:
            self._databases[org_id] = SQLiteKeyJSONDatabase(
                db_name=self.base_dir / f"org_{org_id}.db")
        self._databases[org_id][ds_id] = ds_dict

        # user's dataset list
        self.datasets_user_owned.insert(
            new_idx,
            ds_dict["creator_user_id"] == self.user_id,
            )

        # dataset IDs
        self._dataset_ids.insert(new_idx, ds_id)

        # update the ds_id to index dictionary
        for idx in range(new_idx, new_size):
            self._dataset_index_dict[self._dataset_ids[idx]] = idx

    def _upsert_dataset_update(self, ds_dict):
        """Update an existing dataset

        If the new blob for `ds_dict` is larger than the existing,
        a new copy of the search array is created in memory.
        """
        ds_id = ds_dict["id"]
        org_id = ds_dict["owner_org"]

        # registry does not need updating (only contains ds_id)

        # Find the index in the database
        idx = np.where(self._srt_blobs["id"] == ds_id)[0][0]

        # search array
        blob = _create_blob_for_search(ds_dict)
        if len(blob) > int(self._srt_blobs.dtype["blob"].str[2:]):
            # Rewrite the search blobs, because this blob is bigger than any
            # of the blobs before.
            new_dtype = [("id", "<U36"),
                         ("created", "<U26"),
                         ("blob", f"<U{len(blob) + 10}")
                         ]
            self._srt_blobs = np.array(self._srt_blobs, dtype=new_dtype)
        self._srt_blobs["blob"][idx] = blob

        # cached datasets
        self.datasets[idx] = ds_dict

        # persistent database
        self._databases[org_id][ds_id] = ds_dict

    def upsert_many(self, dataset_dicts, org_id=None):
        """Insert or update multiple datasets at once

        The implementation is faster when the organization ID `org_id`
        is specified. The recommended workflow is to query the DCOR
        server by organization.
        """
        if self._registry_org.get(org_id):
            # Separate the datasets into new and existing datasets.
            ds_list_update = [ds_dict for ds_dict in dataset_dicts
                              if ds_dict["id"] in self._dataset_ids]
        else:
            # We have not seen this organization before
            ds_list_update = []

        if not ds_list_update:
            # Nothing needs to be updated, and all datasets are inserted.
            ds_list_insert = dataset_dicts
        else:
            ds_list_insert = [ds_dict for ds_dict in dataset_dicts
                              if ds_dict["id"] not in self._dataset_ids]

        # Update datasets
        for ds_dict in ds_list_update:
            if self._databases[org_id][ds_dict["id"]] != ds_dict:
                self._upsert_dataset_update(ds_dict)

        if ds_list_insert:
            # Insert datasets by organization
            if org_id is not None:
                # All datasets belong to one organization
                self._upsert_many_insert(org_id=org_id,
                                         dataset_dicts=ds_list_insert)
            else:
                # Iterate over all organization IDs in dataset_dicts
                org_dict = {}
                for ds_dict in ds_list_insert:
                    org_dict.setdefault(ds_dict["owner_org"],
                                        []).append(ds_dict)
                for org_id, ds_list in org_dict.items():
                    self._upsert_many_insert(org_id=org_id,
                                             dataset_dicts=ds_list)

    def _upsert_many_insert(self, org_id, dataset_dicts):
        """Insert multiple datasets at once for one organization

        This is essentially the vectorization of
        :meth:`MetaCache._upsert_dataset_insert`.
        This method MUST NOT be used for "updating" datasets.
        """
        ds_ids = [ds_dict["id"] for ds_dict in dataset_dicts]
        ms_created = [ds_dict["metadata_created"] for ds_dict in dataset_dicts]
        blobs_new = [
            _create_blob_for_search(ds_dict) for ds_dict in dataset_dicts]
        blob_max_len = max(len(b) for b in blobs_new)

        if blob_max_len > int(self._srt_blobs.dtype["blob"].str[2:]):
            # Increase the search blob size.
            new_dtype = [("id", "<U36"),
                         ("created", "<U26"),
                         ("blob", f"<U{blob_max_len + 10}")
                         ]
        else:
            new_dtype = self._srt_blobs.dtype

        # registry
        self._registry_org.setdefault(org_id, []).__add__(ds_ids)

        # search array
        dates_cur = np.array(self._srt_blobs["created"], copy=True)
        size_old = dates_cur.size
        dates_new = np.array(ms_created)
        dates_comb = np.concatenate((dates_cur, dates_new))
        # sort according to dates descending
        sorter = np.argsort(dates_comb)[::-1]

        new_blobs = np.empty(dates_comb.size, dtype=new_dtype)
        new_blobs[:size_old] = self._srt_blobs["blob"]
        new_blobs["blob"][size_old:] = blobs_new
        new_blobs["created"][size_old:] = dates_new
        new_blobs["id"][size_old:] = ds_ids
        new_blobs = new_blobs[sorter]

        self._srt_blobs = new_blobs

        # datasets
        datasets_unsrt = self.datasets + dataset_dicts
        self.datasets = [datasets_unsrt[ii] for ii in sorter]

        # persistent database
        if org_id not in self._databases:
            self._databases[org_id] = SQLiteKeyJSONDatabase(
                db_name=self.base_dir / f"org_{org_id}.db")
        self._databases[org_id].insert_many(dataset_dicts)

        # user's dataset list
        datasets_user_owned_unsrt = (
            self.datasets_user_owned
            + [ds["creator_user_id"] == self.user_id for ds in dataset_dicts]
        )
        self.datasets_user_owned = [
            datasets_user_owned_unsrt[ii] for ii in sorter]

        # dataset IDs
        dataset_ids_unsr = self._dataset_ids + ds_ids
        self._dataset_ids = [dataset_ids_unsr[ii] for ii in sorter]

        for (idx, ds_id) in enumerate(self._dataset_ids):
            self._dataset_index_dict[ds_id] = idx


def _create_blob_for_search(ds_dict: dict) -> str:
    """Create a string blob from a dataset dictionary for free text search"""
    values = _values_only(ds_dict, BLOB_KEYS)
    value_blob: str = "  ".join(values).lower()
    return value_blob


def _values_only(obj: Any,
                 only_keys: list[str],
                 ) -> list[str]:
    """
    Recursively walk a JSON‑compatible object and return a flat list of the
    string representation of all values whose keys are in `only_keys`.
    Lists, tuples and dicts are traversed; other scalar types are converted
    with ``str``.
    """
    if isinstance(obj, dict):
        return chain.from_iterable(
            [_values_only(v, only_keys) for (k, v) in obj.items()
             if k in only_keys])
        # for k, v in obj.items():
        #     if k in only_keys:
        #         vals += _values_only(v, only_keys)
    elif isinstance(obj, (list, tuple)):
        return chain.from_iterable([_values_only(v, only_keys) for v in obj])
        # for v in obj:
        #    vals += _values_only(v, only_keys)
    else:
        # scalar (str, int, float, bool, None)
        if obj not in [None, ""]:
            return [str(obj)]
        else:
            return []
