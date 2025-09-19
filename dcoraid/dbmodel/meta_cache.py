import logging
import pathlib
import traceback
from typing import Any

import numpy as np

from .meta_cache_sqlite import SQLiteKeyJSONDatabase


logger = logging.getLogger(__name__)


class MetaCache:
    """Cache dataset dictionaries (metadata)

    The implementation uses an SQLite database which is loaded upon
    init and edited whenever data changes. For compute-intensive
    tasks (searching), the metadata are loaded into memory.
    """
    def __init__(self,
                 directory: str | pathlib.Path,
                 circle_ids: list[str] = None,
                 ) -> None:
        """
        Scan *directory* for ``circle_*.db`` files, load all of them
        and fill the numpy backing store.

        Parameters
        ----------
        directory : str | pathlib.Path
            Path to the folder that will hold the ``circle_<org_id>.db`` files.
            The folder is created automatically if it does not exist.
        circle_ids : list[str]
            List of circle IDs that should be taken into consideration.
            If set to None (default), all databases in the `directory`
            are loaded.
        """
        self.base_dir = pathlib.Path(directory).expanduser().resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # The registry is a dictionary with circle IDs and a list of
        # dataset IDs as values.
        self._registry_org = {}

        # List of all dataset dictionaries (only used during init)
        datasets = []
        # List of search data (only used during init)
        rows: list[tuple] = []

        # Initialize the databases
        if circle_ids is None:
            db_paths = list(self.base_dir.glob("circle_*.db"))
        else:
            db_paths = [self.base_dir / f"circle_{c}.db" for c in circle_ids]
        # Dictionary of databases for persistent storage
        self._databases = {}
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        for db in self._databases.values():
            db.close()
        self._databases.clear()

    def destroy(self):
        self.close()

        for path in self.base_dir.glob("circle_*.db"):
            path.unlink()

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
            self._upsert_dataset_insert(ds_dict)
        else:
            # We have an existing dataset
            if self._databases[org_id][ds_id] != ds_dict:
                # We have tp update the dataset
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
        dates.resize(new_size)
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
                db_name=self.base_dir / f"circle_{org_id}.db")
        self._databases[org_id][ds_id] = ds_dict

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


def _create_blob_for_search(ds_dict):
    """Create a string blob from a dataset dictionary for free text search"""
    values = _values_only(ds_dict,
                          only_keys=[
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
                          ])
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
    vals = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in only_keys:
                vals += _values_only(v, only_keys)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            vals += _values_only(v, only_keys)
    else:
        # scalar (str, int, float, bool, None)
        if obj not in [None, ""]:
            vals.append(str(obj))
    return vals
