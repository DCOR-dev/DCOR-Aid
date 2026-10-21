from __future__ import annotations

from itertools import islice
import sys
import time
import urllib.parse

import numpy as np

from .db_core import DBInterrogator
from .extract import DBExtract


if sys.version_info >= (3, 12):
    from itertools import batched
else:
    def batched(iterable, chunk_size):
        iterator = iter(iterable)
        while chunk := tuple(islice(iterator, chunk_size)):
            yield chunk


class APIInterrogator(DBInterrogator):
    def __init__(self, api):
        self.api = api.copy()
        if api.user_id:
            user_data = api.get_user_dict()
        else:
            user_data = None
        super().__init__(user_data=user_data)

    def get_circles(self):
        """Return the list of all DCOR Circle dictionaries"""
        return self._get_list_all_fields("organization_list")

    def get_circles_user(self):
        """Return the list of DCOR Circle dictionaries the user is member of
        """
        if self.api.user_id is None:
            return []
        return self.api.get("organization_list_for_user", permission="read")

    def get_collections(self):
        """Return the list of all DCOR Collection dictionaries"""
        return self._get_list_all_fields("group_list")

    def get_collections_user(self):
        """Return the list of DCOR Collection dicts the user is member of

        This includes all collections in which the user has any role
        (also "member").
        """
        # CKAN's `group_list_authz` only returns the collections that the
        # user may edit, and there is no API call that returns the
        # collections in which the user is a "member". Therefore, we have
        # to check the members of every collection.
        user_id = self.api.user_id
        if user_id is None:
            return []
        collections = []
        for col in self.get_collections():
            members = self.api.get("member_list",
                                   id=col["id"],
                                   object_type="user")
            if any(member[0] == user_id for member in members):
                collections.append(col)
        return collections

    def _get_list_all_fields(self, api_call):
        """Return the full result of a CKAN list call with `all_fields=True`

        CKAN returns at most 25 items for e.g. `organization_list` and
        `group_list` when `all_fields=True` is set (configuration option
        `ckan.group_and_organization_list_all_fields_max`), so we have to
        page through the results. The results are sorted by the unique
        name, because the default sorting by title is not stable across
        pages (e.g. for empty titles).
        """
        items = []
        ids = set()
        offset = 0
        while True:
            data = self.api.get(api_call,
                                all_fields=True,
                                sort="name asc",
                                limit=25,
                                offset=offset)
            if not data:
                break
            offset += len(data)
            for item in data:
                # Items could be listed twice if an item was created
                # in-between requests.
                if item["id"] not in ids:
                    ids.add(item["id"])
                    items.append(item)
        return items

    def get_datasets_user_following(self):
        """Return datasets the user is following"""
        if self.api.user_id is not None:
            data = self.api.get("dataset_followee_list", id=self.api.user_name)
        else:
            data = []
        return DBExtract(data)

    def get_datasets_user_owned(self):
        """Return datasets the user created"""
        if self.api.user_id is not None:
            dbe = self.search_dataset_via_api(
                filter_queries=[f"+creator_user_id:{self.api.user_id}"],
                limit=0,
            )
            assert isinstance(dbe, DBExtract)
        else:
            dbe = DBExtract()
        return dbe

    def get_datasets_user_shared(self):
        """Return datasets shared with the user"""
        # Perform a dataset search with all circles and collections.
        # This search may become too large (414 Request-URI Too Large).
        # Limit the search to 20 circles/collections.
        dbe = DBExtract()

        if self.api.user_id is not None:

            for circles_batch in batched(self.get_circles_user(), 20):
                dbe += self.search_dataset_via_api(
                    circles=[c["name"] for c in circles_batch],
                    filter_queries=[f"-creator_user_id:{self.api.user_id}"],
                    limit=0,
                    )

            for collections_batch in batched(self.get_collections_user(),
                                             20):
                dbe += self.search_dataset_via_api(
                    collections=[c["name"] for c in collections_batch],
                    filter_queries=[f"-creator_user_id:{self.api.user_id}"],
                    limit=0,
                    )

            # all packages the user is a collaborator in
            collaborated = self.api.get("package_collaborator_list_for_user",
                                        id=self.user_data["id"])
            for col in collaborated:
                if col["package_id"] not in dbe:
                    ds_dict = self.api.get("package_show",
                                           id=col["package_id"])
                    dbe.add_datasets([ds_dict])

        return dbe

    def get_users(self, ret_fullnames=False):
        """Return the list of DCOR users"""
        data = self.api.get("user_autocomplete", q="")
        user_list = []
        full_list = []
        for dd in data:
            user_list.append(dd["name"])
            if dd["fullname"]:
                full_list.append(dd["fullname"])
            else:
                full_list.append(dd["name"])
        if ret_fullnames:
            return user_list, full_list
        else:
            return user_list

    def search_dataset(self,
                       query: str,
                       limit: int = 100):
        """Free text search for a dataset in the database

        Parameters
        ----------
        text: str
            text to search for
        limit: int
            number of results to return
        """
        data = self.search_dataset_via_api(query=query, limit=limit)
        assert isinstance(data, DBExtract)
        return data

    def search_dataset_via_api(self,
                               query: str = "*:*",
                               filter_queries: list[str] | None = None,
                               circles: list[str] | None = None,
                               collections: list[str] | None = None,
                               circle_collection_union: bool = False,
                               since_time: float | None = None,
                               sort_solr: str = "metadata_created desc",
                               start: int = 0,
                               limit: int = 100,
                               ret_db_extract: bool = True,
                               ):
        """Search datasets via the CKAN API

        Parameters
        ----------
        query: str
            search query
        filter_queries: list of str
            SOLR `fq` filter queries (are joined with 'AND'). The `circles`,
            `collections`, `circle_collection_union`, and `since_date`
            convenience kwargs are appended to the query list.
        circles: list of str
            list of circles (organizations) to search in
        collections: list of str
            list of collections (groups) to search in
        circle_collection_union: bool
            If set to True, make a union of the circle and collection
            sets. Otherwise (default), search only for datasets that
            are at least member of one of the circles and one of the
            collections.
        since_time: float
            Return only datasets that have been modified after this time
            since the epoch.
        sort_solr: str
            SOLR search ordering. By default, sort according to dataset
            creation date `'metadata_created desc'`. The CKAN default is
            `'score desc, metadata_modified desc'`.
            https://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.get.package_search
        start: int
            The offset in the complete result for where the set of
            returned datasets should begin.
        limit: int
            limit number of search results; Set to 0 to get all results
        ret_db_extract: bool
            whether to return an instance of :class:`DBExtract`; if set to
            `False`, then a list of datasets is returned instead which is
            faster.
        """
        if filter_queries is None:
            filter_queries = []
        # https://docs.ckan.org/en/latest/user-guide.html#search-in-detail
        if circles:
            solr_circles = [f"organization:{ci}" for ci in circles]
            if len(circles) == 1:
                solr_circle_query = solr_circles[0]
            else:
                solr_circle_query = f"({' OR '.join(solr_circles)})"
        else:
            solr_circle_query = None

        if collections:
            solr_collections = [f"groups:{co}" for co in collections]
            if len(collections) == 1:
                solr_collections_query = solr_collections[0]
            else:
                solr_collections_query = f"({' OR '.join(solr_collections)})"
        else:
            solr_collections_query = None

        # collections and/or circles filter query
        if solr_circle_query and solr_collections_query:
            if circle_collection_union:
                fq = f"({solr_circle_query} OR {solr_collections_query})"
            else:
                fq = f"({solr_circle_query} AND {solr_collections_query})"
        elif solr_circle_query:
            fq = f"{solr_circle_query}"
        elif solr_collections_query:
            fq = f"{solr_collections_query}"
        else:
            fq = ""
        if fq:
            filter_queries.append(fq)

        # time filter query
        if since_time is not None:
            gm_time_str = time.strftime(r"%Y-%m-%dT%H\:%M\:%SZ",
                                        time.gmtime(since_time))
            # Use "metadata_modified", since datasets that were previously
            # drafts or private datasets made public would not show up
            # if "metadata_created" was used.
            filter_queries.append(f"metadata_modified:[{gm_time_str} TO NOW]")

        if len(filter_queries) == 0:
            final_fq = ""
        elif len(filter_queries) == 1:
            final_fq = filter_queries[0]
        else:
            final_fq = f"({' AND '.join(filter_queries)})"

        if limit < 0:
            raise ValueError(f"`limit` must be 0 or >0!, got {limit}!")
        elif limit == 0:
            limit = np.inf  # type: ignore

        # Number of datasets to retrieve. This is updated with the total
        # number of matching datasets after the first request.
        num_wanted = limit
        num_total = None
        num_retrieved = 0
        if ret_db_extract:
            dbe = DBExtract()
        else:
            dbe = []
        while num_retrieved < num_wanted:
            data = self.api.get(
                "package_search",
                q=urllib.parse.quote(query, safe=""),
                fq=final_fq,
                include_private=bool(self.api.user_id is not None),
                # CKAN returns at most 1000 datasets per request
                rows=min(1000, num_wanted - num_retrieved),
                sort=sort_solr,
                start=start + num_retrieved,
                )
            if num_total is None:
                # first iteration
                num_total = data["count"]
                num_wanted = min(limit, max(0, num_total - start))
            if not data["results"]:
                # Datasets might have been deleted in-between requests.
                break
            num_retrieved += len(data["results"])
            dbe += data["results"]

        return dbe

    def update(self, reset=False):
        """Ignored, since no local database exists"""

    @property
    def local_timestamp(self):
        """Local database date in seconds since epoch"""
        return self.remote_timestamp

    @property
    def local_version_score(self):
        """Local database version"""
        return self.remote_version_score
