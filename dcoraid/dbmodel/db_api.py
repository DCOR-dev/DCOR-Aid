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
        super(APIInterrogator, self).__init__(user_data=user_data)

    def get_circles(self):
        """Return the list of DCOR Circle names
        """
        return self.api.get("organization_list", all_fields=True)

    def get_collections(self):
        """Return the list of DCOR Collection names"""
        data = self.api.get("group_list", all_fields=True, limit=1000)
        if len(data) == 1000:
            raise NotImplementedError(
                "Reached hard limit of 1000 results! "
                + "Please ask someone to implement this with `offset`.")
        return data

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

            for circles_batch in batched(self.get_circles(), 20):
                dbe += self.search_dataset_via_api(
                    circles=[c["name"] for c in circles_batch],
                    filter_queries=[f"-creator_user_id:{self.api.user_id}"],
                    limit=0,
                    )

            for collections_batch in batched(self.get_collections(), 20):
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
                       text: str,
                       limit: int = 100):
        """Free text search for a dataset in the database

        Parameters
        ----------
        text: str
            text to search for
        limit: int
            number of results to return
        """
        return self.search_dataset_via_api(query=text, limit=limit)

    def search_dataset_via_api(self,
                               query: str = "*:*",
                               filter_queries: list[str] = None,
                               circles: list[str] = None,
                               collections: list[str] = None,
                               circle_collection_union: bool = False,
                               since_time: float = None,
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
            rows = 1000  # default batch size
            limit = np.inf
        else:
            rows = min(1000, limit)

        num_total = np.inf  # just the initial value
        num_retrieved = 0
        if ret_db_extract:
            dbe = DBExtract()
        else:
            dbe = []
        while start + num_retrieved < min(start + limit, num_total) and rows:
            data = self.api.get(
                "package_search",
                q=urllib.parse.quote(query, safe=""),
                fq=final_fq,
                include_private=bool(self.api.user_id is not None),
                rows=rows,
                sort=sort_solr,
                start=start + num_retrieved,
                )
            if np.isinf(num_total):
                # first iteration
                num_total = data["count"]
            num_retrieved += len(data["results"])
            if num_retrieved + rows > min(limit, num_total):
                # in the next iteration, only get the final
                # few results.
                rows = num_total - num_retrieved
            dbe += data["results"]

        return dbe

    def update(self, reset=False):
        """Ignored, since no local database exists"""
        pass

    @property
    def local_timestamp(self):
        """Local database date in seconds since epoch"""
        return self.remote_timestamp

    @property
    def local_version_score(self):
        """Local database version"""
        return self.remote_version_score
