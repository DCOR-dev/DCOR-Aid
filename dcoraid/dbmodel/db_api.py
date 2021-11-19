import urllib.parse

import numpy as np

from ..common import ttl_cache

from .db_core import DBInterrogator
from .extract import DBExtract


class APIInterrogator(DBInterrogator):
    def __init__(self, api):
        self.api = api.copy()
        if api.user_id:
            mode = "user"
            user_data = api.get_user_dict()
        else:
            mode = "public"
            user_data = None
        super(APIInterrogator, self).__init__(mode=mode, user_data=user_data)

    @ttl_cache(seconds=5)
    def get_circles(self):
        """Return the list of DCOR Circle names
        """
        if self.mode == "user":
            # Organizations the user is a member of
            circle_dict = self.api.get("organization_list_for_user",
                                       id=self.api.user_id,
                                       permission="read")
            data = [dd["name"] for dd in circle_dict]
        else:
            data = self.api.get("organization_list")
        return data

    @ttl_cache(seconds=5)
    def get_collections(self):
        """Return the list of DCOR Collection names"""
        if self.mode == "user":
            collection_dict = self.api.get("group_list_authz", am_member=True)
            data = [dd["name"] for dd in collection_dict]
        else:
            data = self.api.get("group_list")
        if len(data) == 1000:
            raise NotImplementedError(
                "Reached hard limit of 1000 results! "
                + "Please ask someone to implement this with `offset`.")
        return data

    @ttl_cache(seconds=3600)
    def get_datasets_user_following(self):
        """Return datasets the user is following"""
        assert self.mode == "user"
        data = self.api.get("dataset_followee_list", id=self.api.user_name)
        return DBExtract(data)

    @ttl_cache(seconds=3600)
    def get_datasets_user_owned(self):
        """Return datasets the user created"""
        assert self.mode == "user"
        dbextract = self.search_dataset(
            filter_queries=[f"+creator_user_id:{self.api.user_id}"],
            limit=0,
        )
        return dbextract

    def get_datasets_user_shared(self):
        """Return datasets shared with the user"""
        assert self.mode == "user"
        # perform a dataset search with all circles and collections
        dbextract = self.search_dataset(
            circles=self.get_circles(),
            collections=self.get_collections(),
            circle_collection_union=True,
            filter_queries=[f"-creator_user_id:{self.api.user_id}"],
            limit=0,
        )

        # all packages the user is a collaborator in
        collaborated = self.api.get("package_collaborator_list_for_user",
                                    id=self.user_data["id"])
        for col in collaborated:
            if col["package_id"] not in dbextract:
                ds_dict = self.api.get("package_show", id=col["package_id"])
                dbextract.add_datasets([ds_dict])

        return dbextract

    @ttl_cache(seconds=3600)
    def get_users(self, ret_fullnames=False):
        """Return the list of DCOR users"""
        data = self.api.get("user_list")
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

    def search_dataset(self, query="*:*", filter_queries=None, circles=None,
                       collections=None, circle_collection_union=False,
                       limit=100):
        """Search datasets via the CKAN API

        Parameters
        ----------
        query: str
            search query
        filter_queries: list of str
            SOLR `fq` filter queries (are joined with 'AND')
        circles: list of str
            list of circles (organizations) to search in
        collections: list of str
            list of collections (groups) to search in
        circle_collection_union: bool
            If set to True, make a union of the circle and collection
            sets. Otherwise (default), search only for datasets that
            are are at least member of one of the circles and one of the
            collections.
        limit: int
            limit number of search results; Set to 0 to get all results
        """
        if filter_queries is None:
            filter_queries = []
        # https://docs.ckan.org/en/latest/user-guide.html#search-in-detail
        if circles:
            solr_circles = ["organization:{}".format(ci) for ci in circles]
            if len(circles) == 1:
                solr_circle_query = solr_circles[0]
            else:
                solr_circle_query = f"({' OR '.join(solr_circles)})"
        else:
            solr_circle_query = None

        if collections:
            solr_collections = ["groups:{}".format(co) for co in collections]
            if len(collections) == 1:
                solr_collections_query = solr_collections[0]
            else:
                solr_collections_query = f"({' OR '.join(solr_collections)})"
        else:
            solr_collections_query = None

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
        dbextract = DBExtract()
        while num_retrieved < min(limit, num_total) and rows:
            data = self.api.get("package_search",
                                q=urllib.parse.quote(query, safe=""),
                                fq=final_fq,
                                include_private=(self.mode == "user"),
                                rows=rows,
                                start=num_retrieved,
                                )
            if np.isinf(num_total):
                # first iteration
                num_total = data["count"]
            num_retrieved += len(data["results"])
            if num_retrieved + rows > min(limit, num_total):
                # in the next iteration, only get the final
                # few results.
                rows = num_total - num_retrieved
            dbextract.add_datasets(data["results"])

        return dbextract

    @property
    def local_timestamp(self):
        """Local database date in seconds since epoch"""
        return self.remote_timestamp

    @property
    def local_version_score(self):
        """Local database version"""
        return self.remote_version_score
