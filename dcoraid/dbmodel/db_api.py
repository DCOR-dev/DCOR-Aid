import urllib.parse

import numpy as np

from ..common import ttl_cache

from .db_core import DBInterrogator
from .extract import DBExtract


class APIInterrogator(DBInterrogator):
    def __init__(self, api, mode="public"):
        self.api = api.copy()
        if mode == "user":
            user_data = api.get_user_dict()
        else:
            user_data = None
        super(APIInterrogator, self).__init__(mode=mode, user_data=user_data)

    @ttl_cache(seconds=5)
    def get_circles(self):
        """Return the list of DCOR Circles
        """
        if self.mode == "user":
            # Organizations the user is a member of
            data = self.api.get("organization_list_for_user",
                                permission="read")
        else:
            data = self.api.get("organization_list")
        return data

    @ttl_cache(seconds=5)
    def get_collections(self):
        """Return the list of DCOR Collections"""
        if self.mode == "user":
            data = self.api.get("group_list_authz", am_member=True)
        else:
            data = self.api.get("group_list")
        return data

    @ttl_cache(seconds=3600)
    def get_datasets_user_following(self):
        data = self.api.get("dataset_followee_list",
                            id=self.user_data["name"])
        return DBExtract(data)

    @ttl_cache(seconds=3600)
    def get_datasets_user_owned(self):
        """Get all the user's datasets"""
        numd = self.user_data["number_created_packages"]
        if numd >= 1000:
            raise NotImplementedError(
                "Reached hard limit of 1000 results! "
                + "Please ask someone to implement this with `start`.")
        search = self.api.get("package_search",
                              q="*:*",
                              fq=f"creator_user_id:{self.user_data['id']}",
                              rows=numd+1)

        return DBExtract(search["results"])

    def get_datasets_user_shared(self):
        # TODO:
        # - package_collaborator_list_for_user
        #   - https://github.com/DCOR-dev/DCOR-Aid/issues/32
        #   - https://github.com/DCOR-dev/ckanext-dcor_schemas/issues/10

        # get circles the user is a member of
        circles = self.api.get("organization_list",
                               limit=1000,
                               all_fields=True,
                               include_users=True)
        if len(circles) >= 1000:
            raise NotImplementedError(
                "Reached hard limit of 1000 results! "
                + "Please ask someone to implement this with `offset`.")
        user_circles = []
        for circ in circles:
            for user in circ["users"]:
                if user["id"] == self.api.user_id:
                    user_circles.append(circ["name"])
        # get collections the user is a member of
        collections = self.api.get("group_list",
                                   limit=1000,
                                   all_fields=True,
                                   include_users=True)
        if len(collections) >= 1000:
            raise NotImplementedError(
                "Reached hard limit of 1000 results! "
                + "Please ask someone to implement this with `offset`.")
        user_collections = []
        for coll in collections:
            for user in coll["users"]:
                if user["id"] == self.api.user_id:
                    user_collections.append(coll["name"])

        # perform a dataset search with those circles and collections
        datasets = self.search_dataset(
            circles=user_circles,
            collections=user_collections,
            circle_collection_union=True,
            limit=0,
        )
        return datasets

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

    def search_dataset(self, query="*:*", circles=None, collections=None,
                       circle_collection_union=False, limit=100):
        """Search datasets via the CKAN API

        Parameters
        ----------
        query: str
            search query
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
        # https://docs.ckan.org/en/latest/user-guide.html#search-in-detail
        if circles:
            solr_circles = ["organization:{}".format(ci) for ci in circles]
            solr_circle_query = " OR ".join(solr_circles)
        else:
            solr_circle_query = None

        if collections:
            solr_collections = ["groups:{}".format(co) for co in collections]
            solr_collections_query = " OR ".join(solr_collections)
        else:
            solr_collections_query = None

        if solr_circle_query and solr_collections_query:
            if circle_collection_union:
                fq = f"({solr_circle_query}) OR ({solr_collections_query})"
            else:
                fq = f"({solr_circle_query}) AND ({solr_collections_query})"
        elif solr_circle_query:
            fq = f"{solr_circle_query}"
        elif solr_collections_query:
            fq = f"{solr_collections_query}"
        else:
            fq = ""

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
                                fq=fq,
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
