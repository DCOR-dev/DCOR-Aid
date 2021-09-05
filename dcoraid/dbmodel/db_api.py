import urllib.parse
import warnings

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
        return data

    @ttl_cache(seconds=3600)
    def get_datasets_user_owned(self):
        """Get all the user's datasets"""
        numd = self.user_data["number_created_packages"]
        if numd > 1000:
            raise NotImplementedError(
                "Reached hard limit of 1000 results! "
                + "Please ask someone to implement this with `start`.")
        search = self.api.get("package_search",
                              q="*:*",
                              fq=f"creator_user_id:{self.user_data['id']}",
                              rows=numd+1)

        return search["results"]

    def get_datasets_user_shared(self):
        warnings.warn("`APIInterrogator.get_datasets_user_shared` "
                      + "not yet implemented!")
        return []

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

    def search_dataset(self, query, circles=None, collections=None):
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
            fq = f"({solr_circle_query}) AND ({solr_collections_query})"
        elif solr_circle_query:
            fq = f"{solr_circle_query}"
        elif solr_collections_query:
            fq = f"{solr_collections_query}"
        else:
            fq = ""

        data = self.api.get("package_search",
                            q=urllib.parse.quote(query, safe=""),
                            include_private=(self.mode == "user"),
                            fq=fq,
                            rows=100,
                            )
        return DBExtract(data["results"])

    @property
    def local_timestamp(self):
        """Local database date in seconds since epoch"""
        return self.remote_timestamp

    @property
    def local_version_score(self):
        """Local database version"""
        return self.remote_version_score
