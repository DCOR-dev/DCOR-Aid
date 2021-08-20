import urllib.parse
import warnings

from .core import DBInterrogator, DBModel
from .util import ttl_cache


class APIModel(DBModel):
    def __init__(self, api, *args, **kwargs):
        """A CKAN-API-based database model"""
        db = APIInterrogator(api=api)
        super(APIModel, self).__init__(interrogator=db, *args, **kwargs)


class APIInterrogator(DBInterrogator):
    def __init__(self, api):
        self.api = api.copy()
        super(APIInterrogator, self).__init__()

    @ttl_cache(seconds=5)
    def get_circles(self, mode="public"):
        """Return the list of DCOR Circles
        """
        if mode == "user":
            # Organizations the user is a member of
            data = self.api.get("organization_list_for_user",
                                permission="read")
        else:
            data = self.api.get("organization_list")
        return data

    @ttl_cache(seconds=5)
    def get_collections(self, mode="public"):
        """Return the list of DCOR Collections"""
        if mode == "user":
            data = self.api.get("group_list_authz", am_member=True)
        else:
            data = self.api.get("group_list")
        return data

    @ttl_cache(seconds=3600)
    def get_datasets_user_following(self):
        user_data = self.get_user_data()
        data = self.api.get("dataset_followee_list",
                            id=user_data["name"])
        return data

    @ttl_cache(seconds=3600)
    def get_datasets_user_owned(self):
        """Get all the user's datasets"""
        user_data = self.get_user_data()
        numd = user_data["number_created_packages"]
        if numd > 1000:
            raise NotImplementedError(
                "Reached hard limit of 1000 results! "
                + "Please ask someone to implement this with `start`.")
        data2 = self.api.get("package_search",
                             q="*:*",
                             fq="creator_user_id:{}".format(user_data["id"]),
                             rows=numd+1)
        # Hello, I removed this check, because there were race conditions
        # during (parallel on multiple workers) testing on GH Actions.
        # if data2["count"] != numd:
        #     raise ValueError("Number of user datasets don't match "
        #                      + f"(expected {numd}, got {data2['count']})!")

        return data2["results"]

    def get_datasets_user_shared(self):
        warnings.warn("`APIInterrogator.get_datasets_user_shared` "
                      + "not yet implemented!")
        return []

    def get_license_list(self):
        """Return the servers license list"""
        return self.api.get_license_list()

    def get_supplementary_resource_schema(self):
        """Return the servers supplementary resource schema"""
        return self.api.get_supplementary_resource_schema()

    def get_supported_resource_suffixes(self):
        """Return the servers supported resource suffixes"""
        return self.api.get_supported_resource_suffixes()

    @ttl_cache(seconds=3600)
    def get_user_data(self):
        """Return the current user data dictionary"""
        return self.api.get_user_dict()

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

    def search_dataset(self, query, circles, collections, mode="public"):
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
                            include_private=(mode == "user"),
                            fq=fq,
                            rows=100,
                            )
        return data["results"]

    @property
    def local_timestamp(self):
        """Local database date in seconds since epoch"""
        return self.remote_timestamp

    @property
    def local_version_score(self):
        """Local database version"""
        return self.remote_version_score
