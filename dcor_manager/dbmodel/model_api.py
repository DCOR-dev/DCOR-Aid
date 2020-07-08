import warnings

import requests

from .core import DBInterrogator, DBModel
from .util import ttl_cache


class APIKeyError(BaseException):
    pass


class APIModel(DBModel):
    def __init__(self, url, api_key=None, *args, **kwargs):
        """A CKAN-API-based database model"""
        db = APIInterrogator(url=url, api_key=api_key)
        super(APIModel, self).__init__(interrogator=db, *args, **kwargs)


class APIInterrogator(DBInterrogator):
    def __init__(self, url, api_key=None):
        self.api_key = api_key
        self.api_url = self._make_api_url(url)
        self.headers = {"Authorization": api_key}
        super(APIInterrogator, self).__init__()

    def _call(self, api_call, **kwargs):
        if kwargs:
            # Add keyword arguments
            kwv = []
            for kw in kwargs:
                kwv.append("{}={}".format(kw, kwargs[kw]))
            api_call += "?" + "&".join(kwv)
        url_call = self.api_url + api_call
        req = requests.get(url_call, headers=self.headers)
        data = req.json()
        if not data["success"]:
            raise ConnectionError(
                "Could not run API call '{}'! ".format(url_call)
                + "Reason: {}".format(req.reason))
        return data["result"]

    def _make_api_url(self, url):
        if not url.count("//"):
            url = "https://" + url
        if not url.endswith("/action/"):
            url = url.rstrip("/") + "/api/3/action/"
        return url

    @ttl_cache(seconds=5)
    def get_circles(self, mode="public"):
        """Return the list of DCOR Circles
        """
        if mode == "user":
            # Organizations the user is a member of
            data = self._call("organization_list_for_user",
                              permission="read")
        else:
            data = self._call("organization_list")
        return data

    @ttl_cache(seconds=5)
    def get_collections(self, mode="public"):
        """Return the list of DCOR Collections"""
        if mode == "user":
            data = self._call("group_list_authz", am_member=True)
        else:
            data = self._call("group_list")
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
        data2 = self._call("package_search",
                           q="*:*",
                           fq="creator_user_id:{}".format(user_data["id"]),
                           rows=numd+1)
        if data2["count"] != numd:
            raise ValueError("Number of user datasets don't match!")

        return data2["results"]

    def get_datasets_user_shared(self):
        warnings.warn("`APIInterrogator.get_datasets_user_shared` "
                      + "not yet implemented!")
        return []

    @ttl_cache(seconds=3600)
    def get_user_data(self):
        """Return the current user data dictionary"""
        # Workaround for https://github.com/ckan/ckan/issues/5490
        # Get the user that has a matching API key
        data = self._call("user_list")
        for user in data:
            if user.get("apikey") == self.api_key:
                userdata = user
                break
        else:
            raise APIKeyError(
                "Could not determine user data. Please check API key.")
        return userdata

    @ttl_cache(seconds=3600)
    def get_users(self, ret_fullnames=False):
        """Return the list of DCOR users"""
        data = self._call("user_list")
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
        solr_circles = ["organization:{}".format(ci) for ci in circles]
        solr_circle_query = " OR ".join(solr_circles)

        solr_collections = ["groups:{}".format(co) for co in collections]
        solr_collections_query = " OR ".join(solr_collections)

        data = self._call("package_search",
                          q=query,
                          include_private=(mode == "user"),
                          fq="({}) AND ({})".format(solr_circle_query,
                                                    solr_collections_query)
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
