import copy
import json

import requests


class APIKeyError(BaseException):
    pass


class CKANAPI():
    def __init__(self, server, api_key):
        self.api_key = api_key
        self.api_url = self._make_api_url(server)
        self.headers = {"Authorization": api_key}

    def _make_api_url(self, url):
        if not url.count("//"):
            url = "https://" + url
        if not url.endswith("/action/"):
            url = url.rstrip("/") + "/api/3/action/"
        return url

    def get(self, api_call, **kwargs):
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
                + "Reason: {} ({})".format(req.reason,
                                           data["error"]["message"]))
        return data["result"]

    def post(self, api_call, data, dump_json=True, headers={}):
        new_headers = copy.deepcopy(self.headers)
        new_headers.update(headers)
        if dump_json:
            if "Content-Type" in headers:
                raise ValueError("Do not specify 'Content-Type' with "
                                 + "`dump_json=True`!")
            # This is necessary because we cannot otherwise
            # create packages with tags (list of dicts);
            # We have to json-dump the dict.
            new_headers["Content-Type"] = "application/json"
            data = json.dumps(data)

        url_call = self.api_url + api_call
        req = requests.post(url_call,
                            data=data,
                            headers=new_headers)
        data = req.json()
        if not data["success"]:
            raise ConnectionError(
                "Could not run API call '{}'! ".format(url_call)
                + "Reason: {} ({})".format(req.reason,
                                           data["error"]["message"]))
        return data["result"]

    def get_user_dict(self):
        """Return the current user data dictionary"""
        # Workaround for https://github.com/ckan/ckan/issues/5490
        # Get the user that has a matching API key
        data = self.get("user_list")
        for user in data:
            if user.get("apikey") == self.api_key:
                userdata = user
                break
        else:
            raise APIKeyError(
                "Could not determine user data. Please check API key.")
        return userdata
