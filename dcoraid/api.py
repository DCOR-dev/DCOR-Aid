import copy
import json

import requests

from ._version import version


#: List of license lists for each DCOR server
SERVER_LICENCES = {}

#: List of supplementary resource schema dictionaries
SERVER_RSS = {}

#: List of supported resource suffixes
SERVER_RSUFFIX = {}


class APIError(BaseException):
    pass


class APIAuthorizationError(APIError):
    pass


class APIBadGatewayError(APIError):
    pass


class APIConflictError(APIError):
    pass


class APIGatewayTimeoutError(APIError):
    pass


class APIKeyError(APIError):
    pass


class APINotFoundError(APIError):
    pass


class CKANAPI:
    def __init__(self, server, api_key, ssl_verify=True):
        """User-convenient interface to the CKAN API"""
        self.api_key = api_key
        self.server = self._make_server_url(server)
        self.api_url = self._make_api_url(server)
        self.headers = {"X-CKAN-API-Key": api_key,
                        "user-agent": "DCOR-Aid/{}".format(version)
                        }
        self.verify = ssl_verify

    def _make_api_url(self, url):
        """Generate a complete CKAN API URL

        Any given string is changed to yield the
        form "https://domain.name/api/3/action".
        """
        url = self._make_server_url(url)
        if not url.endswith("/action/"):
            url = url.rstrip("/") + "/api/3/action/"
        return url

    def _make_server_url(self, url):
        """Generate a complete CKAN server URL

        Any given string is changed to yield the
        form "https://domain.name/".
        """
        if not url.count("//"):
            url = "https://" + url
        return url

    def copy(self):
        return CKANAPI(server=self.server, api_key=self.api_key,
                       ssl_verify=self.verify)

    def is_available(self):
        """Check whether server and API are reachable"""
        try:
            self.get("site_read")
        except BaseException:
            status = False
        else:
            status = True
        return status

    def get(self, api_call, **kwargs):
        """GET request

        Parameters
        ----------
        api_call: str
            An API call function (e.g. "package_show")
        kwargs: Any
            Any keyword arguments to the API call
            (e.g. `name="test-dataset"`)

        Returns
        -------
        result: dict
            Result of the API call converted to a dictionary
            from the returned json string
        """
        if "?" in api_call:
            raise ValueError("Please onyl use original API call without '?'!")
        if kwargs:
            # Add keyword arguments
            kwv = []
            for kw in kwargs:
                kwv.append("{}={}".format(kw, kwargs[kw]))
            api_call += "?" + "&".join(kwv)
        url_call = self.api_url + api_call
        req = requests.get(url_call,
                           headers=self.headers,
                           verify=self.verify,
                           timeout=27.9)
        try:
            rdata = req.json()
        except BaseException:
            rdata = {}
        if isinstance(rdata, str):
            raise ValueError(
                "Command did not succeed, output: '{}'".format(rdata))
        if not req.ok:
            error = rdata.get("error", {})
            etype = error.get("__type", req.reason)
            etext = ""
            for key in error:
                if not key.startswith("_"):
                    etext += "{}: {}".format(key, error[key])
            msg = "{}: {} (for '{}')".format(etype, etext, api_call)
            if req.reason == "NOT FOUND":
                raise APINotFoundError(msg)
            else:
                raise ConnectionError(msg)
        elif not rdata["success"]:
            raise ConnectionError(
                "Could not run API call '{}'! ".format(url_call)
                + "Reason: {} ({})".format(req.reason, rdata["error"]))
        return rdata["result"]

    def get_license_list(self):
        """Return the servers license list

        License lists are cached in :const:`SERVER_LICENCES`.
        """
        if self.api_url not in SERVER_LICENCES:
            SERVER_LICENCES[self.api_url] = self.get("license_list")
        return copy.deepcopy(SERVER_LICENCES[self.api_url])

    def get_supplementary_resource_schema(self):
        """Return the servers supplementary resource schema

        Schemas are cached in :const:`SERVER_RSS`.
        """
        if self.api_url not in SERVER_RSS:
            SERVER_RSS[self.api_url] = self.get("resource_schema_supplements")
        return copy.deepcopy(SERVER_RSS[self.api_url])

    def get_supported_resource_suffixes(self):
        """Return the servers supported resource suffixes

        Suffix lists are cached in :const:`SERVER_RSUFFIX`.
        """
        if self.api_url not in SERVER_RSUFFIX:
            SERVER_RSUFFIX[self.api_url] = self.get(
                "supported_resource_suffixes")
        return copy.deepcopy(SERVER_RSUFFIX[self.api_url])

    def get_user_dict(self):
        """Return the current user data dictionary

        The user name is inferred from the user list.
        """
        # Workaround for https://github.com/ckan/ckan/issues/5490
        # Get the user for which the email field is visible.
        data = self.get("user_list")
        for user in data:
            if user.get("email", ""):
                userdata = user
                break
        else:
            raise APIKeyError(
                "Could not determine user data. Please check API key.")
        return userdata

    def post(self, api_call, data, dump_json=True, headers={}):
        """POST request

        Parameters
        ----------
        api_call: str
            An API call function (e.g. "package_create")
        data: dict, MultipartEncoder, ...
            The data connected to the post request. For
            "package_create", this would be a dictionary
            with the dataset name, author, license, etc.
        dump_json: bool
            If True (default) dump `data` into a json string.
            If False, `data` is not touched.
        headers: dict
            Additional headers (updates `self.headers`) for the
            POST request (used for multipart uploads).

        Returns
        -------
        result: dict
            Result of the API call converted to a dictionary
            from the returned json string
        """
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
                            headers=new_headers,
                            verify=self.verify)
        try:
            rdata = req.json()
        except BaseException:
            rdata = {}
        if isinstance(rdata, str):
            raise ValueError(
                "Command did not succeed, output: '{}'".format(rdata))
        if not req.ok:
            error = rdata.get("error", {})
            etype = error.get("__type", req.reason)
            etext = ""
            for key in error:
                if not key.startswith("_"):
                    etext += "{}: {}".format(key, error[key])
            msg = "{}: {} (for '{}')".format(etype, etext, api_call)
            if req.reason == "NOT FOUND":
                raise APINotFoundError(msg)
            elif req.reason == "CONFLICT":
                raise APIConflictError(msg)
            elif req.reason == "Gateway Time-out":
                raise APIGatewayTimeoutError(msg)
            elif req.reason == "Bad Gateway":
                raise APIBadGatewayError(msg)
            elif req.reason == "FORBIDDEN":
                raise APIAuthorizationError(msg)
            else:
                raise ConnectionError(msg)
        if not rdata["success"]:
            raise ConnectionError(
                "Could not run API call '{}'! ".format(url_call)
                + "Reason: {} ({})".format(req.reason, rdata["error"]))
        return rdata["result"]
