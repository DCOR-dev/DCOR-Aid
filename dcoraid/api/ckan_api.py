from __future__ import annotations

import copy
import functools
import json
import logging
import pathlib
import traceback
import numbers

from dclab.external.packaging import parse as parse_version
import requests
import requests_cache

from .._version import version

from .errors import (APIConflictError, APINotFoundError, NoAPIKeyError,
                     APIBadGatewayError, APIBadRequest, APIGatewayTimeoutError,
                     APIAuthorizationError, APIOutdatedError)

#: Minimum required CKAN version on the server side
MIN_CKAN_VERSION = "2.9.4"

#: List of license lists for each DCOR server
SERVER_LICENCES = {}

#: List of supplementary resource schema dictionaries
SERVER_RSS = {}

#: List of supported resource suffixes
SERVER_RSUFFIX = {}


class CKANAPI:
    def __init__(self,
                 server: str,
                 api_key: str | None = "",
                 ssl_verify: bool | str = True,
                 check_ckan_version: bool = True,
                 caching: bool | str | pathlib.Path = True):
        """User-convenient interface to the CKAN API

        Parameters
        ----------
        server: str
            Server to which to connect to, e.g. dcor.mpl.mpg.de
        api_key: str
            API token to use for accessing private datasets
        ssl_verify: bool or str
            Either a boolean indicating whether SSL certificates should be
            verified or a path to an SSL certificate for certificate pinning.
        check_ckan_version: bool
            Whether to check for the proper CKAN version
        caching: bool or str or pathlib.Path
            Whether to perform caching `get` requests. If a path is
            specified, it is used for caching, otherwise responses
            are cached in memory.
        """
        self.api_key = (api_key or "").strip()
        self.server = self._make_server_url(server)
        self.api_url = self._make_api_url(server)
        self.headers = {"user-agent": f"DCOR-Aid/{version}"
                        }
        if self.api_key:
            self.headers["X-CKAN-API-Key"] = self.api_key

        self.verify = ssl_verify

        self.logger = logging.getLogger(__name__)

        self._user_dict = None

        if check_ckan_version:
            CKANAPI.check_ckan_version(self.server, ssl_verify=ssl_verify)

        # Caching
        if caching:
            if isinstance(caching, bool):
                kwargs = {"backend": "memory"}
            else:
                kwargs = {"backend": "sqlite",
                          "cache_name": pathlib.Path(caching)}
            self.req_ses = requests_cache.CachedSession(urls_expire_after={
                # API calls without parameters; seconds cached
                self.api_url + "user_show": 5,
                self.api_url + "group_list": 10,
                self.api_url + "user_list": 30,
                self.api_url + "status_show": 60,
                self.api_url + "organization_list": 60,
                # API calls with parameters
                self.api_url + "package_search*": 10,
                self.api_url + "package_collaborator_list_for_user*": 60,
                self.api_url + "organization_list_for_user*": 60,
                self.api_url + "group_list_authz*": 60,
                self.api_url + "dataset_followee_list*": 3600,
                self.api_url + "package_show*": requests_cache.DO_NOT_CACHE,
                self.api_url + "resource_show*": requests_cache.DO_NOT_CACHE,
                "*": requests_cache.DO_NOT_CACHE,
                },
                **kwargs)
        else:
            self.req_ses = requests

    def __repr__(self):
        return f"<CKANAPI {self.api_url} at {hex(id(self))}>"

    @property
    def ckan_version_object(self):
        version_act = self.get("status_show")["ckan_version"]
        return parse_version(version_act)

    @property
    def user_name(self):
        return self._get_user_data().get("name")

    @property
    def user_id(self):
        return self._get_user_data().get("id")

    def _get_user_data(self):
        if self._user_dict is None:
            # initial call to populate self._user_dict
            try:
                ud = self.get_user_dict()
            except (NoAPIKeyError, APIAuthorizationError):
                self.logger.warning("Could not get CKAN user dict.")
                # anonymous access
                self._user_dict = {}
            else:
                self._user_dict = {"id": ud["id"],
                                   "name": ud["name"]}
        return self._user_dict

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

    @staticmethod
    @functools.lru_cache(maxsize=32)
    def check_ckan_version(server, ssl_verify):
        api = CKANAPI(server=server, ssl_verify=ssl_verify,
                      check_ckan_version=False)
        cur_version = api.ckan_version_object
        if api.ckan_version_object < parse_version(MIN_CKAN_VERSION):
            raise ValueError(
                f"DCOR-Aid requires CKAN version {MIN_CKAN_VERSION}, but "
                + f"the server {api.server} is running CKAN {cur_version}. "
                + "Please ask the admin of the server to upgrade CKAN or "
                + "downgrade your version of DCOR-Aid."
            )

    def handle_response(self, req, api_call):
        try:
            rdata = req.json()
        except BaseException:
            self.logger.error(traceback.format_exc())
            rdata = {}

        if isinstance(rdata, str):
            # Something went wrong on a lower level.
            if rdata.startswith("Bad request"):
                raise APIBadRequest(rdata)
            else:
                raise ValueError(
                    "Command did not succeed, output: '{}'".format(rdata))

        if not req.ok:
            error = rdata.get("error", {})
            e_type = error.get("__type", req.reason)
            etext = ""
            for key in error:
                if not key.startswith("_"):
                    etext += "{}: {}".format(key, error[key])
            if not etext and len(req.reason) < 100:
                # Skip large html output, only use small error messages
                etext = req.content.decode("utf-8")
            msg = "{}: {} (for '{}')".format(e_type, etext, api_call)
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
            elif req.reason == "Bad Request" and etext.endswith("outdated."):
                raise APIOutdatedError(msg)
            else:
                raise ConnectionError(msg)
        elif not rdata["success"]:
            url_call = self.api_url + api_call
            raise ConnectionError(
                "Could not run API call '{}'! ".format(url_call)
                + "Reason: {} ({})".format(req.reason, rdata["error"]))
        return rdata

    def copy(self):
        return CKANAPI(server=self.server, api_key=self.api_key,
                       ssl_verify=self.verify)

    def is_available(self, with_api_key=False, with_correct_version=False):
        """Check whether server and API are reachable

        Parameters
        ----------
        with_api_key: bool
            Make sure that we can access the API via an API key.
            This will always result in False if `self.api_key` is
            not set.
        with_correct_version: bool
            Make sure that the remote server is running the minimum
            required version of DCOR. Otherwise, return False.

        Returns
        -------
        is_available: bool
            The server is available and meets all requirements.
        """
        # simply check whether we can access the site
        try:
            self.get("site_read")
        except BaseException:
            self.logger.error(traceback.format_exc())
            status = False
        else:
            status = True
        # check whether we are using the correct ckan version
        if status and with_correct_version:
            try:
                CKANAPI.check_ckan_version(server=self.server,
                                           ssl_verify=self.verify)
            except ValueError:
                self.logger.error(traceback.format_exc())
                status = False
        # check whether we have an API key if we need one
        if status and with_api_key and not self.api_key:
            # We need access with an API key, but we don't have one
            status = False
        # Do something only logged-in users can do
        if status and with_api_key:
            try:
                self.get_user_dict()
            except BaseException:
                self.logger.error(traceback.format_exc())
                status = False
            else:
                status = True
        return status

    def get(self,
            api_call: str,
            timeout: float = 27.9,
            **kwargs: str | numbers.Number):
        """GET request

        Parameters
        ----------
        api_call: str
            An API call function (e.g. "package_show")
        timeout: float
            Timeout for the request call
        kwargs: dict
            Any keyword arguments to the API call
            (e.g. `name="test-dataset"`)

        Returns
        -------
        result: dict
            Result of the API call converted to a dictionary
            from the returned json string
        """
        if "?" in api_call:
            raise ValueError("Please only use original API call without '?'!")
        if kwargs:
            # Add keyword arguments
            kwv = []
            for kw in kwargs:
                kwv.append(f"{kw}={kwargs[kw]}")
            api_call += "?" + "&".join(kwv)
        req = self.req_ses.get(self.api_url + api_call,
                               headers=self.headers,
                               verify=self.verify,
                               timeout=timeout)
        rdata = self.handle_response(req, api_call)
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
        """
        if self.ckan_version_object >= parse_version("2.10.1"):
            try:
                userdata = self.get("user_show")
            except APINotFoundError:
                # This is a workaround for a broken CKAN API. CKAN should
                # actually return "Access Denied" instead of "Not Found",
                # more details: https://github.com/ckan/ckan/issues/7838
                # This has been fixed in CKAN 2.10.3.
                raise APIAuthorizationError("Access denied for 'user_show'")
        else:
            # TODO:
            #  When there are no more 2.9 CKAN instances, remove this
            #  part and increment MIN_CKAN_VERSION.
            self.logger.info("Using workaround for userdata retrieval #6338")
            # Workaround for https://github.com/ckan/ckan/issues/5490
            # Get the user for which the email field is visible.
            try:
                data = self.get("user_list")
            except APIAuthorizationError as e:
                raise NoAPIKeyError("API key probably invalid!") from e
            else:
                for user in data:
                    if user.get("email", ""):
                        userdata = user
                        break
                else:
                    raise NoAPIKeyError(
                        "Could not determine user data. Please check API key.")
        return userdata

    def post(self, api_call, data, dump_json=True, headers=None,
             timeout=27.9):
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
        timeout: float
            Timeout for :func:`requests.request` [s]

        Returns
        -------
        result: dict
            Result of the API call converted to a dictionary
            from the returned json string
        """
        if headers is None:
            headers = {}
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
                            verify=self.verify,
                            timeout=timeout)
        resp = self.handle_response(req, api_call)
        return resp["result"]
