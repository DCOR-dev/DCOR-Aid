"""Helpers for DCOR users and API keys"""
from __future__ import annotations

import uuid

from .ckan_api import CKANAPI
from .errors import APIAuthorizationError, APINotFoundError, NoAPIKeyError


def create_dcor_dev_user(server: str = "dcor-dev.mpl.mpg.de",
                         fullname: str | None = None,
                         ssl_verify: bool = True,
                         ) -> str:
    """Create a new user on a DCOR-dev-like server and return its API key

    Only DCOR instances meant for testing (such as DCOR-dev) allow
    creating users via the API. These users are named "dcoraid-xxxxx"
    and are purged regularly.

    Parameters
    ----------
    server: str
        DCOR server on which to create the user
    fullname: str
        Full name of the user; defaults to "Player xxxxx"
    ssl_verify: bool
        Whether to verify SSL certificates

    Returns
    -------
    api_key: str
        API token of the new user
    """
    api = CKANAPI(server=server, ssl_verify=ssl_verify)
    rstr = str(uuid.uuid4())[:5]
    name = f"dcoraid-{rstr}"
    user_dict = api.post(
        "user_create",
        data={"name": name,
              "fullname": fullname or f"Player {rstr}",
              "email": f"{name}@dcor-dev.mpl.mpg.de",
              "password": str(uuid.uuid4()),
              "with_apitoken": True,
              })
    return user_dict["token"]


def is_valid_api_key(server: str,
                     api_key: str,
                     ssl_verify: bool = True,
                     ) -> bool:
    """Check whether `api_key` is a valid API key on `server`"""
    if not api_key:
        return False
    api = CKANAPI(server=server, api_key=api_key, ssl_verify=ssl_verify)
    try:
        api.get_user_dict()
    except (APIAuthorizationError, APINotFoundError, NoAPIKeyError):
        return False
    return True
