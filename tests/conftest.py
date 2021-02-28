from dcoraid.api import APIConflictError

import common


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    api = common.get_api()
    api.get("group_show", id=common.COLLECTION)
    try:
        api.post("group_create", {"name": common.COLLECTION})
    except APIConflictError:
        pass
    try:
        api.post("organization_create", {"name": common.CIRCLE})
    except APIConflictError:
        pass
    user_dict = api.get("user_show", id=common.USER)
    user_dict["fullname"] = common.USER_NAME
    api.post("user_update", user_dict)
