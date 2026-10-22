import atexit
import logging
import os
import pathlib
import shutil
import tempfile

from PyQt6 import QtCore

from ..api import CKANAPI, create_dcor_dev_user, is_valid_api_key

_disable_ssl = os.environ.get("DCORAID_SKIP_SSL_VERIFY", "").lower() == "true"
#: Either a boolean or a path to the server's SSL certificate. Defaults
#: to `None` which means DCOR-Aid decides depending on where it is.
_SSL_VERIFY = False if _disable_ssl else None

#: Cache for CKANAPI instance
_CKAN_API = None

logger = logging.getLogger(__name__)


def get_ckan_api(public=False):
    """Convenience function for obtaining CKANAPI instance from settings"""
    global _CKAN_API
    if _CKAN_API is None:
        settings = QtCore.QSettings()
        if public:
            api_key = None
        else:
            api_key = settings.value("auth/api key", "")
        server = settings.value("auth/server", "dcor.mpl.mpg.de")
        ssl_verify = setup_certificate_file()
        _CKAN_API = CKANAPI(server=server,
                            api_key=api_key,
                            ssl_verify=ssl_verify,
                            check_ckan_version=False)
    return _CKAN_API


def ensure_playground_user():
    """Make sure that the user of the DCOR-dev playground exists

    In the "dcor-dev" user scenario (playground), DCOR-Aid uses a user
    that was created automatically on DCOR-dev. All users and data on
    DCOR-dev are purged regularly. If the API key of the playground user
    is not valid anymore, a new user is created and its API key is
    stored in the settings.

    This function must be called before :func:`get_ckan_api` is called.

    Returns
    -------
    created: bool
        Whether a new user was created
    """
    global _CKAN_API
    settings = QtCore.QSettings()
    if settings.value("user scenario", "") != "dcor-dev":
        return False
    api_key = settings.value("auth/api key", "")
    if not api_key:
        # Not set up yet (this is done by the setup wizard).
        return False
    server = settings.value("auth/server", "dcor-dev.mpl.mpg.de")
    ssl_verify = setup_certificate_file()
    if is_valid_api_key(server=server,
                        api_key=api_key,
                        ssl_verify=ssl_verify):
        return False
    api_key = create_dcor_dev_user(server=server, ssl_verify=ssl_verify)
    settings.setValue("auth/api key", api_key)
    settings.sync()
    # make sure the new API key is used
    _CKAN_API = None
    return True


def setup_certificate_file():
    global _SSL_VERIFY
    if _SSL_VERIFY is None:
        settings = QtCore.QSettings()
        if settings.value("user scenario") == "medical":
            cert_data = settings.value("auth/certificate")
            tmpdir = tempfile.mkdtemp(prefix="dcoraid_certificate_pinning_")
            cert_path = pathlib.Path(tmpdir) / "server.cert"
            cert_path.write_bytes(cert_data)
            atexit.register(shutil.rmtree, tmpdir, ignore_errors=True)
            _SSL_VERIFY = str(cert_path)
        else:
            _SSL_VERIFY = True
    logger.debug(f"SSL verification: {_SSL_VERIFY}")
    return _SSL_VERIFY
