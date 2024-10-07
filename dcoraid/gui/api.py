import atexit
import logging
import os
import pathlib
import shutil
import tempfile

from PyQt5 import QtCore

from ..api import CKANAPI

_disable_ssl = os.environ.get("DCORAID_SKIP_SSL_VERIFY", "").lower() == "true"
#: Either a boolean or a path to the server's SSL certificate. Defaults
#: to `None` which means DCOR-Aid decides depending on where it is.
_SSL_VERIFY = False if _disable_ssl else None

logger = logging.getLogger(__name__)


def get_ckan_api(public=False):
    """Convenience function for obtaining CKANAPI instance from settings"""
    settings = QtCore.QSettings()
    if public:
        api_key = None
    else:
        api_key = settings.value("auth/api key", "")
    server = settings.value("auth/server", "dcor.mpl.mpg.de")
    ssl_verify = setup_certificate_file()
    api = CKANAPI(server=server, api_key=api_key, ssl_verify=ssl_verify,
                  check_ckan_version=False)
    return api


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
