# flake8: noqa: F401
import logging

from . import api, dbmodel, download, upload
from .api import CKANAPI
from .dbmodel import APIInterrogator
from .upload.task import create_task
from ._version import version as __version__

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(processName)s/%(threadName)s "
           + "in %(name)s: %(message)s",
    datefmt='%H:%M:%S')
