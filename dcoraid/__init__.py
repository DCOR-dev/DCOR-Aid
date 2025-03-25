"""Management data on the deformability cytometry open repository
Copyright (C) 2020 Paul Müller

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <https://www.gnu.org/licenses/>.
"""
# flake8: noqa: F401
from . import api, dbmodel, download, upload
from .api import CKANAPI
from .dbmodel import APIInterrogator
from .upload.task import create_task
from ._version import version as __version__
