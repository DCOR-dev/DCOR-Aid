from __future__ import annotations

import argparse
from argparse import RawTextHelpFormatter
import logging
import os
import pathlib
import sys
import traceback

from ..api import CKANAPI
from ..bagit import bag_circle
from ..loggers import setup_logging
from .._version import version


def archive_circle(circle: str | None = None,
                   target_path: str | pathlib.Path | None = None,
                   server: str | None = None,
                   api_key: str | None = None,
                   retries_num: int = 5,
                   retries_wait: float = 30.0):
    """Archive an entire DCOR circle in the BagIt format

    The datasets are stored in the subdirectory `target_path/CIRCLE_NAME`,
    one bag per dataset (see :func:`dcoraid.bagit.bag_circle`). If an
    archiving attempt is interrupted, calling this function again with
    the same arguments resumes archiving.

    Parameters
    ----------
    circle: str
        ID or name (slug) of the circle
    target_path: str or pathlib.Path
        directory in which the circle subdirectory is created
    server: str
        DCOR server to use (e.g. dcor.mpl.mpg.de)
    api_key: str
        user API token; if not specified, the environment variable
        `DCOR_API_KEY` is used
    retries_num: int
        number of times to retry archiving a dataset or downloading
        a resource when connection problems are encountered
    retries_wait: float
        seconds to wait in-between retries
    """
    # Initialize with None, otherwise we might get issues if parsing
    # of the arguments fails.
    path_error = None
    # fails by default if there is no success
    exit_status: int | str | None = 1
    try:
        if circle is None:
            parser = archive_circle_parser()
            args = parser.parse_args()
            circle = args.circle
            target_path = args.target_path
            server = args.server
            api_key = args.api_key
        if target_path is None:
            raise ValueError("No target path specified!")
        if server is None:
            raise ValueError("No DCOR server specified!")
        if api_key is None:
            api_key = os.environ.get("DCOR_API_KEY")
            if not api_key:
                raise ValueError(
                    "No API key specified! Either pass the API key as an "
                    "argument or set the `DCOR_API_KEY` environment variable.")

        print("Initializing.")
        api = CKANAPI(server, api_key=api_key)
        # `organization_show` accepts both the circle ID and name
        assert circle is not None
        circle_dict = api.get("organization_show", id=circle)
        circle_name = circle_dict["name"]
        circle_path = pathlib.Path(target_path) / circle_name
        circle_path.mkdir(parents=True, exist_ok=True)
        path_error = circle_path / "archive_circle_error.txt"

        print(f"Archiving circle '{circle_name}' to '{circle_path}'.")
        bag_circle(api=api,
                   circle_name=circle_name,
                   target_path=circle_path,
                   retries_num=retries_num,
                   retries_wait=retries_wait,
                   )
        print("Done.")
    except SystemExit as e:
        # e.code is 0 if the user just passed --help or --version
        exit_status = e.code
    except KeyboardInterrupt:
        print("Aborted by user. Run the same command again to resume.")
    except BaseException:
        # Write errors to errors file
        print(traceback.format_exc())
        if path_error is not None:
            path_error.write_text(traceback.format_exc())
    else:
        if path_error.exists():
            path_error.unlink(missing_ok=True)
        exit_status = 0
    # return sys.exit for testing (monkeypatched)
    return sys.exit(exit_status)


def archive_circle_parser():
    setup_logging("dcoraid")
    setup_logging("dclab")
    setup_logging("requests", level=logging.INFO)

    descr = (
        "Archive all datasets of a DCOR circle in the BagIt format "
        + "(RFC 8493).\nThe datasets are stored in the subdirectory "
        + "TARGET_PATH/CIRCLE_NAME. Running\nthe same command again "
        + "resumes an interrupted archiving process.\n"
        + "\nExample usage::\n"
        + "\n    dcoraid-archive-circle my-circle /path/to/archive "
        + "dcor.mpl.mpg.de eyJ0eXAiOiJKV1QiLCJhbGc\n"
        + "\nInstead of passing the API key as an argument, you may also "
        + "set the\nenvironment variable `DCOR_API_KEY`::\n"
        + "\n    export DCOR_API_KEY=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ"
        + "\n    dcoraid-archive-circle my-circle /path/to/archive "
        + "dcor.mpl.mpg.de\n"
        + "\nTo validate the archived bags, you may use the `bagit` "
        + "package::\n"
        + "\n    pip install bagit"
        + "\n    bagit.py --validate --quiet /path/to/archive/my-circle/*"
        )
    parser = argparse.ArgumentParser(description=descr,
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('circle', metavar="CIRCLE", type=str,
                        help='ID or name of the circle to archive')
    parser.add_argument('target_path', metavar="TARGET_PATH", type=str,
                        help='Directory in which the circle subdirectory '
                             'is created')
    parser.add_argument('server', metavar="SERVER", type=str,
                        help='DCOR instance to archive from '
                             '(e.g. dcor.mpl.mpg.de)')
    parser.add_argument('api_key', metavar="API_KEY", type=str, nargs="?",
                        help='Your DCOR API token (defaults to the '
                             'environment variable DCOR_API_KEY)')
    parser.add_argument('--version', action='version',
                        version=f'dcoraid-archive-circle {version}')
    return parser
