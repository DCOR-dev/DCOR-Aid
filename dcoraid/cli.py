import argparse
from argparse import RawTextHelpFormatter
import logging
import pathlib
import threading
import time
import traceback
import sys

import urllib3.exceptions
import requests.exceptions

from .api import CKANAPI
from .upload import task
from ._version import version


def monitor_upload_progress(upload_job):
    newline = False
    while upload_job.state == "parcel":
        time.sleep(1)
    while upload_job.state == "transfer":
        print(upload_job.get_progress_string(), end="\r", flush=True)
        newline = True
        time.sleep(1)
    if newline:
        # Only print new line if we printed something before.
        print("")


def ascertain_state_or_bust(upload_job, state):
    """If `upload_job.state != `state`, raise an exception"""
    if upload_job.state != state:
        if upload_job.state == "error":
            print(f"Job {upload_job} encountered an error:")
            print(upload_job.traceback)
            raise ValueError("See message above!")
        else:
            raise ValueError(
                f"The upload job {upload_job} should be in the state "
                + f"'{state}', but it's state is '{upload_job.state}'!"
            )


def upload_task(path_task=None, server=None, api_key=None, ret_job=False,
                retries_num=10, retries_wait=30):
    """Upload a .dcoraid-task file to a DCOR instance"""
    # Initialize with None, otherwise we might get issues if parsing
    # of the arguments fails or in `finally`.
    path_error = None
    uj = None
    exit_status = 1  # fails by default if there is no success
    try:

        if path_task is None or server is None or api_key is None:
            parser = upload_task_parser()
            args = parser.parse_args()
            path_task = args.path_task
            server = args.server
            api_key = args.api_key
        path_task = pathlib.Path(path_task)
        path_error = path_task.parent / (path_task.name + "_error.txt")
        for retry in range(retries_num):
            try:
                print("Initializing.")
                # set up the api
                api = CKANAPI(server, api_key=api_key)
                # load the .dcoraid-task file
                uj = task.load_task(path_task,
                                    api=api,
                                    update_dataset_id=True)
                print(f"Dataset ID is {uj.dataset_id}.")
                print("Compressing resources.")
                uj.task_compress_resources()
                ascertain_state_or_bust(uj, "parcel")

                print("Uploading resources.")
                # thread that prints the upload progress
                monitor_thread = threading.Thread(
                    target=monitor_upload_progress,
                    name="Upload Monitor",
                    args=(uj,),
                    daemon=True)
                monitor_thread.start()
                uj.task_upload_resources()
                monitor_thread.join()
                ascertain_state_or_bust(uj, "online")
                print("Verifying upload.")
                uj.task_verify_resources()
                ascertain_state_or_bust(uj, "done")
                print("Done.")
            except (urllib3.exceptions.HTTPError,
                    requests.exceptions.RequestException) as e:
                path_error.write_text(f"Encountered Exception: {e}")
                print(f"Encountered a transfer error. Retrying {retry + 1}...")
                time.sleep(retries_wait)
                httperror = e
                continue
            except BaseException as e:
                # let the outer try-except clause handle all other errors
                raise e
            else:
                # Upload successfully completed
                break
        else:
            # We only get here if we "continued" through the entire loop,
            # which means no successful upload and only HTTPErrors.
            raise httperror
    except SystemExit:
        # The user just passed --help or --version
        exit_status = 0
    except BaseException:
        # Write errors to errors file
        print(traceback.format_exc())
        if path_error is not None:
            path_error.write_text(traceback.format_exc())
    else:
        if path_error.exists():
            path_error.unlink(missing_ok=True)
        exit_status = 0
    finally:
        if ret_job and not exit_status:
            return uj
        # return sys.exit for testing (monkeypatched)
        return sys.exit(exit_status)


def upload_task_parser():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(processName)s/%(threadName)s "
               + "in %(name)s: %(message)s",
        datefmt='%H:%M:%S')

    descr = (
        "Upload a .dcoraid-task file to a DCOR instance. Example usage::\n"
        + "\n    dcoraid-upload-task upload_job.dcoraid-task "
        + "dcor-dev.mpl.mpg.de eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiO"
        )
    parser = argparse.ArgumentParser(description=descr,
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('path_task', metavar="PATH", type=str,
                        help='The .dcoraid-task file')
    parser.add_argument('server', metavar="SERVER", type=str,
                        help='DCOR instance to upload to')
    parser.add_argument('api_key', metavar="API_KEY", type=str,
                        help='Your DCOR API key or token')
    parser.add_argument('--version', action='version',
                        version=f'dcoraid-upload-task {version}')
    return parser
