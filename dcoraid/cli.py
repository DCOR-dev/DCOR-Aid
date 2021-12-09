import argparse
from argparse import RawTextHelpFormatter
import threading
import time

from .api import CKANAPI
from .upload import task


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


def upload_task(path_task=None, server=None, api_key=None):
    """Upload a .dcoraid-task file to a DCOR instance"""
    if path_task is None or server is None or api_key is None:
        parser = upload_task_parser()
        args = parser.parse_args()
        path_task = args.path_task
        server = args.server
        api_key = args.api_key

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
    print("Uploading resources.")
    # thread that prints the upload progress
    monitor_thread = threading.Thread(target=monitor_upload_progress,
                                      name="Upload Monitor",
                                      args=(uj,))
    monitor_thread.start()
    uj.task_upload_resources()
    monitor_thread.join()
    print("Verifying upload.")
    uj.task_verify_resources()
    print("Done.")
    return uj


def upload_task_parser():
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
    return parser
