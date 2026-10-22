from __future__ import annotations

import hashlib
import json
import logging
import pathlib
import shutil
import threading
import time
from collections.abc import Callable

import requests.exceptions
import urllib3.exceptions

from ..api import CKANAPI
from ..api import errors as api_errors
from ..common import is_dc_resource_dict
from ..dbmodel import APIInterrogator
from ..download import DownloadJob
from . import info, manifest


logger = logging.getLogger(__name__)

#: Errors that are considered transient and trigger a retry
TRANSIENT_ERRORS = (
    ConnectionError,
    TimeoutError,
    requests.exceptions.RequestException,
    urllib3.exceptions.HTTPError,
    api_errors.APIBadGatewayError,
    api_errors.APIGatewayTimeoutError,
)


class ArchiveError(Exception):
    """Raised when a resource or dataset could not be archived"""


class ResourceVerificationError(Exception):
    """Raised when a downloaded resource could not be verified"""


def bag_circle(api: CKANAPI,
               circle_name: str,
               target_path: pathlib.Path,
               abort_event: threading.Event | None = None,
               callback: Callable | None = None,
               retries_num: int = 3,
               retries_wait: float = 10.0):
    """Download an entire circle to a target directory in BagIt format

    The format follows RFC 8493 "The BagIt File Packaging Format (V1.0)".

    To validate the BagIt bags:

        pip install bagit
        bagit.py --validate --quiet target_path/*

    Parameters
    ----------
    api
        CKANAPI for connecting to the DCOR instance
    circle_name
        Name of the circle to archive
    target_path
        Download location
    abort_event
        Specify a `threading.Event` to be able to abort archiving;
        when you wish to abort `.set()` the event.
    callback
        Method for progress tracking (returns a float between 0 and 1)
    retries_num
        Number of times to retry archiving a dataset or downloading a
        resource when transient errors (e.g. connection problems) are
        encountered
    retries_wait
        Seconds to wait in-between retries

    Raises
    ------
    ArchiveError
        If a resource could not be downloaded or a dataset could not
        be archived after `retries_num` retries
    """
    # fetch total list of active datasets
    ai = APIInterrogator(api)
    dataset_dicts = ai.search_dataset_via_api(circles=[circle_name],
                                              limit=0,
                                              ret_db_extract=False)

    num_datasets = len(dataset_dicts)

    # sort datasets according to creation date
    dataset_dicts = sorted(dataset_dicts, key=lambda x: x["metadata_created"])

    # compute sha256 hash of all dataset IDs
    hasher = hashlib.sha256()
    for ds_dict in dataset_dicts:
        hasher.update(ds_dict["id"].encode(encoding="utf-8"))
    sha256_hash = hasher.hexdigest()

    # Check whether there is already a list of dataset IDs in the directory,
    # and if yes, compute the MD5 hash and compare it. If the comparison
    # fails, then the user has to choose a different `target_path`, because
    # we cannot guarantee data integrity.
    target_path.mkdir(parents=True, exist_ok=True)
    circle_jsonlines_path = target_path / "circle.jsonlines"
    if circle_jsonlines_path.exists():
        lines = circle_jsonlines_path.read_text().split("\n")
        hasher2 = hashlib.sha256()
        for line in lines:
            if line.strip():
                hasher2.update(json.loads(line)["id"].encode(encoding="utf-8"))
        sha256_hash2 = hasher2.hexdigest()
        if sha256_hash != sha256_hash2:
            raise ValueError(
                f"A previous attempt to archive circle {circle_name} was made "
                f"in directory {target_path}. However, the number of datasets "
                f"changed since then. Therefore, it is not possible to "
                f"archive this circle to that directory.")
    else:
        # save list of datasets as jsonlines
        with circle_jsonlines_path.open("w", encoding="utf-8") as f:
            for ds_dict in dataset_dicts:
                f.write(json.dumps(ds_dict) + "\n")

    # number of digits for enumeration
    max_digits = len(str(num_datasets))

    # bag all dataset
    for ii, ds_dict in enumerate(dataset_dicts):
        if callback:
            callback(ii / num_datasets)

        dataset_index = ii + 1
        prefix = str(dataset_index).zfill(max_digits+1)
        bag_path = target_path / f"{prefix}_{ds_dict['name']}"

        if manifest.is_bagged(bag_path):
            logger.info(f"Dataset {dataset_index}/{num_datasets} already "
                        f"archived: {ds_dict['name']}")
        else:
            logger.info(f"Archiving dataset {dataset_index}/{num_datasets}: "
                        f"{ds_dict['name']}")
            for retry in range(retries_num + 1):
                try:
                    bag_dataset(api=api,
                                ds_dict=ds_dict,
                                dataset_index=dataset_index,
                                num_datasets=num_datasets,
                                bag_path=bag_path,
                                abort_event=abort_event,
                                # Only start from scratch in the first try.
                                # In subsequent tries, keep the resources
                                # that were already downloaded and verified.
                                clear=retry == 0,
                                retries_num=retries_num,
                                retries_wait=retries_wait,
                                )
                except TRANSIENT_ERRORS as e:
                    # Resource downloads are retried in `download_resource`
                    # which raises an `ArchiveError` (not retried here) if
                    # all retries failed.
                    if retry == retries_num:
                        raise ArchiveError(
                            f"Failed to archive dataset {ds_dict['name']} "
                            f"({ds_dict['id']}) after {retries_num} retries: "
                            f"{e}") from e
                    logger.warning(
                        f"Failed to archive dataset {ds_dict['name']}: {e}. "
                        f"Retrying {retry + 1}...")
                    time.sleep(retries_wait)
                else:
                    break
        if abort_event is not None and abort_event.is_set():
            return
    if callback:
        callback(1)


def bag_dataset(api: CKANAPI,
                ds_dict: dict,
                bag_path: pathlib.Path,
                abort_event: threading.Event | None = None,
                dataset_index: int = 1,
                num_datasets: int = 1,
                clear: bool = True,
                retries_num: int = 3,
                retries_wait: float = 10.0,
                ):
    """Download a dataset to a target directory in BagIt format

    Parameters
    ----------
    api
        CKANAPI for connecting to the DCOR instance
    ds_dict
        CKAN dataset dictionary
    bag_path
        Path of the bag
    abort_event
        Event for aborting (see :func:`bag_circle`)
    dataset_index
        Index of this dataset in the circle
    num_datasets
        Total number of datasets in the circle
    clear
        Whether to remove `bag_path` before downloading; set this to
        False to keep resources that were already downloaded and
        verified (e.g. when retrying)
    retries_num
        Number of times to retry downloading a resource
        (see :func:`download_resource`)
    retries_wait
        Seconds to wait in-between retries
    """
    # clear/create download directory
    if clear and bag_path.exists():
        shutil.rmtree(bag_path)
    bag_path.mkdir(parents=True, exist_ok=True)
    data_path = bag_path / "data"
    data_path.mkdir(parents=True, exist_ok=True)

    # dataset dictionary
    meta = json.dumps(ds_dict, indent=2, sort_keys=True)
    (data_path / "dataset.json").write_text(meta)

    # download all resources
    for res_dict in ds_dict["resources"]:
        if abort_event is not None and abort_event.is_set():
            return

        # resource
        download_resource(api=api,
                          bag_path=bag_path,
                          res_dict=res_dict,
                          condensed=False,
                          abort_event=abort_event,
                          retries_num=retries_num,
                          retries_wait=retries_wait,
                          )

        if abort_event is not None and abort_event.is_set():
            return

        # condensed resource (must match the criterion used in
        # `manifest.write_manifest`)
        if is_dc_resource_dict(res_dict):
            download_resource(api=api,
                              bag_path=bag_path,
                              res_dict=res_dict,
                              condensed=True,
                              abort_event=abort_event,
                              retries_num=retries_num,
                              retries_wait=retries_wait,
                              )

    if abort_event is not None and abort_event.is_set():
        return

    # create BagIt files
    info.write_bag_info(bag_path=bag_path,
                        bag_index=dataset_index,
                        num_bags=num_datasets,
                        ds_dict=ds_dict)

    # create BagIt manifest files
    manifest.write_manifest(bag_path=bag_path,
                            ds_dict=ds_dict)


def download_resource(api: CKANAPI,
                      bag_path: pathlib.Path,
                      res_dict: dict,
                      condensed: bool,
                      abort_event: threading.Event | None = None,
                      retries_num: int = 3,
                      retries_wait: float = 10.0,
                      ):
    """Download and verify a resource from DCOR

    Parameters
    ----------
    api
        CKANAPI for connecting to the DCOR instance
    bag_path
        Path of the bag
    res_dict
        CKAN resource dictionary
    condensed
        Whether to download the condensed resource (or the original resource)
    abort_event
        For stopping the download process prematurely
    retries_num
        Number of times to retry the download when transient errors
        (e.g. connection problems) occur or when verification fails
    retries_wait
        Seconds to wait in-between retries

    Raises
    ------
    ArchiveError
        If the resource could not be downloaded and verified after
        `retries_num` retries or if there is not enough disk space
    """
    data_path = bag_path / "data"
    data_path.mkdir(parents=True, exist_ok=True)
    dl_path = data_path / res_dict["name"]
    if condensed:
        dl_path = dl_path.with_name(dl_path.stem + "_condensed.rtdc")
    res_label = f"{res_dict['name']} ({res_dict['id']}" \
        + (", condensed)" if condensed else ")")

    for retry in range(retries_num + 1):
        try:
            # A new job resumes from a partially downloaded temporary file.
            dj = DownloadJob(api=api,
                             resource_id=res_dict["id"],
                             download_path=dl_path,
                             condensed=condensed,
                             )
            if abort_event is not None and abort_event.is_set():
                return

            dj.task_download_resource(abort_event=abort_event)

            if abort_event is not None and abort_event.is_set():
                return

            if dj.state == "wait-disk":
                raise ArchiveError(
                    f"Not enough disk space to download resource "
                    f"{res_label} to {dl_path}")

            dj.task_verify_resource()

            if dj.state != "done":
                # Remove the corrupt temporary file, so that the next try
                # starts from scratch instead of resuming.
                if dj.path_temp is not None:
                    dj.path_temp.unlink(missing_ok=True)
                raise ResourceVerificationError(
                    dj.traceback or f"Download job is in state '{dj.state}'")
        except (ResourceVerificationError,) + TRANSIENT_ERRORS as e:
            if retry == retries_num:
                raise ArchiveError(
                    f"Failed to download resource {res_label} after "
                    f"{retries_num} retries: {e}") from e
            logger.warning(f"Failed to download resource {res_label}: {e}. "
                           f"Retrying {retry + 1}...")
            time.sleep(retries_wait)
        else:
            break
