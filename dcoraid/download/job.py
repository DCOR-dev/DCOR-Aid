import copy
import hashlib
import logging
import pathlib
import shutil
import traceback

import time
import warnings

import numpy as np
import requests

from ..api import errors as api_errors
from ..common import etagsum, sha256sum, weak_lru_cache


logger = logging.getLogger(__name__)

#: Valid job states (in more or less chronological order)
JOB_STATES = [
    "init",  # initial
    "wait-disk",  # waiting for free disk space to download
    "transfer",  # download in progress
    "downloaded",  # dataset has been transferred
    "verify",  # perform SHA256 sum verification
    "done",  # job finished
    "abort",  # user aborted
    "error",  # error occurred
]


class DownloadJob:
    def __init__(self, api, resource_id, download_path, condensed=False):
        """Wrapper for resource downloads

        The function `task_download_resource` performs the actual
        download. During download, the progress is monitored and
        can be read from other threads using the e.g. `get_status`
        function.

        Parameters
        ----------
        api: dcoraid.api.CKANAPI
            The CKAN/DCOR API instance used for the download
        resource_id: str
            ID of the CKAN/DCOR resource
        download_path: str or pathlib.Path
            Local path to where the resource will be downloaded.
            If a directory is specified, that directory must exist.
            If the path to the local target is specified, its
            parent directory must exist.
        condensed: bool
            Whether to download the condensed dataset
        """
        self.api = api.copy()  # create a copy of the API
        self.resource_id = resource_id
        self.job_id = resource_id + ("_cond" if condensed else "")
        self._user_path = pathlib.Path(download_path)
        self.condensed = condensed
        self.path_temp = None
        # SHA256 sum of the *downloaded* resource (computed either while
        # downloading or after the download) for verification.
        self.sha256sum_dl = None
        self.state = None
        self.set_state("init")
        self.traceback = None
        #: The bytes of the file that has been downloaded in the current
        #: session. It does not include bytes from a previous session
        #: (after resuming a download).
        self.file_bytes_downloaded = 0
        self.start_time = None
        self.end_time = None
        self._last_time = 0
        self._last_bytes = 0
        self._last_rate = 0

    def __getstate__(self):
        """Get the state of the DownloadJob instance

        This is not the state of the download! For monitoring the
        download, please see :func:`DownloadJob.get_status`,
        :const:`DownloadJob.state`, and :func:`DownloadJob.set_state`.

        See Also
        --------
        from_download_job_state: to recreate a job from a state
        """
        dj_state = {
            "resource_id": self.resource_id,
            "download_path": str(self.path),
            "condensed": self.condensed,
        }
        return dj_state

    @property
    def path(self):
        """Path to the resource which is downloaded"""
        return self.get_download_path()

    @property
    def path_dir(self):
        """Path to the download directory"""
        return self.path.parent

    @staticmethod
    def from_download_job_state(dj_state, api):
        """Reinstantiate a job from an `DownloadJob.__getstate__` dict
        """
        return DownloadJob(api=api, **dj_state)

    @property
    def file_size(self):
        return self.get_resource_dict()["size"]

    @property
    def id(self):
        return self.resource_id

    @weak_lru_cache(maxsize=100)
    def get_resource_dict(self):
        """Return resource dictionary"""
        return self.api.get("resource_show", id=self.resource_id)

    @weak_lru_cache(maxsize=100)
    def get_dataset_dict(self):
        res_dict = self.get_resource_dict()
        ds_dict = self.api.get("package_show", id=res_dict["package_id"])
        return ds_dict

    @weak_lru_cache()
    def get_download_path(self):
        """Return the final location to which the file is downloaded"""
        if self._user_path.is_dir():
            # Compute the resource path from the dataset dictionary
            rsdict = self.get_resource_dict()
            ds_name = self.get_dataset_dict()["name"]
            # append dataset name to user-specified download directory
            ds_dir = self._user_path / ds_name
            ds_dir.mkdir(parents=True, exist_ok=True)
            if self.condensed and rsdict["mimetype"] == "RT-DC":
                stem, suffix = rsdict["name"].rsplit(".", 1)
                res_name = stem + "_condensed." + suffix
            else:
                res_name = rsdict["name"]
            dl_path = ds_dir / res_name
        elif self._user_path.parent.is_dir():
            # user specified an actual file
            dl_path = self._user_path
        else:
            raise ValueError(
                f"The `download_path` passed in __init__ is invalid. "
                f"Please make sure the target directory for {self._user_path} "
                f"exists.")

        return dl_path

    def get_resource_url(self):
        """Return a link to the resource on DCOR"""
        res_dict = self.get_resource_dict()
        # If we have an .rtdc dataset and want the condensed version,
        # we have a different download path.
        if res_dict["mimetype"] == "RT-DC" and self.condensed:
            dl_path = f"{self.api.server}/dataset/{res_dict['package_id']}" \
                      + f"/resource/{self.resource_id}/condensed.rtdc"
        else:
            dl_path = f"{self.api.server}/dataset/{res_dict['package_id']}" \
                      + f"/resource/{self.resource_id}/download/" \
                      + f"{res_dict['name']}"
        return dl_path

    def get_progress_string(self):
        """Return a nice string representation of the progress"""
        status = self.get_status()
        state = status["state"]

        if state in ["init", "transfer", "wait-disk"]:
            progress = "{:.0f}%".format(
                status["bytes local"] / status["bytes total"] * 100)
        elif state in ["downloaded", "verify", "done"]:
            progress = "100%"
        elif state in ["abort", "error"]:
            progress = "--"
        elif state in JOB_STATES:
            # seems like you missed to update a case here?
            warnings.warn(f"Please add state '{state}' to these cases!")
            progress = "undefined"
        return progress

    def get_rate_string(self):
        """Return a nice string representing download rate"""
        status = self.get_status()
        state = status["state"]
        rate = status["rate"]
        if state in ["init", "wait-disk"]:
            rate_label = "-- kB/s"
        else:
            if rate > 1e6:
                rate_label = "{:.1f} MB/s".format(rate / 1e6)
            else:
                rate_label = "{:.0f} kB/s".format(rate / 1e3)
            if state != "transfer":
                rate_label = "⌀ " + rate_label
        return rate_label

    def get_rate(self, resolution=3.05):
        """Return the current resource download rate

        Parameters
        ----------
        resolution: float
            Time interval in which to perform the average.

        Returns
        -------
        download_rate: float
            Mean download rate in bytes per second
        """
        cur_time = time.perf_counter()
        # get bytes of files that have been downloaded
        cur_bytes = self.file_bytes_downloaded
        delta_time = cur_time - self._last_time
        delta_bytes = cur_bytes - self._last_bytes

        if self.start_time is None:
            # not started yet
            rate = 0
            self._last_bytes = 0
        elif self.end_time is None:
            # not finished yet
            if self._last_time == 0:
                # first time we are here
                delta_time = cur_time - self.start_time
            if delta_time > resolution:
                rate = delta_bytes / delta_time
                self._last_time = cur_time
                self._last_bytes = cur_bytes
            else:
                rate = self._last_rate
        else:
            # finished
            tdelt = (self.end_time - self.start_time)
            rate = cur_bytes / tdelt
        self._last_rate = rate
        return rate

    def get_status(self):
        """Get the status of the current job

        Returns
        -------
        status: dict
            Dictionary with various interesting parameters
        """
        # Check whether the resource exists
        try:
            data = {
                "state": self.state,
                "bytes total": self.file_size,
                "bytes downloaded": self.file_bytes_downloaded,
                "rate": self.get_rate(),
            }
            if self.path.exists() and self.path.is_file():
                data["bytes local"] = self.path.stat().st_size
            elif self.path_temp is not None and self.path_temp.is_file():
                data["bytes local"] = self.path_temp.stat().st_size
            else:
                data["bytes local"] = 0
        except api_errors.APINotFoundError:
            # The user likely tried to download the file from a different
            # host or an evil admin deleted a file.
            self.traceback = traceback.format_exc()
            self.set_state("error")
            data = {
                "state": self.state,
                "bytes total": np.nan,
                "bytes downloaded": np.nan,
                "rate": np.nan,
                "bytes local": np.nan,
            }
        return data

    def retry_download(self):
        """Retry downloading resources when an error occured"""
        if self.state in ["abort", "error"]:
            self.set_state("init")
        else:
            raise ValueError("Can only retry download in error state!")

    def set_state(self, state):
        """Set the current job state

        The state is checked against :const:`JOB_STATES`
        """
        if state not in JOB_STATES:
            raise ValueError("Unknown state: '{}'".format(state))
        if state == "error":
            logger.error("Entered error state")
            if self.traceback:
                logger.error(f"{self.traceback}")
        self.state = state

    def task_download_resource(self):
        """Start the download

        The progress of the download is monitored and written
        to the attributes. The current status can be retrieved
        via :func:`DownloadJob.get_status`.
        """
        if self.state in ["init", "wait-disk"]:
            if self.path.exists():
                self.start_time = None
                self.end_time = None
                self.file_bytes_downloaded = 0
                self.set_state("downloaded")
            else:
                # set-up temporary path
                self.path_temp = self.path.with_name(self.path.name + "~")
                # check for disk space
                if self.condensed:
                    # get the size from the server
                    url = self.get_resource_url()
                    req = requests.get(url,
                                       stream=True,
                                       headers=self.api.headers,
                                       verify=self.api.verify,
                                       timeout=29.9,
                                       )
                    size = int(req.headers["Content-length"])
                else:
                    size = self.get_resource_dict()["size"]
                if shutil.disk_usage(self.path_temp.parent).free < size:
                    # there is not enough space on disk for the download
                    self.set_state("wait-disk")
                    time.sleep(1)
                else:
                    # proceed with download
                    # reset everything
                    hasher = hashlib.sha256()
                    self.file_bytes_downloaded = 0
                    self.start_time = None
                    self.end_time = None
                    self._last_time = 0
                    self._last_bytes = 0
                    self._last_rate = 0
                    # begin transfer
                    self.set_state("transfer")
                    self.start_time = time.perf_counter()
                    # Do the things to do and watch self.state while doing so
                    url = self.get_resource_url()
                    headers = copy.deepcopy(self.api.headers)

                    bytes_present = 0
                    if self.path_temp.exists():
                        # Resume a previous download.
                        # We have to update the hash of the current file with
                        # the data that has already been uploaded.
                        if (self.sha256sum_dl is None
                                # We do not verify SHA256 for condensed
                                and not self.condensed):
                            with self.path_temp.open("rb") as fd:
                                while chunk := fd.read(1024**2):
                                    hasher.update(chunk)
                        bytes_present = self.path_temp.stat().st_size
                        headers["Range"] = f"bytes={bytes_present}-"

                    if bytes_present != self.file_size:
                        with requests.get(url,
                                          stream=True,
                                          headers=headers,
                                          verify=self.api.verify,
                                          timeout=29.9) as r:
                            r.raise_for_status()
                            with self.path_temp.open('ab') as f:
                                mib = 1024 * 1024
                                for chunk in r.iter_content(chunk_size=mib):
                                    f.write(chunk)
                                    self.file_bytes_downloaded += len(chunk)
                                    # Compute the SHA256 sum while downloading.
                                    # This is faster than reading everything
                                    # again after the download but has the
                                    # slight risk of losing data in memory
                                    # before it got written to disk. A risk we
                                    # are going to take for the sake of
                                    # performance.
                                    if (self.sha256sum_dl is None
                                            # We do not verify SHA256
                                            # for condensed data.
                                            and not self.condensed):
                                        hasher.update(chunk)
                    self.sha256sum_dl = hasher.hexdigest()
                    self.end_time = time.perf_counter()
                    self.set_state("downloaded")
        else:
            warnings.warn("Starting a download only possible when state is "
                          + "'init' or 'wait-disk', but current state is "
                          + "'{}'!".format(self.state))

    def task_verify_resource(self):
        """Perform ETag/SHA256 verification"""
        if self.state == "downloaded":
            if self.path.exists() and self.path.is_file():
                # This means the download succeeded to `self.path_temp`
                if self.path_temp is not None and self.path_temp.exists():
                    # Delete any temporary data.
                    self.path_temp.unlink()
                self.set_state("done")
            else:  # only verify if we have self.temp_path
                self.set_state("verify")
                if self.condensed:
                    # do not perform ETag/SHA256 check
                    # TODO:
                    #  - Check whether the condensed file can be opened
                    #    with dclab?
                    #  - Verify the ETag of the condensed file (S3 normally
                    #    sends the ETag in the header, if the file was not
                    #    uploaded via multipart, then we can just verify the
                    #    MD5 sum)?
                    self.path_temp.rename(self.path)
                    self.set_state("done")
                else:
                    # perform ETag/SHA256 check
                    res_dict = self.get_resource_dict()
                    rid = self.resource_id
                    # Can we verify the SHA256 sum?
                    sha256_expected = res_dict.get("sha256")
                    if sha256_expected is None:
                        # The server has not yet computed the SHA256 sum
                        # of the resource. This can happen when we are
                        # downloading a resource immediately after it was
                        # uploaded. Instead of verifying he SHA256 sum,
                        # verify the ETag of the file.
                        # TODO: Compute the ETag during download.
                        logger.info(f"Resource {rid} has no SHA256 set, "
                                    f"falling back to ETag verification.")
                        etag_expected = res_dict.get("etag")
                        if etag_expected is None:
                            self.traceback = (f"Neither SHA256 nor ETag "
                                              f"defined for resource {rid}")
                            self.set_state("error")
                        else:
                            etag_actual = etagsum(self.path_temp)
                            if etag_expected != etag_actual:
                                self.traceback = (
                                    f"ETag verification failed for resource "
                                    f"{rid} ({self.path_temp})")
                                self.set_state("error")
                            else:
                                logger.info(f"ETag verified ({rid})")
                                self.path_temp.rename(self.path)
                                self.set_state("done")
                    else:
                        if self.sha256sum_dl is None:
                            logger.info(f"Computing SHA256 for resource {rid} "
                                        f"({self.path_temp})")
                            self.sha256sum_dl = sha256sum(self.path_temp)
                        sha256_actual = self.sha256sum_dl
                        if sha256_expected != sha256_actual:
                            self.traceback = (f"SHA256 sum check failed for "
                                              f"{self.path}!")
                            self.set_state("error")
                        else:
                            logger.info(f"SHA256 verified ({rid})")
                            self.path_temp.rename(self.path)
                            self.set_state("done")
        elif self.state != "done":  # ignore state "done" [sic!]
            # Only issue this warning if the download is not already done.
            warnings.warn("Resource verification is only possible when state "
                          + "is 'downloaded', but current state is "
                          + "'{}'!".format(self.state))
