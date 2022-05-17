import copy
import functools
import pathlib
import shutil
import time
import warnings

import requests

from ..common import sha256sum

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
        self.path = pathlib.Path(download_path)
        self.condensed = condensed
        self.path_temp = None
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

    @functools.lru_cache(maxsize=100)
    def get_resource_dict(self):
        """Return resource dictionary"""
        return self.api.get("resource_show", id=self.resource_id)

    @functools.lru_cache(maxsize=100)
    def get_dataset_dict(self):
        res_dict = self.get_resource_dict()
        ds_dict = self.api.get("package_show", id=res_dict["package_id"])
        return ds_dict

    def get_download_path(self):
        """Return the final location to which the file will be downloaded"""
        rsdict = self.get_resource_dict()
        ds_name = self.get_dataset_dict()["name"]
        if self.condensed and rsdict["mimetype"] == "RT-DC":
            stem, suffix = rsdict["name"].rsplit(".", 1)
            res_name = stem + "_condensed." + suffix
        else:
            res_name = rsdict["name"]
        ds_dir = self.path / ds_name
        ds_dir.mkdir(exist_ok=True, parents=True)
        return ds_dir / res_name

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
        data = {
            "state": self.state,
            "bytes total": self.file_size,
            "bytes downloaded": self.file_bytes_downloaded,
            "rate": self.get_rate(),
        }
        if self.path is not None and self.path.is_file():
            data["bytes local"] = self.path.stat().st_size
        elif self.path_temp is not None and self.path_temp.is_file():
            data["bytes local"] = self.path_temp.stat().st_size
        else:
            data["bytes local"] = 0

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
        self.state = state

    def task_download_resource(self):
        """Start the download

        The progress of the download is monitored and written
        to the attributes. The current status can be retrieved
        via :func:`DownloadJob.get_status`.
        """
        if self.state in ["init", "wait-disk"]:
            # set-up temporary path
            if self.path.is_dir():
                # if a directory is given, we prepend the dataset name
                self.path = self.get_download_path()
            if self.path.exists():
                self.start_time = None
                self.end_time = None
                self.file_bytes_downloaded = 0
                self.set_state("downloaded")
            else:
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
                    if self.path_temp.exists():
                        # resume previous download
                        bytes_present = self.path_temp.stat().st_size
                        headers["Range"] = f"bytes={bytes_present}-"
                    with requests.get(url,
                                      stream=True,
                                      headers=headers,
                                      verify=self.api.verify,
                                      timeout=29.9) as r:
                        r.raise_for_status()
                        with self.path_temp.open('ab') as f:
                            chunk_size = 1024 * 1024
                            for chunk in r.iter_content(chunk_size=chunk_size):
                                # If you have chunk encoded response uncomment
                                # if and set chunk_size parameter to None.
                                # if chunk:
                                f.write(chunk)
                                self.file_bytes_downloaded += len(chunk)
                    self.end_time = time.perf_counter()
                    self.set_state("downloaded")
        else:
            warnings.warn("Starting a download only possible when state is "
                          + "'init' or 'wait-disk', but current state is "
                          + "'{}'!".format(self.state))

    def task_verify_resource(self):
        """Perform SHA256 verification"""
        if self.state == "downloaded":
            if self.path is not None and self.path.is_file():
                if self.path_temp is not None and self.path_temp.exists():
                    self.path_temp.unlink()
                self.set_state("done")
            else:  # only verify if we have self.temp_path
                self.set_state("verify")
                if self.condensed:
                    # do not perform SHA256 check
                    # TODO:
                    #  - Check whether the condensed file can be opened
                    #    with dclab?
                    self.path_temp.rename(self.path)
                    self.set_state("done")
                else:
                    # First check whether all SHA256 sums are already available
                    # online
                    if (self.get_resource_dict()["sha256"]
                            != sha256sum(self.path_temp)):
                        self.set_state("error")
                        self.traceback = "SHA256 sum check failed!"
                    else:
                        self.path_temp.rename(self.path)
                        self.set_state("done")
        elif self.state != "done":  # ignore state "done" [sic!]
            # Only issue this warning if the download is not already done.
            warnings.warn("Resource verification is only possible when state "
                          + "is 'downloaded', but current state is "
                          + "'{}'!".format(self.state))
