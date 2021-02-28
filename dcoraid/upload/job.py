from functools import lru_cache
import hashlib
import pathlib
import shutil
import time
import traceback as tb
import warnings

import appdirs
from dclab.rtdc_dataset.check import IntegrityChecker
from dclab.cli import compress

from ..api import APIError

from . import dataset


#: Valid job states (in more or less chronological order)
JOB_STATES = [
    "init",  # inital
    "compress",  # compression (only for DC data)
    "parcel",  # ready for upload
    "transfer",  # upload in progress
    "online",  # dataset has been transferred
    "verify",  # perform SHA256 sum verification
    "finalize",  # CKAN dataset is being activated
    "done",  # job finished
    "abort",  # user aborted
    "error",  # error occured
]


class UploadJob(object):
    def __init__(self, dataset_dict, paths, api,
                 resource_names=None, supplements=None):
        """Wrapper for resource uploads

        This job is meant to be run from a separate thread.
        The function `task_compress_resources` ensures that all
        resources are compressed. The function `task_upload_resources`
        performs the actual upload. During upload, the progress
        is monitored and can be read from other threads
        using the e.g. `get_status` function.
        """
        self.dataset_dict = dataset_dict
        self.dataset_id = dataset_dict["id"]
        self.api = api.copy()  # create a copy of the API
        self.paths = paths
        if resource_names is None:
            resource_names = [pathlib.Path(pp).name for pp in paths]
        #: Important supplementary resource schema meta data that will
        #: be formatted to composite {"sp:section:key" = value} an appended
        #: to the resource metadata.
        self.supplements = supplements
        self.resource_names = resource_names
        self.paths_uploaded = []
        self.set_state("init")
        self.traceback = None
        self.file_sizes = [pathlib.Path(ff).stat().st_size for ff in paths]
        self.file_bytes_uploaded = [0] * len(paths)
        self.index = 0
        self.start_time = None
        self.end_time = None
        self._last_time = 0
        self._last_bytes = 0
        self._last_rate = 0
        # caching
        dcoraid_cache = pathlib.Path(appdirs.user_cache_dir()) / "dcoraid"
        self.cache_dir = dcoraid_cache / "compress-{}".format(self.dataset_id)

    def cleanup(self):
        """cleanup temporary files in the user's cache directory"""
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def get_composite_supplements(self, resource_index):
        """Return the composite supplements "sp:section:key" for a resource"""
        sp = {}
        if self.supplements:
            sp_dict = self.supplements[resource_index]
            for sec in sp_dict:
                for key in sp_dict[sec]:
                    sp["sp:{}:{}".format(sec, key)] = sp_dict[sec][key]
        return sp

    def get_dataset_url(self):
        """Return a link to the dataset on DCOR"""
        # The API prepends missing "https://"
        return "{}/dataset/{}".format(self.api.server, self.dataset_id)

    def get_progress_string(self):
        """Return a nice string representation of the progress"""
        status = self.get_status()
        state = status["state"]
        plural = "s" if status["files total"] > 1 else ""

        if state in ["init", "compress", "parcel"]:
            progress = "0% ({} file{})".format(status["files total"], plural)
        elif state == "transfer":
            progress = "{:.0f}% (file {}/{})".format(
                status["bytes uploaded"]/status["bytes total"]*100,
                status["files uploaded"]+1,
                status["files total"])
        elif state in ["finalize", "online", "verify", "done"]:
            progress = "100% ({} file{})".format(status["files total"],
                                                 plural)
        elif state in ["abort", "error"]:
            progress = "-- ({}/{} file{})".format(status["files uploaded"],
                                                  status["files total"],
                                                  plural)
        return progress

    def get_rate_string(self):
        """Return a nice string representing upload rate"""
        status = self.get_status()
        state = status["state"]
        rate = status["rate"]
        if state in ["init", "compress", "parcel"]:
            rate_label = "-- kB/s"
        else:
            if rate > 1e6:
                rate_label = "{:.1f} MB/s".format(rate/1e6)
            else:
                rate_label = "{:.0f} kB/s".format(rate/1e3)
            if state != "transfer":
                rate_label = "⌀ " + rate_label
        return rate_label

    def get_rate(self, resolution=1):
        """Return the current resource upload rate

        Parameters
        ----------
        resolution: float
            Time interval in which to perform the average.

        Returns
        -------
        upload_rate: float
            Mean upload rate in bytes per second
        """
        cur_time = time.perf_counter()
        cur_bytes = sum(self.file_bytes_uploaded)

        delta_time = cur_time - self._last_time
        delta_bytes = cur_bytes - self._last_bytes

        if self.start_time is None:
            # not started yet
            rate = 0
            self._last_bytes = 0
        elif self.end_time is None:
            # not finished yet
            if delta_time > resolution:
                rate = delta_bytes / delta_time
                self._last_time = cur_time
                self._last_bytes = cur_bytes
            else:
                rate = self._last_rate
        else:
            # finished
            tdelt = (self.end_time - self.start_time)
            rate = sum(self.file_bytes_uploaded) / tdelt
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
            "files total": len(self.paths),
            "files uploaded": len(self.paths_uploaded),
            "bytes total": sum(self.file_sizes),
            "bytes uploaded": sum(self.file_bytes_uploaded),
            "rate": self.get_rate(),
        }
        return data

    def monitor_callback(self, monitor):
        """Upload progress monitor callback function

        This method updates `self.file_sizes` and
        `self.file_bytes_uploaded` for the currently
        uploading resource.
        """
        self.file_sizes[self.index] = monitor.len
        self.file_bytes_uploaded[self.index] = monitor.bytes_read

    def retry_upload(self):
        """Retry uploading resources when an error occured"""
        if self.state in ["abort", "error"]:
            self.set_state("parcel")
        else:
            raise ValueError("Can only retry upload in error state!")

    def set_state(self, state):
        """Set the current job state

        The state is checked against :const:`JOB_STATES`
        """
        if state not in JOB_STATES:
            raise ValueError("Unknown state: '{}'".format(state))
        self.state = state

    def task_compress_resources(self):
        """Compress resources if they are not fully compressed

        Data are stored in the user's cache directory and
        deleted after upload is complete.
        """
        self.set_state("compress")
        for ii, path in enumerate(list(self.paths)):
            if path.suffix in [".rtdc", ".dc"]:  # do we have an .rtdc file?
                # check for compression
                with IntegrityChecker(path) as ic:
                    cdata = ic.check_compression()[0].data
                if cdata["uncompressed"]:  # (partially) not compressed?
                    res_dir = self.cache_dir / "{}".format(ii)
                    res_dir.mkdir(exist_ok=True, parents=True)
                    path_out = res_dir / path.name
                    compress(path_out=path_out, path_in=path)
                    self.paths[ii] = path_out
        self.set_state("parcel")

    def task_upload_resources(self):
        """Start the upload

        The progress of the upload is monitored and written
        to attributes. The current status can be retrieved
        via :func:`UploadJob.get_status`.
        """
        if self.state == "parcel":
            try:
                self.set_state("transfer")
                self.start_time = time.perf_counter()
                # Do the things to do and watch self.state while doing so
                for ii, path in enumerate(self.paths):
                    self.index = ii
                    resource_name = self.resource_names[ii]
                    resource_supplement = self.get_composite_supplements(ii)
                    exists = dataset.resource_exists(
                        dataset_id=self.dataset_id,
                        resource_name=resource_name,
                        api=self.api)
                    if exists:
                        # We are currently retrying an upload. If the
                        # resource exists, we have already uploaded it.
                        self.file_bytes_uploaded[ii] = self.file_sizes[ii]
                        continue
                    if not exists:
                        try:
                            dataset.add_resource(
                                dataset_id=self.dataset_id,
                                path=path,
                                resource_name=resource_name,
                                resource_dict=resource_supplement,
                                api=self.api,
                                monitor_callback=self.monitor_callback)
                            self.paths_uploaded.append(path)
                        except APIError:
                            # Workaround for large datasets (no response)
                            # just check whether the resource is there
                            exists = dataset.resource_exists(
                                dataset_id=self.dataset_id,
                                resource_name=resource_name,
                                api=self.api)
                            if not exists:
                                raise
                        except SystemExit:
                            # This thread has just been killed
                            self.start_time = None
                            self.file_bytes_uploaded[ii] = 0
                            self.set_state("abort")
                            return
                self.end_time = time.perf_counter()
                self.set_state("online")
            except BaseException:
                self.set_state("error")
                self.traceback = tb.format_exc()
        else:
            warnings.warn("Starting an upload only possible when state is "
                          + "'parcel', but current state is "
                          + "'{}'!".format(self.state))

    def task_verify_resources(self):
        """Perform SHA256 verification"""
        if self.state == "online":
            # First check whether all SHA256 sums are already available online
            sha256dict = dataset.resource_sha256_sums(
                dataset_id=self.dataset_id,
                api=self.api)
            if sum([sha256dict[name] is None for name in sha256dict]) != 0:
                # only start verification if all SHA256 sums are available
                pass
            else:
                self.set_state("verify")
                for ii, path in enumerate(self.paths):
                    resource_name = self.resource_names[ii]
                    # compute SHA256 sum
                    sha = sha256sum(path)
                    if sha != sha256dict[resource_name]:
                        self.set_state("error")
                        self.traceback = "SHA256 sum failed for resource " \
                            + "'{}' ({} vs. {})!".format(
                                resource_name, sha, sha256dict[resource_name])
                        break
                else:
                    self.cleanup()
                    # finalize dataset
                    self.set_state("finalize")
                    # draft -> active
                    dataset.activate_dataset(
                        dataset_id=self.dataset_id,
                        api=self.api)
                    self.set_state("done")
        else:
            warnings.warn("Resource verification is only possible when state "
                          + "is 'online', but current state is "
                          + "'{}'!".format(self.state))


@lru_cache(maxsize=2000)
def sha256sum(path):
    """Compute the SHA256 sum of a file on disk"""
    block_size = 2**20
    path = pathlib.Path(path)
    file_hash = hashlib.sha256()
    with path.open("rb") as fd:
        while True:
            data = fd.read(block_size)
            if not data:
                break
            file_hash.update(data)
    return file_hash.hexdigest()
