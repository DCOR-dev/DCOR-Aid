import tempfile
import pathlib
import shutil
import time
import warnings

from dclab.rtdc_dataset.check import IntegrityChecker
from dclab.cli import compress

from ..api import (dataset_activate, resource_add, resource_exists,
                   resource_sha256_sums)
from ..common import sha256sum


#: Valid job states (in more or less chronological order)
JOB_STATES = [
    "init",  # initial
    "wait-disk",  # waiting for free disk space to compress
    "compress",  # compression (only for DC data)
    "parcel",  # ready for upload
    "transfer",  # upload in progress
    "online",  # dataset has been transferred
    "wait-dcor",  # resources are processed by DCOR
    "verify",  # perform SHA256 sum verification
    "finalize",  # CKAN dataset is being activated
    "done",  # job finished
    "abort",  # user aborted
    "error",  # error occurred
]

#: taken from ckanext-dcor_schemas
VALID_RESOURCE_CHARS = "abcdefghijklmnopqrstuvwxyz" \
                       + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" \
                       + "0123456789" \
                       + ".,-_+()[]"
#: same as `VALID_RESOURCE_CHARS` only as regexp
VALID_RESOURCE_REGEXP = r"^[a-zA-Z0-9\.,\-_\+\(\)\[\]]+$"


class UploadJob:
    def __init__(self, api, dataset_id, resource_paths,
                 resource_names=None, resource_supplements=None,
                 task_id=None, cache_dir=None):
        """Wrapper for resource uploads

        This job is meant to be run from a separate thread.
        The function `task_compress_resources` ensures that all
        resources are compressed. The function `task_upload_resources`
        performs the actual upload. During upload, the progress
        is monitored and can be read from other threads
        using the e.g. `get_status` function.

        Parameters
        ----------
        api: dcoraid.api.CKANAPI
            The CKAN/DCOR API instance used for the upload
        dataset_id: str
            ID of the CKAN/DCOR dataset
        resource_paths: list
            Paths to the dataset resources
        resource_names: list
            Corresponding names of the resources as they should appear
            on DCOR
        resource_supplements: list of dict
            Supplementary resource information
        task_id: str
            Unique task ID (used for identifying jobs uploaded already)
        cache_dir: str or pathlib.Path
            Cache directory for storing compressed .rtdc files;
            if not supplied, a temporary directory is created
        """
        self.api = api.copy()  # create a copy of the API
        self.dataset_id = dataset_id
        # make sure the dataset_id is valid
        self.api.get("package_show", id=self.dataset_id)
        self.paths = [pathlib.Path(pp).resolve() for pp in resource_paths]
        if resource_names is None:
            resource_names = [pp.name for pp in self.paths]
        # make sure that only valid characters are used as resource names
        resource_names = [valid_resource_name(rn) for rn in resource_names]
        #: Important supplementary resource schema meta data that will
        #: be formatted to composite {"sp:section:key" = value} an appended
        #: to the resource metadata.
        self.supplements = resource_supplements
        self.resource_names = resource_names
        self.task_id = task_id
        self.paths_uploaded = []
        self.paths_uploaded_before = []
        self.state = None
        self.set_state("init")
        self.traceback = None
        self.file_sizes = [pathlib.Path(ff).stat().st_size
                           for ff in self.paths]
        self.file_bytes_uploaded = [0] * len(self.paths)
        self.index = 0
        self.start_time = None
        self.end_time = None
        self.wait_time = 0
        self._last_time = 0
        self._last_bytes = 0
        self._last_rate = 0
        # caching
        if cache_dir is None:
            cache_dir = pathlib.Path(tempfile.mkdtemp(
                prefix=f"dcoraid_upload_compress-{self.dataset_id}_"))
        else:
            cache_dir = pathlib.Path(cache_dir) / f"compress-{self.dataset_id}"
        self.cache_dir = cache_dir

    def __getstate__(self):
        """Get the state of the UploadJob instance

        This is not the state of the upload! For monitoring the
        upload, please see :func:`UploadJob.get_status`,
        :const:`UploadJob.state`, and :func:`UploadJob.set_state`.

        See Also
        --------
        from_upload_job_state: to recreate a job from a state
        """
        uj_state = {
            "dataset_id": self.dataset_id,
            "resource_paths": [str(pp) for pp in self.paths],
            "resource_names": self.resource_names,
            "resource_supplements": self.supplements,
            "task_id": self.task_id,
        }
        return uj_state

    @staticmethod
    def from_upload_job_state(uj_state, api, cache_dir=None):
        """Reinstantiate a job from an `UploadJob.__getstate__` dict

        Note that there is no `UploadJob.__setstate__` function,
        because that would just not be possible/debugabble when one
        of the tasks (compress, upload) is running. The upload job
        is implemented in such a way that it will just skip existing
        resources, so it will just go through the states until it
        is "done", even if there is nothing to upload. BTW this is
        also a great way of making sure that an upload is complete.

        Note
        ----
        The `uj_state` dictionary must contain the "dataset_id",
        otherwise we won't know where to upload the resources to.
        """
        return UploadJob(api=api, cache_dir=cache_dir, **uj_state)

    @property
    def id(self):
        return self.dataset_id

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

        if state in ["init", "compress", "parcel", "wait-disk"]:
            progress = "0% ({} file{})".format(status["files total"], plural)
        elif state == "transfer":
            progress = "{:.0f}% (file {}/{})".format(
                status["bytes uploaded"]/status["bytes total"]*100,
                status["files uploaded"]+1,
                status["files total"])
        elif state in ["finalize", "online", "wait-dcor", "verify", "done"]:
            progress = "100% ({} file{})".format(status["files total"],
                                                 plural)
        elif state in ["abort", "error"]:
            progress = "-- ({}/{} file{})".format(status["files uploaded"],
                                                  status["files total"],
                                                  plural)
        elif state in JOB_STATES:
            # seems like you missed updating a case here?
            warnings.warn(f"Please add state '{state}' to these cases!")
            progress = "undefined"
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

    def get_rate(self, resolution=3.05):
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
        # get bytes of files that have been uploaded
        cur_bytes = 0
        for ii, path in enumerate(self.paths):
            if path not in self.paths_uploaded_before:
                # only count files from the current session
                cur_bytes += self.file_bytes_uploaded[ii]

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
            tdelt = (self.end_time - self.start_time - self.wait_time)
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
            self.set_state("init")
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
        # make sure that we have .rtdc or .dc files
        dc_files = [pp for pp in self.paths if pp.suffix in [".rtdc", ".dc"]]
        if not dc_files:
            raise ValueError("There are no RT-DC files in this dataset!")
        self.set_state("compress")
        for ii, path in enumerate(self.paths):
            if path.suffix in [".rtdc", ".dc"]:  # do we have an .rtdc file?
                with IntegrityChecker(path) as ic:
                    # check for compression
                    cdata = ic.check_compression()[0].data
                    # perform a sanity check for messy data
                    insane = [c.msg for c in ic.sanity_check()]
                    # check for features not defined in dclab
                    insane += [c.msg for c in ic.check_features_unknown_hdf5()]
                    if insane:
                        # The user is responsible for cleaning up the mess.
                        # We just make sure no dirty data gets uploaded to
                        # DCOR.
                        # The check_features_unknown_hdf5 cues are not messy,
                        # but the dclab-compressed file would not contain such
                        # features. So we just fail and let the user remove
                        # the features knowingly (e.g. via DCKit).
                        raise IOError(f"Sanity check failed for {path}!\n"
                                      + "\n".join(insane))
                if cdata["uncompressed"]:  # (partially) not compressed?
                    res_dir = self.cache_dir / "{}".format(ii)
                    res_dir.mkdir(exist_ok=True, parents=True)
                    path_out = res_dir / path.name
                    # Check if the output path already exists:
                    # We can trust dclab version 0.34.4 that the
                    # compression was successful, because it uses
                    # temporary file paths and only renames the
                    # temporary files to the output files when
                    # the conversion was successful.
                    if not path_out.exists():
                        # perform compression
                        size_in = path.stat().st_size
                        while shutil.disk_usage(res_dir).free < size_in:
                            # OK, now there might be the possibility that
                            # there are rogue upload caches from jobs that
                            # were created during previous runs. Since
                            # DCOR-Aid 0.5.4, upload jobs are run in
                            # alphabetical order. This means that we may
                            # delete the upload cache of jobs that would
                            # be uploaded after this job here.
                            # This is important so that for slow uploads,
                            # the hard disk is not filled up and then no more
                            # jobs can be uploaded because the existing cache
                            # data blocks jobs that are earlier in the queue
                            # from compressing themselves.
                            # find all upload job cache candidates
                            cands = sorted(
                                self.cache_dir.parent.glob("compress-*"))
                            # determine the index of the current curve
                            for ci, cand in enumerate(cands):
                                if cand.samefile(self.cache_dir):
                                    # we have found ourself
                                    for rogue in cands[ci+1:]:
                                        # Remove rogue cache data
                                        shutil.rmtree(rogue,
                                                      ignore_errors=True)
                                    # don't continue
                                    break

                            # As long as there is less space free than the
                            # input file size, we stall here.
                            self.set_state("wait-disk")
                            time.sleep(1)
                        self.set_state("compress")
                        compress(path_out=path_out, path_in=path)
                    # replace current path_out with compressed path
                    self.paths[ii] = path_out
        self.set_state("parcel")

    def task_upload_resources(self):
        """Start the upload

        The progress of the upload is monitored and written
        to attributes. The current status can be retrieved
        via :func:`UploadJob.get_status`.
        """
        if self.state == "parcel":
            # reset everything
            self.paths_uploaded.clear()
            self.paths_uploaded_before.clear()
            self.file_bytes_uploaded = [0] * len(self.paths)
            self.index = 0
            self.start_time = time.perf_counter()
            self.end_time = None
            self.wait_time = 0
            self._last_time = 0
            self._last_bytes = 0
            self._last_rate = 0
            # begin transfer
            self.set_state("transfer")
            # Do the things to do and watch self.state while doing so
            for ii, path in enumerate(self.paths):
                self.index = ii
                resource_name = self.resource_names[ii]
                resource_supplement = self.get_composite_supplements(ii)
                exists = resource_exists(
                    dataset_id=self.dataset_id,
                    resource_name=resource_name,
                    resource_dict=resource_supplement,
                    api=self.api)
                if exists:
                    # We are currently retrying an upload. If the
                    # resource exists, we have already uploaded it.
                    self.file_bytes_uploaded[ii] = self.file_sizes[ii]
                    self.paths_uploaded.append(path)
                    self.paths_uploaded_before.append(path)
                    continue
                else:
                    # Normal upload.
                    srv_time = resource_add(
                        dataset_id=self.dataset_id,
                        path=path,
                        resource_name=resource_name,
                        resource_dict=resource_supplement,
                        api=self.api,
                        exist_ok=True,
                        monitor_callback=self.monitor_callback)
                    self.paths_uploaded.append(path)
                    self.wait_time += srv_time
            self.end_time = time.perf_counter()
            self.set_state("online")
        else:
            warnings.warn("Starting an upload only possible when state is "
                          + "'parcel', but current state is "
                          + "'{}'!".format(self.state))

    def task_verify_resources(self):
        """Perform SHA256 verification"""
        if self.state == "online":
            # Make sure that all SHA256 sums are already available online.
            for ii in range(500):
                sha256dict = resource_sha256_sums(
                    dataset_id=self.dataset_id,
                    api=self.api)
                missing = [n for n in sha256dict if sha256dict[n] is None]
                if missing:
                    self.set_state("wait-dcor")
                    time.sleep(1)
                    continue
                else:
                    # all SHA256 sums are available
                    break
            else:
                # things are taking too long
                self.set_state("error")
                msg_parts = ["SHA256 sums not computed by DCOR:"]
                msg_parts += [f" - {name}" for name in missing]
                self.traceback = "\n".join(msg_parts)

            # Only start verification if all SHA256 sums are available.
            if not missing:
                self.set_state("verify")
                bad_sha256 = []
                for ii, path in enumerate(self.paths):
                    resource_name = self.resource_names[ii]
                    # compute SHA256 sum
                    sha = sha256sum(path)
                    if sha != sha256dict[resource_name]:
                        bad_sha256.append(
                            [resource_name, sha, sha256dict[resource_name]])
                if bad_sha256:
                    # we have bad resources, tell the user
                    self.set_state("error")
                    msg_parts = ["SHA256 sum failed for resources:"]
                    for item in bad_sha256:
                        msg_parts.append("'{}' ({} vs. {})!".format(*item))
                    self.traceback = "\n".join(msg_parts)
                else:
                    # green means go: remove temporary files and activate
                    self.cleanup()
                    # finalize dataset
                    self.set_state("finalize")
                    # draft -> active
                    dataset_activate(
                        dataset_id=self.dataset_id,
                        api=self.api)
                    self.set_state("done")
        elif self.state != "done":  # ignore state "done" [sic!]
            # Only issue this warning if the upload is not already done.
            warnings.warn("Resource verification is only possible when state "
                          + "is 'online', but current state is "
                          + "'{}'!".format(self.state))


def valid_resource_name(path_name):
    """Return a valid DCOR resource name, by replacing characters"""
    # convert spaces to underscores
    path_name = path_name.replace(" ", "_")
    # convert all other characters to dots
    new_name = ""
    for char in path_name:
        if char in VALID_RESOURCE_CHARS:
            new_name += char
        else:
            new_name += "."
    return new_name
