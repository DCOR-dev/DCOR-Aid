import pathlib
import time
import traceback as tb
import warnings

from .dataset import activate_dataset, add_resource


JOB_STATES = [
    "init",  # inital
    "compress",  # compression (only for DC data)
    "parcel",  # ready for upload
    "transfer",  # upload in progress
    "finalize",  # CKAN dataset is being activated
    "done",  # job finished
    "abort",  # user aborted
    "error",  # error occured
]


class UploadJob(object):
    def __init__(self, dataset_dict, paths, server, api_key):
        """Wrapper for resource uploads

        This job is meant to be run from a separate thread.
        The function `compress_resources` ensures that all
        resources are compressed. The function `upload_resources`
        performs the actual upload. During upload, the progress
        is monitored and can be read from other threads
        using the e.g. `get_status` function.
        """
        self.dataset_dict = dataset_dict
        self.dataset_id = dataset_dict["id"]
        self.server = server
        self.api_key = api_key
        self.paths = paths
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

    def compress_resources(self):
        # TODO
        pass
        self.set_state("parcel")

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
        elif state in ["finalize", "done"]:
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

    def set_state(self, state):
        if state not in JOB_STATES:
            raise ValueError("Unknown state: '{}'".format(state))
        self.state = state

    def upload_resources(self):
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
                    add_resource(dataset_id=self.dataset_id,
                                 path=path,
                                 server=self.server,
                                 api_key=self.api_key,
                                 monitor_callback=self.monitor_callback)
                    self.paths_uploaded.append(path)
                self.end_time = time.perf_counter()
                # finalize dataset
                self.set_state("finalize")
                activate_dataset(dataset_id=self.dataset_id,
                                 server=self.server,
                                 api_key=self.api_key)
                self.set_state("done")
            except BaseException:
                self.set_state("error")
                self.traceback = tb.format_exc()
        else:
            warnings.warn("Starting an upload only possible when state is "
                          + "'parcel', but current state is "
                          + "'{}'!".format(self.state))

    def stop(self):
        """Stop the upload

        This only changes the `self.state` string.
        """
        self.set_state("abort")
        raise NotImplementedError(
            "I don't know how to implement this with "
            + "'requests_toolbelt.MultipartEncoderMonitor'!")
