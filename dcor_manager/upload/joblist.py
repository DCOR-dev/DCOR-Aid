import pathlib
from threading import Thread
import time

from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from ..api import CKANAPI

from .dataset import activate_dataset


class UploadJob(object):
    def __init__(self, dataset_dict, paths, server, api_key):
        self.dataset_dict = dataset_dict
        self.dataset_id = dataset_dict["id"]
        self.server = server
        self.api_key = api_key
        self.paths = paths
        self.paths_uploaded = []
        self.state = "queued"
        self.file_sizes = [pathlib.Path(ff).stat().st_size for ff in paths]
        self.file_bytes_uploaded = [0] * len(paths)
        self.index = 0
        self.start_time = None
        self.end_time = None
        self._last_time = 0
        self._last_bytes = 0

    def get_rate(self, resolution=1):
        """Get the dataset rate with 1s precision"""
        cur_time = time.perf_counter()
        cur_bytes = sum(self.file_bytes_uploaded)

        if (cur_time - self._last_time) > resolution:
            self._last_time = cur_time
            self._last_bytes = cur_bytes

        if self.start_time is None:
            # not started yet
            rate = 0
        elif self.end_time is None:
            # not finished yet
            tdelt = self._last_time - self.start_time
            rate = self._last_bytes / tdelt
        else:
            # finished
            tdelt = (self.end_time - self.start_time)
            rate = sum(self.file_bytes_uploaded) / tdelt
        return rate

    def get_status(self):
        """Get the status of the current job"""
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
        self.file_sizes[self.index] = monitor.len
        self.file_bytes_uploaded[self.index] = monitor.bytes_read

    def start(self):
        """Start the upload"""
        self.state = "running"
        self.start_time = time.perf_counter()
        # Do the things to do and watch self.state while doing so
        api = CKANAPI(server=self.server, api_key=self.api_key)
        for ii, path in enumerate(self.paths):
            self.index = ii
            e = MultipartEncoder(
                fields={'package_id': self.dataset_id,
                        'name': path.name,
                        'upload': (path.name, path.open('rb'))}
            )
            m = MultipartEncoderMonitor(e, self.monitor_callback)
            api.post("resource_create",
                     data=m,
                     dump_json=False,
                     headers={"Content-Type": m.content_type})
            self.paths_uploaded.append(path)
        self.end_time = time.perf_counter()
        # finalize dataset
        self.state = "finalizing"
        activate_dataset(dataset_id=self.dataset_id,
                         server=self.server,
                         api_key=self.api_key)
        self.state = "finished"

    def stop(self):
        """Stop the upload"""
        self.state = "aborted"


class UploadJobList(object):
    def __init__(self, server, api_key):
        self.server = server
        self.api_key = api_key
        self.jobs = []
        self.runner = UploadRunner(self.jobs)
        self.runner.start()

    def __getitem__(self, index):
        return self.jobs[index]

    def __len__(self):
        return len(self.jobs)

    def abort_job(self, dataset_id):
        """Abort a running job"""
        self.get_job(dataset_id).stop()

    def add_job(self, dataset_dict, paths):
        """Add a job to the job list"""
        job = UploadJob(dataset_dict=dataset_dict,
                        paths=paths,
                        server=self.server,
                        api_key=self.api_key)
        self.jobs.append(job)

    def get_job(self, dataset_id):
        for job in self.jobs:
            if job.dataset_id == dataset_id:
                return job
        else:
            raise KeyError("Job '{}' not found!".format(dataset_id))

    def get_status(self, dataset_id):
        """Return the status of an upload job"""
        self.get_job(dataset_id).get_status()

    def run(self):
        """Commence uploading"""
        # Get the first job that is not running
        self.runner.state

    def start(self, dataset_id):
        job = self.get_job(dataset_id).get_status()
        if job.state == "aborted":
            job.state = "queued"

    def stop(self):
        """Stop uploading"""
        self.runner.state = "paused"
        for job in self.jobs:
            if job.state == "running":
                job.stop()


class UploadRunner(Thread):
    daemon = True  # We don't have to worry about ending this thread

    def __init__(self, jobs):
        """This upload runner is constantly running in the background"""
        self.jobs = jobs
        self.state = "running"
        super(UploadRunner, self).__init__()

    def run(self):
        while True:
            if self.state != "running":
                # Don't do anything
                time.sleep(.1)
                continue
            else:
                # Get the first job that has not been started
                for job in list(self.jobs):
                    if job.state == "queued":
                        break
                else:
                    # Nothing to do, sleep a little to avoid 100% CPU
                    time.sleep(.1)
                    continue
                # Start the job. This will block this thread until the
                # job aborts itself or is finished.
                job.start()
