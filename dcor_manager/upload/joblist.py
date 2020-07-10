import pathlib
from threading import Thread
import time

import requests
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor


class UploadJob(object):
    def __init__(self, dataset_id, paths, server, api_key):
        self.dataset_id = dataset_id
        self.server = server
        self.api_key = api_key
        self.paths = paths
        self.paths_uploaded = []
        self.state = "queued"
        self.file_sizes = [pathlib.Path(ff).stat().st_size for ff in paths]
        self.file_bytes_uploaded = [0] * len(paths)
        self.index = 0

    def get_status(self):
        """Get the status of the current job"""
        data = {
            "state": self.state,
            "files total": len(self.paths),
            "files uploaded": len(self.paths_uploaded),
            "bytes total": sum(self.file_sizes),
            "bytes uploaded": sum(self.file_bytes_uploaded),
        }
        return data

    def monitor_callback(self, monitor):
        self.file_sizes[self.index] = monitor.len
        self.file_bytes_uploaded[self.index] = monitor.bytes_read

    def start(self):
        """Start the upload"""
        self.state = "running"
        # Do the things to do and watch self.state while doing so
        url = "https://{}/api/3/resource_create".format(self.server)
        for ii, path in enumerate(self.paths):
            self.index = ii
            e = MultipartEncoder(
                fields={'package_id': self.dataset_id,
                        'name': path.name,
                        'upload': (path.name, path.open('rb'))}
            )

            m = MultipartEncoderMonitor(e, self.monitor_callback)
            headers = {"Authorization": self.api_key,
                       "Content-Type": m.content_type}
            req = requests.post(url,
                                data=e,
                                headers=headers,
                                )
            if not req.ok:
                raise ValueError(
                    "Upload failed. Reason: {}".format(req.reason))
            self.paths_uploaded.append(path)
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

    def abort_job(self, dataset_id):
        """Abort a running job"""
        self.get_job(dataset_id).stop()

    def add_job(self, dataset_id, paths):
        """Add a job to the job list"""
        job = UploadJob(dataset_id=dataset_id,
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

    def run(self):
        while True:
            if self.status != "running":
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
