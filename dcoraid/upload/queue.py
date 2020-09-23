from threading import Thread
import time

from .job import UploadJob


class UploadQueue(object):
    def __init__(self, server, api_key):
        self.server = server
        self.api_key = api_key
        self.jobs = []
        self.compress_runner = CompressRunner(self.jobs)
        self.compress_runner.start()
        self.upload_runner = UploadRunner(self.jobs)
        self.upload_runner.start()

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

    def start(self):
        """Stop uploading"""
        self.compress_runner.state = "running"
        self.upload_runner.state = "running"

    def stop(self):
        """Stop uploading"""
        self.compress_runner.state = "paused"
        self.upload_runner.state = "paused"
        for job in self.jobs:
            job.stop()


class CompressRunner(Thread):
    daemon = True  # We don't have to worry about ending this thread

    def __init__(self, jobs):
        """This compress runner is constantly running in the background"""
        self.jobs = jobs
        self.state = "running"
        super(CompressRunner, self).__init__()

    def run(self):
        while True:
            if self.state != "running":
                # Don't do anything
                time.sleep(.1)
                continue
            else:
                # Get the first job that has not been started
                for job in list(self.jobs):
                    if job.state == "init":
                        break
                else:
                    # Nothing to do, sleep a little to avoid 100% CPU
                    time.sleep(.1)
                    continue
                # Perform data compression.
                job.compress_resources()


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
                # Get the first job that is in the parcel state
                for job in list(self.jobs):
                    if job.state == "parcel":
                        break
                else:
                    # Nothing to do, sleep a little to avoid 100% CPU
                    time.sleep(.1)
                    continue
                # Start the job. This will block this thread until the
                # job aborts itself or is finished.
                job.upload_resources()
