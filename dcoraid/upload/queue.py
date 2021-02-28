import time
import warnings

from .job import UploadJob
from .kthread import KThread


class UploadQueue(object):
    def __init__(self, api):
        self.api = api.copy()
        if not api.api_key:
            warnings.warn("No API key is set! Upload will not work!")
        self.jobs = []
        self.daemon_compress = CompressDaemon(self.jobs)
        self.daemon_upload = UploadDaemon(self.jobs)
        self.daemon_verify = VerifyDaemon(self.jobs)

    def __getitem__(self, index):
        return self.jobs[index]

    def __len__(self):
        return len(self.jobs)

    def abort_job(self, dataset_id):
        """Abort a running job but don't remove it from the queue"""
        job = self.get_job(dataset_id)
        if job.state == "transfer":
            job.set_state("abort")
            # https://github.com/requests/toolbelt/issues/297
            self.dameon_upload.terminate()
            self.daemon_upload = UploadDaemon(self.jobs)

    def add_job(self, dataset_dict, paths, resource_names=None,
                supplements=None):
        """Add a job to the job list"""
        job = UploadJob(dataset_dict=dataset_dict,
                        paths=paths,
                        resource_names=resource_names,
                        supplements=supplements,
                        api=self.api)
        self.jobs.append(job)

    def get_job(self, dataset_id):
        """Return the job instance belonging to the dataset ID"""
        for job in self.jobs:
            if job.dataset_id == dataset_id:
                return job
        else:
            raise KeyError("Job '{}' not found!".format(dataset_id))

    def get_status(self, dataset_id):
        """Return the status of an upload job"""
        self.get_job(dataset_id).get_status()

    def remove_job(self, dataset_id):
        """Remove a job from the queue and perform cleanup

        It has not been tested what happens when a running job
        is aborted. It will probably keep running and then complain
        about resources that are expected to be there. Don't do it.
        """
        for ii, job in enumerate(list(self.jobs)):
            if job.dataset_id == dataset_id:
                self.jobs.pop(ii)
                job.cleanup()


class Daemon(KThread):
    daemon = True  # We don't have to worry about ending this thread

    def __init__(self, queue, job_trigger_state, job_function_name):
        """Daemon base class"""
        self.queue = queue
        self.state = "running"
        self.job_trigger_state = job_trigger_state
        self.job_function_name = job_function_name
        super(Daemon, self).__init__()
        self.start()

    def run(self):
        while True:
            if self.state != "running":
                # Don't do anything
                time.sleep(.1)
                continue
            else:
                # Get the first job that is in the trigger state
                for job in self.queue:
                    if job.state == self.job_trigger_state:
                        break
                else:
                    # Nothing to do, sleep a little to avoid 100% CPU
                    time.sleep(.1)
                    continue
                # Perform daemon task
                task = getattr(job, self.job_function_name)
                task()


class CompressDaemon(Daemon):
    def __init__(self, jobs):
        """Compression daemon"""
        super(CompressDaemon, self).__init__(
            jobs,
            job_trigger_state="init",
            job_function_name="task_compress_resources")


class UploadDaemon(Daemon):
    def __init__(self, jobs):
        """Upload daemon"""
        super(UploadDaemon, self).__init__(
            jobs,
            job_trigger_state="parcel",
            job_function_name="task_upload_resources")


class VerifyDaemon(Daemon):
    def __init__(self, jobs):
        """Verify daemon"""
        super(VerifyDaemon, self).__init__(
            jobs,
            job_trigger_state="online",
            job_function_name="task_verify_resources")
