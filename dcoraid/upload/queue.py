import pathlib
import time
import warnings

from .job import UploadJob
from .task import load_task, save_task
from .kthread import KThread


class PersistentUploadJobList:
    def __init__(self, path):
        """A file-system and JSON-based persistent UploadJob list"""
        self.path = pathlib.Path(path)
        self.path_completed = self.path / "completed"
        self.path_queued = self.path / "queued"
        self.path_completed.mkdir(parents=True, exist_ok=True)
        self.path_queued.mkdir(parents=True, exist_ok=True)

    def get_queued_dataset_ids(self):
        return [pp.stem for pp in self.path_queued.glob("*.json")]

    def immortalize_job(self, upload_job):
        """Put this job in the persistent queue list"""
        pout = self.path_queued / (upload_job.dataset_id + ".json")
        save_task(upload_job=upload_job, path=pout)

    def obliterate_job(self, dataset_id):
        """Remove a job from the persistent queue list"""
        pdel = self.path_queued / (dataset_id + ".json")
        pdel.unlink()

    def set_job_done(self, dataset_id):
        """Move the job from the queue to the complete list"""
        pin = self.path_queued / (dataset_id + ".json")
        pout = self.path_completed / (dataset_id + ".json")
        pin.rename(pout)

    def summon_job(self, dataset_id, api):
        """Instantiate job from the persistent queue list"""
        pin = self.path_queued / (dataset_id + ".json")
        upload_job = load_task(path=pin, api=api)
        assert upload_job.dataset_id == dataset_id
        return upload_job


class UploadQueue:
    def __init__(self, api, path_persistent_job_list=None):
        """Manager for running multiple UploadJobs in sequence

        Parameters
        ----------
        api: dclab.api.CKANAPI
            The CKAN/DCOR API instance used for the uploads
        path_persistent_job_list: str or pathlib.Path
            Path to a directory for storing UploadJobs in a
            persistent manner across restarts.
        """
        self.api = api.copy()
        if not api.api_key:
            warnings.warn("No API key is set! Upload will not work!")
        self.jobs = []
        if path_persistent_job_list is not None:
            self.jobs_eternal = PersistentUploadJobList(
                path_persistent_job_list)
            # add any previously queued jobs
            for dataset_id in self.jobs_eternal.get_queued_dataset_ids():
                uj = self.jobs_eternal.summon_job(dataset_id, api=self.api)
                self.jobs.append(uj)
        else:
            self.jobs_eternal = None
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
            self.daemon_upload.terminate()
            self.daemon_upload = UploadDaemon(self.jobs)

    def add_job(self, dataset_id, paths, resource_names=None,
                supplements=None):
        """Create an UploadJob and add it to the job list"""
        uj = UploadJob(
            api=self.api,
            dataset_id=dataset_id,
            resource_paths=paths,
            resource_names=resource_names,
            resource_supplements=supplements,
            )

        if self.jobs_eternal is not None:
            # also add to shelf for persistence
            self.jobs_eternal.immortalize_job(uj)

        self.jobs.append(uj)

    def get_job(self, dataset_id):
        """Return the queued UploadJob belonging to the dataset ID"""
        for job in self.jobs:
            if job.dataset_id == dataset_id:
                return job
        else:
            raise KeyError("Job '{}' not found!".format(dataset_id))

    def get_status(self, dataset_id):
        """Return the status of an UploadJob"""
        self.get_job(dataset_id).get_status()

    def remove_job(self, dataset_id):
        """Remove an UploadJob from the queue and perform cleanup

        It has not been tested what happens when a running job
        is aborted. It will probably keep running and then complain
        about resources that are expected to be there. Don't do it.
        """
        for ii, job in enumerate(list(self.jobs)):
            if job.dataset_id == dataset_id:
                self.jobs.pop(ii)
                job.cleanup()
        # also remove from shelf
        if self.jobs_eternal is not None:
            self.jobs_eternal.obliterate_job(dataset_id)


class Daemon(KThread):
    def __init__(self, queue, job_trigger_state, job_function_name):
        """Daemon base class"""
        self.queue = queue
        self.state = "running"
        self.job_trigger_state = job_trigger_state
        self.job_function_name = job_function_name
        super(Daemon, self).__init__()
        self.daemon = True  # We don't have to worry about ending this thread
        self.start()

    def join(self, *args, **kwargs):
        """Join thread by breaking the while loop"""
        self.state = "exiting"
        super(Daemon, self).join(*args, **kwargs)
        assert self.state == "exited"

    def run(self):
        while True:
            if self.state == "exiting":
                self.state = "exited"
                break
            elif self.state != "running":
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
