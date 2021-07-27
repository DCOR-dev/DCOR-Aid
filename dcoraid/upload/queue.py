import pathlib
import time
import warnings

from ..api import APINotFoundError
from .job import UploadJob
from .task import LocalTaskResourcesNotFoundError, load_task, save_task
from .kthread import KThread


class DCORAidQueueWarning(UserWarning):
    pass


class PersistentUploadJobList:
    def __init__(self, path):
        """A file-system and JSON-based persistent UploadJob list"""
        self.path = pathlib.Path(path)
        self.path_completed = self.path / "completed"
        self.path_queued = self.path / "queued"
        self.path_completed.mkdir(parents=True, exist_ok=True)
        self.path_queued.mkdir(parents=True, exist_ok=True)

    def __contains__(self, item):
        if isinstance(item, UploadJob):
            dataset_id = item.dataset_id
        else:
            dataset_id = item
        return self.job_exists(dataset_id)

    @property
    def num_completed(self):
        """Return number of completed tasks"""
        return len(list(self.path_completed.glob("*.json")))

    @property
    def num_queued(self):
        """Return number of queued tasks"""
        return len(list(self.path_queued.glob("*.json")))

    def get_queued_dataset_ids(self):
        """Return list of DCOR dataset IDs corresponding to queued jobs"""
        return sorted([pp.stem for pp in self.path_queued.glob("*.json")])

    def is_job_done(self, dataset_id):
        jp = self.path_completed / (dataset_id + ".json")
        return jp.exists()

    def is_job_queued(self, dataset_id):
        jp = self.path_queued / (dataset_id + ".json")
        return jp.exists()

    def immortalize_job(self, upload_job):
        """Put this job in the persistent queue list"""
        pout = self.path_queued / (upload_job.dataset_id + ".json")
        if self.is_job_queued(upload_job.dataset_id):
            raise FileExistsError(f"The job '{upload_job.dataset_id}' is "
                                  f"already present at '{pout}'!")
        elif self.is_job_done(upload_job.dataset_id):
            # This is safe in async mode, because checking for "done"
            # is done after checking for queued (above case).
            raise FileExistsError(f"The job '{upload_job.dataset_id}' is "
                                  f"already done!")
        save_task(upload_job=upload_job, path=pout)

    def job_exists(self, dataset_id):
        return self.is_job_queued(dataset_id) or self.is_job_done(dataset_id)

    def obliterate_job(self, dataset_id):
        """Remove a job from the persistent queue list"""
        pdel = self.path_queued / (dataset_id + ".json")
        pdel.unlink()

    def set_job_done(self, dataset_id):
        """Move the job from the queue to the complete list"""
        pin = self.path_queued / (dataset_id + ".json")
        pout = self.path_completed / (dataset_id + ".json")
        pin.rename(pout)

    def summon_job(self, dataset_id, api, cache_dir=None):
        """Instantiate job from the persistent queue list"""
        pin = self.path_queued / (dataset_id + ".json")
        upload_job = load_task(path=pin, api=api, cache_dir=cache_dir)
        assert upload_job.dataset_id == dataset_id
        return upload_job


class UploadQueue:
    def __init__(self, api, path_persistent_job_list=None, cache_dir=None):
        """Manager for running multiple UploadJobs in sequence

        Parameters
        ----------
        api: dclab.api.CKANAPI
            The CKAN/DCOR API instance used for the uploads
        path_persistent_job_list: str or pathlib.Path
            Path to a directory for storing UploadJobs in a
            persistent manner across restarts.
        cache_dir: str or pathlib.Path
            Cache directory for storing compressed .rtdc files;
            if not supplied, a temporary directory is created for
            each UploadJob
        """
        self.api = api.copy()
        if not api.api_key:
            warnings.warn("No API key is set! Upload will not work!")
        self.cache_dir = cache_dir
        self.jobs = []
        if path_persistent_job_list is not None:
            self.jobs_eternal = PersistentUploadJobList(
                path_persistent_job_list)
            # add any previously queued jobs
            for dataset_id in self.jobs_eternal.get_queued_dataset_ids():
                try:
                    uj = self.jobs_eternal.summon_job(dataset_id,
                                                      api=self.api,
                                                      cache_dir=self.cache_dir)
                except APINotFoundError:
                    pp = self.jobs_eternal.path_queued / (dataset_id + ".json")
                    warnings.warn(f"Datast {dataset_id} could not be found "
                                  f"on {self.api.server}! If the dataset has "
                                  f"been deleted, please remove the local "
                                  f"file {pp}.",
                                  DCORAidQueueWarning)
                except LocalTaskResourcesNotFoundError as e:
                    resstr = ", ".join([str(pp) for pp in e.missing_resources])
                    warnings.warn("The following resources are missing for "
                                  f"dataset {dataset_id}: {resstr}. The "
                                  "job will not be queued.",
                                  DCORAidQueueWarning)
                else:
                    self.jobs.append(uj)
        else:
            self.jobs_eternal = None
        self.daemon_compress = CompressDaemon(self.jobs)
        self.daemon_upload = UploadDaemon(self.jobs)
        self.daemon_verify = VerifyDaemon(self.jobs)

    def __contains__(self, upload_job):
        return upload_job in self.jobs

    def __getitem__(self, index):
        return self.jobs[index]

    def __len__(self):
        return len(self.jobs)

    def find_zombie_caches(self):
        """Return list of cache directories that don't belong to this Queue

        Returns
        -------
        zombies: list of pathlib.Path
            List of zombie cache directories
        """
        if self.cache_dir is None:
            # We can only check if the directory was given
            raise ValueError("UploadQueue was instantiated without cache_dir!")
        else:
            dataset_ids = [job.dataset_id for job in self]
            zombies = []
            for pp in pathlib.Path(self.cache_dir).glob("compress-*"):
                did = pp.name.split("-", 1)[1]
                if did not in dataset_ids:
                    zombies.append(pp)
        return zombies

    def abort_job(self, dataset_id):
        """Abort a running job but don't remove it from the queue"""
        job = self.get_job(dataset_id)
        if job.state == "transfer":
            job.set_state("abort")
            # https://github.com/requests/toolbelt/issues/297
            self.daemon_upload.terminate()
            self.daemon_upload = UploadDaemon(self.jobs)

    def add_job(self, upload_job):
        """Add an UploadJob to the queue"""
        if self.jobs_eternal is not None:
            if upload_job in self.jobs_eternal:
                # Previously, this function would break hard here, because a
                # job cannot be immortalized twice. The thing is, however, that
                # sometimes a user moves the resource files to a different hard
                # drive or folder and then the upload job cannot be summoned
                # (The immortalized job has the wrong resource paths)! We offer
                # the user a workaround: If the dataset_id is in the eternal
                # job list, but not in self (because summoning it failed),
                # then we remove the old task from the eternal job list and
                # add the new task.
                if self.jobs_eternal.is_job_queued(upload_job.dataset_id):
                    try:
                        self.get_job(upload_job.dataset_id)
                    except KeyError:
                        # Job is immortalized, but failed to be summoned to
                        # Queue during initialization.
                        self.jobs_eternal.obliterate_job(upload_job.dataset_id)
                    else:
                        # Job is immortalized and already in the queue.
                        # Everything is fine and we need not worry. Must not
                        # append job!
                        return
                else:
                    # Job is immortalized and already done! Under no
                    # circumstance should we add this job.
                    return
            # Add to eternal jobs for persistence
            self.jobs_eternal.immortalize_job(upload_job)
        self.jobs.append(upload_job)

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

    def new_job(self, dataset_id, paths, resource_names=None,
                supplements=None):
        """Create an UploadJob and add it to the upload queue

        Parameters
        ----------
        dataset_id: str
            The CKAN/DCOR dataset ID
        paths: list of str or list of pathlib.Path
            Paths to the resource to upload
        resource_names: list of str
            The names under which the resources are stored
        supplements: list of dict
            Resource schema supplements

        Returns
        -------
        upload_job: dcoraid.upload.job.UploadJob
            The upload job that was appended to the upload queue
        """
        upload_job = UploadJob(
            api=self.api,
            dataset_id=dataset_id,
            resource_paths=paths,
            resource_names=resource_names,
            resource_supplements=supplements,
            cache_dir=self.cache_dir,
            )
        self.add_job(upload_job)
        return upload_job

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
        # also remove from eternal jobs
        if (self.jobs_eternal is not None
                and self.jobs_eternal.is_job_queued(dataset_id)):
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
