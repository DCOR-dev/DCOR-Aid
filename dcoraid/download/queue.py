import pathlib
import warnings

from ..worker import Daemon

from .job import DownloadJob
from .task import load_task, save_task


class DCORAidQueueWarning(UserWarning):
    pass


class PersistentDownloadJobList:
    def __init__(self, path):
        """A file-system and JSON-based persistent DownloadJob list"""
        self.path = pathlib.Path(path)
        self.path_queued = self.path / "queued"
        self.path_queued.mkdir(parents=True, exist_ok=True)

    def __contains__(self, item):
        if isinstance(item, DownloadJob):
            resource_id = item.resource_id
        else:
            resource_id = item
        return self.job_exists(resource_id)

    @property
    def num_queued(self):
        """Return number of queued tasks"""
        return len(list(self.path_queued.glob("*.json")))

    def get_queued_resource_ids(self):
        """Return list of DCOR resource IDs corresponding to queued jobs"""
        return sorted([pp.stem for pp in self.path_queued.glob("*.json")])

    def is_job_queued(self, resource_id):
        jp = self.path_queued / (resource_id + ".json")
        return jp.exists()

    def immortalize_job(self, download_job):
        """Put this job in the persistent queue list"""
        pout = self.path_queued / (download_job.resource_id + ".json")
        if self.is_job_queued(download_job.resource_id):
            raise FileExistsError(f"The job '{download_job.resource_id}' is "
                                  f"already present at '{pout}'!")
        save_task(download_job=download_job, path=pout)

    def job_exists(self, resource_id):
        return self.is_job_queued(resource_id)

    def obliterate_job(self, resource_id):
        """Remove a job from the persistent queue list"""
        pdel = self.path_queued / (resource_id + ".json")
        pdel.unlink()

    def set_job_done(self, resource_id):
        """Remove a job from the persistent queue list"""
        self.obliterate_job(resource_id)

    def summon_job(self, resource_id, api, cache_dir=None):
        """Instantiate job from the persistent queue list"""
        pin = self.path_queued / (resource_id + ".json")
        download_job = load_task(path=pin, api=api)
        assert download_job.resource_id == resource_id
        return download_job


class DownloadQueue:
    def __init__(self, api, path_persistent_job_list=None):
        """Manager for running multiple downloadJobs in sequence

        Parameters
        ----------
        api: dcoraid.api.CKANAPI
            The CKAN/DCOR API instance used for the downloads
        path_persistent_job_list: str or pathlib.Path
            Path to a directory for storing downloadJobs in a
            persistent manner across restarts.
        """
        self.api = api.copy()
        if not api.api_key:
            warnings.warn("No API key is set! download may not work!")
        self.jobs = []
        if path_persistent_job_list is not None:
            self.jobs_eternal = PersistentDownloadJobList(
                path_persistent_job_list)
            # add any previously queued jobs
            for resource_id in self.jobs_eternal.get_queued_resource_ids():
                dj = self.jobs_eternal.summon_job(resource_id, api=self.api)
                self.jobs.append(dj)
        else:
            self.jobs_eternal = None
        self.daemon_download = DownloadDaemon(self.jobs)
        self.daemon_verify = VerifyDaemon(self.jobs)

    def __contains__(self, download_job):
        return download_job in self.jobs

    def __getitem__(self, index):
        return self.jobs[index]

    def __len__(self):
        return len(self.jobs)

    def abort_job(self, resource_id):
        """Abort a running job but don't remove it from the queue"""
        job = self.get_job(resource_id)
        if job.state == "transfer":
            job.set_state("abort")
            # https://github.com/requests/toolbelt/issues/297
            self.daemon_download.terminate()
            self.daemon_download = DownloadDaemon(self.jobs)

    def add_job(self, download_job):
        """Add a DownloadJob to the queue"""
        if self.jobs_eternal is not None:
            if download_job in self.jobs_eternal:
                self.jobs_eternal.obliterate_job(download_job.resource_id)
            # Add to eternal jobs for persistence
            self.jobs_eternal.immortalize_job(download_job)
        try:
            self.get_job(download_job.resource_id)
        except KeyError:
            self.jobs.append(download_job)

    def get_job(self, resource_id):
        """Return the queued DownloadJob belonging to the resource ID"""
        for job in self.jobs:
            if job.resource_id == resource_id:
                return job
        else:
            raise KeyError("Job '{}' not found!".format(resource_id))

    def get_status(self, resource_id):
        """Return the status of an downloadJob"""
        self.get_job(resource_id).get_status()

    def new_job(self, resource_id, download_path):
        """Create an downloadJob and add it to the download queue

        Parameters
        ----------
        resource_id: str
            The CKAN/DCOR dataset ID
        download_path: str or pathlib.Path
            Download path

        Returns
        -------
        download_job: dcoraid.download.job.DownloadJob
            The download job that was appended to the download queue
        """
        download_job = DownloadJob(
            api=self.api,
            resource_id=resource_id,
            download_path=download_path,
        )
        self.add_job(download_job)
        return download_job

    def remove_job(self, resource_id):
        """Remove a DownloadJob from the queue and perform cleanup

        Running jobs are aborted before they are removed.
        """
        self.abort_job(resource_id)
        for ii, job in enumerate(list(self.jobs)):
            if job.resource_id == resource_id:
                self.jobs.pop(ii)
                # cleanup temp files
                try:
                    job.path_temp.unlink()
                except BaseException:
                    pass
            elif job.state == "abort":
                job.set_state("init")
        # also remove from eternal jobs
        if (self.jobs_eternal is not None
                and self.jobs_eternal.is_job_queued(resource_id)):
            self.jobs_eternal.obliterate_job(resource_id)


class DownloadDaemon(Daemon):
    def __init__(self, jobs):
        """Download daemon"""
        super(DownloadDaemon, self).__init__(
            jobs,
            job_trigger_state="init",
            job_function_name="task_download_resource")


class VerifyDaemon(Daemon):
    def __init__(self, jobs):
        """Verify daemon"""
        super(VerifyDaemon, self).__init__(
            jobs,
            job_trigger_state="downloaded",
            job_function_name="task_verify_resource")
