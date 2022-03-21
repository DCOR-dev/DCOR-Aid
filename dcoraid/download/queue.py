import pathlib
import time
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
        assert isinstance(item, DownloadJob)
        return self.job_exists(item)

    @property
    def num_queued(self):
        """Return number of queued tasks"""
        return len(list(self.path_queued.glob("*.json")))

    def _get_job_path(self, download_job):
        name = download_job.job_id + ".json"
        return self.path_queued / name

    def get_queued_jobs(self, api):
        """Return list of DCOR resource IDs corresponding to queued jobs"""
        jobs = []
        for pp in self.path_queued.glob("*.json"):
            dj = load_task(path=pp, api=api)
            jobs.append(dj)
        return jobs

    def is_job_queued(self, download_job):
        return self._get_job_path(download_job).exists()

    def immortalize_job(self, download_job):
        """Put this job in the persistent queue list"""
        pout = self._get_job_path(download_job)
        if self.is_job_queued(download_job):
            raise FileExistsError(f"The job '{download_job.job_id}' is "
                                  f"already present at '{pout}'!")
        save_task(download_job=download_job, path=pout)

    def job_exists(self, download_job):
        return self.is_job_queued(download_job)

    def obliterate_job(self, download_job):
        """Remove a job from the persistent queue list"""
        pdel = self._get_job_path(download_job)
        pdel.unlink()

    def set_job_done(self, download_job):
        """Remove a job from the persistent queue list"""
        self.obliterate_job(download_job)


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
            for dj in self.jobs_eternal.get_queued_jobs(self.api):
                self.jobs.append(dj)
        else:
            self.jobs_eternal = None
        self.daemon_download = DownloadDaemon(self.jobs)
        self.daemon_verify = VerifyDaemon(self.jobs)

    def __contains__(self, download_job):
        return download_job in self.jobs

    def __del__(self):
        self.daemon_download.shutdown_flag.set()
        self.daemon_verify.shutdown_flag.set()
        time.sleep(.2)
        self.daemon_download.terminate()
        self.daemon_verify.terminate()

    def __getitem__(self, index):
        return self.jobs[index]

    def __len__(self):
        return len(self.jobs)

    def abort_job(self, job_id):
        """Abort a running job but don't remove it from the queue"""
        job = self.get_job(job_id)
        if job.state == "transfer":
            job.set_state("abort")
            # https://github.com/requests/toolbelt/issues/297
            self.daemon_download.terminate()
            self.daemon_download = DownloadDaemon(self.jobs)

    def add_job(self, download_job):
        """Add a DownloadJob to the queue"""
        if self.jobs_eternal is not None:
            if download_job in self.jobs_eternal:
                self.jobs_eternal.obliterate_job(download_job)
            # Add to eternal jobs for persistence
            self.jobs_eternal.immortalize_job(download_job)
        try:
            self.get_job(download_job.job_id)
        except KeyError:
            self.jobs.append(download_job)

    def get_job(self, job_id):
        """Return the queued DownloadJob belonging to the resource ID"""
        for job in self.jobs:
            if job.job_id == job_id:
                return job
        else:
            raise KeyError("Job '{}' not found!".format(job_id))

    def get_status(self, job_id):
        """Return the status of an downloadJob"""
        self.get_job(job_id).get_status()

    def new_job(self, resource_id, download_path, condensed=False):
        """Create an downloadJob and add it to the download queue

        Parameters
        ----------
        resource_id: str
            The CKAN/DCOR dataset ID
        download_path: str or pathlib.Path
            Download path
        condensed: bool
            Whether to download the condensed version.

        Returns
        -------
        download_job: dcoraid.download.job.DownloadJob
            The download job that was appended to the download queue
        """
        download_job = DownloadJob(
            api=self.api,
            resource_id=resource_id,
            download_path=download_path,
            condensed=condensed,
        )
        self.add_job(download_job)
        return download_job

    def remove_job(self, job_id):
        """Remove a DownloadJob from the queue and perform cleanup

        Running jobs are aborted before they are removed.
        """
        dj = self.get_job(job_id)
        self.abort_job(job_id)
        for ii, job in enumerate(list(self.jobs)):
            if job.job_id == job_id:
                self.jobs.pop(ii)
                # cleanup temp files
                try:
                    job.path_temp.unlink()
                except BaseException:
                    pass
                break
        # also remove from eternal jobs
        if (self.jobs_eternal is not None
                and self.jobs_eternal.is_job_queued(dj)):
            self.jobs_eternal.obliterate_job(dj)


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
